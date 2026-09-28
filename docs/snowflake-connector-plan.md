# Snowflake Connector (FileStorage-based) — Implementation Plan

## Background

`CluedIn.Connector.Snowflake` is a separate, old repository whose connector predates
CluedIn 5.0 and writes directly via row-by-row `MERGE INTO` SQL. Rather than upgrading
that repo in place, this plan adds a **new connector project inside this repository**
(`CluedIn.Connector.AzureDataLake`), built on `Connector.FileStorage.Common`, the same
base every connector in this repo (`Connector.AmazonS3`, `Connector.FabricOpenMirroring`,
`Connector.AzureDataLake`, `Connector.OneLake`, `Connector.AzureDatabricks`,
`Connector.SynapseDataEngineering`, `Connector.AzureAIStudio`) is built on.

## Requirements

- Project name: `Connector.Snowflake`, everything self-contained in that one project
  (no shared "common" sub-project needed beyond `Connector.FileStorage.Common`).
- Supports **Sync mode only** — Event Stream mode is not supported (same restriction as
  `Connector.FabricOpenMirroring` and `Connector.AmazonS3`).
- **Delta mode**, with soft-delete semantics following `Connector.FabricOpenMirroring`
  exactly (see "Delta & soft-delete" below).
- Configuration fields similar to the original `CluedIn.Connector.Snowflake` connector
  (account, database, schema, table/target, warehouse, role, auth).
- Loads data into Snowflake via the **Snowpipe Streaming REST API** (not the classic
  `MERGE INTO ... VALUES` row-by-row approach, not file-based `PUT`/`COPY INTO`), driven
  from the **export job**, not from `StoreData`.
- Integration tests, run against a real Snowflake account (details below).

## Reference connectors studied

| Aspect | `Connector.AmazonS3` | `Connector.FabricOpenMirroring` | Decision for Snowflake |
|---|---|---|---|
| `StoreData` override | none | none | **none** — base `StorageConnectorBase.StoreData` handles everything |
| `IsStreamCacheEnabled` | base default (`false`) | `true` | `true` |
| `IsDeltaMode` | base default (`false`) | `true` | `true` |
| `IsSoftDelete` | base default (`true`), unused (no delta) | base default (`true`), **not overridden** | **not overridden** — inherit base default `true` |
| `GetSupportedModes()` | `[StreamMode.Sync]` | `[StreamMode.Sync]` | `[StreamMode.Sync]` |
| Custom `ISqlDataWriter` | none (uses base CSV/JSON/Parquet writers) | CSV/Parquet writers for Fabric mirroring format | **custom writer** that streams rows to Snowpipe Streaming REST API instead of formatting bytes |
| `PostExportAsync` | not used | not used | **used** — this is where transient→target `MERGE` + transient table cleanup happens |

## Delta & soft-delete mechanics (already built into `Connector.FileStorage.Common`, do not reimplement)

1. `StorageConnectorBase.StoreData` writes every entity change into a SQL Server
   **system-versioned temporal table** (`Connector.FileStorage.Common\Connector\StorageConnectorBase.cs`,
   `WriteToCacheTable`). When the entity is removed and soft-delete is in effect, it does
   an `UPDATE ... SET [__ChangeType__] = 'Removed'` (not a real `DELETE`) — this bumps the
   row's `ValidFrom` in the temporal table.
2. `StorageExportEntitiesJobBase` (`GetDataSql`) reads deltas with:
   `SELECT * FROM [table] FOR SYSTEM_TIME AS OF @asOfTime WHERE ValidFrom > @validFrom`.
   Because a soft-delete bumps `ValidFrom`, removed rows are naturally picked up as part
   of the delta, tagged `__ChangeType__ = Removed`.
3. `SqlDataWriterBase.ShouldSkip` already encodes: in delta mode, skip `Removed` rows only
   on the **initial** export; include them on every subsequent delta export.
4. Conclusion: the Snowflake connector configuration only needs to set
   `IsStreamCacheEnabled = true` and `IsDeltaMode = true` (mirroring
   `OpenMirroringConnectorConfiguration`) and must **not** override `StoreData` or
   `IsSoftDelete`. All delta/soft-delete plumbing is inherited for free.

## Export flow (confirmed against user's original description)

1. Export job starts. Before creating a new transient/landing table, check for and clean
   up a leftover transient table from a previous run that crashed/failed.
2. Create a fresh transient table in Snowflake to stage rows for this run.
3. `StorageExportEntitiesJobBase` reads delta rows from the SQL Server cache table (step 2
   of "Delta & soft-delete" above) and hands them to a **custom `ISqlDataWriter`**
   (`SnowflakeSnowpipeSqlDataWriter`). Its `WriteOutputAsync` iterates the `SqlDataReader`
   and, for each row, calls the **Snowpipe Streaming REST API** to append the row (with
   `__ChangeType__` and `PersistVersion`) into the transient table. Always a plain
   append/insert — no upsert logic at this stage.
4. `PostExportAsync` (a no-op hook in the base class, not used by any existing connector —
   this is the "post export" hook referenced in the original design discussion) runs the
   `MERGE` from the transient table into the real target table: dedupe to the latest
   `PersistVersion` per entity first (the same entity can appear multiple times in the
   transient table across one run), then apply insert/update/delete based on each row's
   `__ChangeType__`. Then drops the transient table.

## `IStorageClient` / `IStorageFileClient` for Snowflake

The base pipeline (`StorageExportEntitiesJobBase`) always drives its bookkeeping through
`IStorageClient`/`IStorageFileClient` (temp file → rename, `GetBaseDirectoryPathAsync`,
`CreateDirectoryIfNotExistsAsync`, `GetFileMetadataAsync`, etc.) regardless of writer.
Confirmed by reading the base class directly:

- `GetLastExportedFile` (default, not overridden by anyone) reads from the **export
  history SQL table**, not from any real file — so delta bookkeeping does not require a
  real file to exist.
- The one place `GetFileMetadataAsync` result is used (`outputFileExists` /
  `existingFileMatchesExpected`) is a secondary duplicate-run guard; the primary guard is
  `HasExported(...)`, which is already history-table-based and runs earlier in the
  pipeline regardless of writer.

Decision: implement a lightweight `SnowflakeStorageClient : IStorageClient` /
`SnowflakeStorageFileClient : IStorageFileClient` that does **not** talk to any blob
storage. `GetFileMetadataAsync`/`FileExistsAsync` simply report "not found" (the real
duplicate-run guard already exists independently), `GetBaseDirectoryPathAsync` returns a
virtual path built from `{Database}.{Schema}` for logging purposes only, and
`CreateDirectoryIfNotExistsAsync`/connection verification run a lightweight Snowflake
connectivity check instead. No real bytes are ever written through this client — the real
data path to Snowflake is entirely through the Snowpipe Streaming REST API inside the
custom `ISqlDataWriter`.

## Snowpipe Streaming REST API client

Needs its own thin client in the new project: open a streaming channel for the transient
table, append rows, track offset tokens, close the channel at the end of the run.
Authentication for the Snowpipe Streaming REST API is **key-pair (JWT) based**, not
username/password.

### Provided Snowflake test account details

```
Account    qs30799
Database   SNOWFLAKE_LEARNING_DB
Schema     TESTSCHEMA
Table      MYTESTTABLE
Warehouse  COMPUTE_WH
Role       ACCOUNTADMIN
```

No username, password, or private key was supplied. These are required to actually
authenticate (JDBC/ADO.NET connection for the `MERGE`/DDL side needs a user + password or
key pair; the Snowpipe Streaming REST API specifically requires RSA key-pair JWT auth).
**Configuration and integration tests are written to read these from environment
variables** (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY` /
`SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_TABLE`,
`SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ROLE`), following the same pattern
`Connector.AmazonS3.Tests.Integration` uses for its S3 credentials — never hardcoded.
Tests that need credentials skip gracefully when the environment variables are absent, but
are otherwise fully wired up and ready to run once a user + private key are supplied.

## Phases

- [x] **Phase 0** — this plan document. _(commit d5dbff9)_
- [x] **Phase 1** — project scaffolding: `src/Connector.Snowflake` csproj, solution entry,
  `IConfigurationConstants`/`SnowflakeConfigurationConstants`, config UI fields, resource
  icon, `InstallComponents`. _(commit 5d527bc)_
- [x] **Phase 2** — `SnowflakeConnectorConfiguration` (`IsStreamCacheEnabled = true`,
  `IsDeltaMode = true`, no `IsSoftDelete` override), `SnowflakeStorageFactory`,
  `SnowflakeConnector` (`GetSupportedModes() => [Sync]`, connection verification).
  _(commit fd2de7b)_
- [x] **Phase 3** — `SnowflakeStorageClient`/`SnowflakeStorageFileClient` (lightweight,
  no blob storage, per design above). _(commit 0b8437d)_
- [x] **Phase 4** — Snowflake REST API client: the SQL API for DDL/MERGE and the Snowpipe
  Streaming REST API for row ingestion, both authenticated with the same RSA key-pair JWT,
  implemented with only `HttpClient` + BCL crypto (no ADO.NET driver / extra NuGet
  dependency - see "Implementation notes" below). _(commit b2e9f6d)_
- [x] **Phase 5** — `SnowflakeSnowpipeSqlDataWriter : ISqlDataWriter` (streams delta rows
  from the reader into the transient table via the REST client, batched). _(commit 978296b)_
- [x] **Phase 6** — `SnowflakeExportEntitiesJob : StorageExportEntitiesJobBase` wiring the
  writer + `PostExportAsync` (dedupe-and-`MERGE` transient → target, then truncate the
  transient table - see "Implementation notes" for why it is truncated rather than
  dropped). _(commit f8179c1)_
- [x] **Phase 7** — Unit tests (`test/unit/Connector.Snowflake.Tests.Unit`): 24 tests
  covering JWT construction, SQL generation (transient table/pipe DDL, truncate, and the
  dedupe + insert/update/delete `MERGE`), configuration field mapping, and
  `SnowflakeApiClient` request construction against a mocked `HttpMessageHandler`. All
  passing, no credentials required. _(commit de86dab)_
- [x] **Phase 8** — Integration tests (`test/integration/Connector.Snowflake.Tests.Integration`)
  against the real Snowflake account above: `SELECT 1` connectivity, a transient
  table/query round trip via the SQL API, and a full Snowpipe Streaming channel
  open/append/close round trip - the highest-risk, least-verified part of phase 4.
  Gated on `SNOWFLAKE_USER`/`SNOWFLAKE_PRIVATE_KEY` env vars via `Assert.Skip` (xunit v3's
  dynamic skip); confirmed they report as **Skipped**, not failed, when those env vars are
  absent, which is the case in this environment - so these have not yet run for real.
  _(commit a0303f8)_
- [x] **Phase 9** — Docs + final pass. _(this commit)_

Each phase is committed separately on branch `feature/snowflake-connector`.

## Setup checklist (to actually run this for real)

Updated after a live run against the real account (user `CLUEDIN_STREAM_SVC` and an RSA
key pair were supplied after phase 8 landed):

1. ~~Generate an RSA key pair and register the public key on a Snowflake user~~ - done;
   user `CLUEDIN_STREAM_SVC` with key-pair auth is working end-to-end through JWT
   generation.
2. **Found and fixed two real bugs in `SnowflakeApiClient` while running against the live
   account** (both committed):
   - The HTTP host was built from `SnowflakeJwtTokenBuilder.NormalizeAccount(account)`,
     which strips everything after the first `.` - correct for the JWT `iss`/`sub` claims
     (which must use the bare account locator), but wrong for the host, which needs the
     **full** account identifier including any region/cloud suffix. This account's
     deployment is at `qs30799.ap-southeast-1.snowflakecomputing.com` - the bare
     `qs30799.snowflakecomputing.com` returns Snowflake's generic 404 HTML page (confirmed
     with `curl`). Fixed by using `settings.Account` directly (untouched) for the host, and
     keeping `NormalizeAccount` only for the JWT claims. **Configured `SNOWFLAKE_ACCOUNT`
     must therefore be the full identifier, `qs30799.ap-southeast-1`**, not the bare
     `qs30799` given in the original account details - `SnowflakeTestCredentials`'s default
     was updated accordingly.
   - The SQL API rejected requests with "Invalid or empty User-Agent header set" - added a
     `User-Agent` header to every request.
3. ~~Grant a role to `CLUEDIN_STREAM_SVC`~~ - done. `ACCOUNTADMIN` (given in the original
   account details) turned out not to be granted to this service user; `CLUEDIN_STREAM_ROLE`
   was granted instead (`GRANT ROLE CLUEDIN_STREAM_ROLE TO USER CLUEDIN_STREAM_SVC;`) and
   works. **`SNOWFLAKE_ROLE=CLUEDIN_STREAM_ROLE`**, not `ACCOUNTADMIN`.
4. **All 3 phase 8 integration tests now pass for real against the live account** (SQL API:
   `SELECT 1`, create/insert/query a transient table; Snowpipe Streaming: open channel,
   append a row, close channel). While getting there, found and fixed a third real bug -
   the Snowpipe Streaming REST API is a **completely separate deployment** from the SQL
   API, not just a different path prefix on the same host, as the original implementation
   assumed. Confirmed against the live account (see the probe sequence that established
   this, run via `curl` with a hand-generated JWT):
   - `GET /v2/streaming/hostname` on the **control host** (`{account}.snowflakecomputing.com`,
     authorized with the account JWT) returns the actual **ingest host**
     (`QS30799.ingest.sinats.snowflakecomputing.com` for this account) - a different
     hostname entirely, not derivable from the account name.
   - The account JWT must then be exchanged for a **scoped token** via
     `POST /oauth/token` on the control host (form-encoded
     `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&scope={ingestHost}`, still
     authorized with the account JWT) - a plain-text bearer token in the response body, not
     JSON-wrapped.
   - All channel operations go to the **ingest host**, authorized with the **scoped
     token** (not the JWT): open/close channel is
     `{method} /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}`
     (`PUT`/`DELETE`); append rows is
     `POST /v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows?continuationToken=...`
     with the body as **newline-delimited JSON** (`Content-Type: application/x-ndjson`, one
     row object per line), not a JSON object wrapping a `"rows"` array as first implemented.
   `SnowflakeApiClient` now does hostname discovery and scoped-token exchange itself
   (cached, refreshed alongside the JWT), and unit tests
   (`SnowflakeApiClientTests.CreateStreamingHandler`) mock both steps.
5. Ensure the target table has the shape this connector's `MERGE` expects: at minimum an
   `ID` column and a `DATA VARIANT` column (see "Implementation notes" below) - adjust
   `MYTESTTABLE` or `SnowflakeSqlBuilder.MergeTransientIntoTarget` if the real target shape
   differs. Not yet exercised end-to-end (the integration tests cover the REST client, not
   a full connector export run).
6. Wiring this connector into an actual CluedIn export target/stream and running a real
   export end-to-end has not been done yet - the integration tests validate
   `SnowflakeApiClient` directly, not `SnowflakeExportEntitiesJob`'s full flow.

## Implementation notes (refinements made while building phases 1-6)

- **Stable transient table + pipe, not "fresh per run".** Snowpipe Streaming pipes are
  bound to a fixed target table via `COPY INTO`, so recreating the transient table with a
  unique name every run would mean recreating its pipe every run too. Instead
  `SnowflakeConnectorConfiguration.TransientTableName`/`PipeName` are deterministic
  (`{TableName}__CLUEDIN_TRANSIENT` / `{TableName}__CLUEDIN_PIPE`), created idempotently
  (`CREATE ... IF NOT EXISTS`) in `InitializeBaseDirectoryAsync`, and **truncated** (not
  dropped) both at the start of a run (clears anything left by a previous crashed run) and
  again in `PostExportAsync` after a successful `MERGE` (steady-state cleanup).
- **Transient table is schema-independent.** It has a fixed 4-column shape -
  `ENTITY_ID VARCHAR, CHANGE_TYPE VARCHAR, PERSIST_VERSION NUMBER, ROW_DATA VARIANT` (see
  `TransientTableColumns`) - rather than one column per CluedIn property, because the
  target table's real schema is user-managed and not knowable at DDL time. Consequently
  **the target table (e.g. `MYTESTTABLE`) is expected to have an `ID` column and a `DATA`
  VARIANT column** for the generated `MERGE` (`SnowflakeSqlBuilder.MergeTransientIntoTarget`)
  to work as written - this is a real constraint on the pre-existing target table, not
  auto-created by this connector.
- **No Snowflake ADO.NET driver / extra NuGet dependency.** Both the DDL/MERGE side and the
  Snowpipe Streaming ingestion side go over Snowflake's public REST surface (the SQL API and
  the Snowpipe Streaming REST API respectively), authenticated with a hand-built RS256 JWT
  signed with `System.Security.Cryptography.RSA` - no `Snowflake.Data` package reference.
  This keeps the connector's only dependency on Snowflake being `HttpClient`, and keeps both
  REST surfaces unit-testable behind `ISnowflakeApiClient` with a mocked
  `HttpMessageHandler`, without needing a live account.
- **Risk flag:** the exact Snowpipe Streaming REST API request/response shapes in
  `SnowflakeApiClient` (channel open/append-rows/close) are implemented to the best of
  available knowledge of Snowflake's public REST contract, but have **not been verified
  against a live account** (no Snowflake user/private key was supplied - see "Provided
  Snowflake test account details" above). Once real key-pair credentials are available,
  the integration tests in phase 8 will be the first real signal on whether the request/
  response shapes need adjusting.
