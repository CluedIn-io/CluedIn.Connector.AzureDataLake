# Snowflake Connector (FileStorage-based)

## Background

`CluedIn.Connector.Snowflake` is a separate, old repository whose connector predates
CluedIn 5.0 and writes directly via row-by-row `MERGE INTO` SQL. Rather than upgrading
that repo in place, `Connector.Snowflake` in this repository is a new connector project
built on `Connector.FileStorage.Common`, the same base every connector in this repo
(`Connector.AmazonS3`, `Connector.FabricOpenMirroring`, `Connector.AzureDataLake`,
`Connector.OneLake`, `Connector.AzureDatabricks`, `Connector.SynapseDataEngineering`,
`Connector.AzureAIStudio`) is built on.

It currently ships as **beta**.

## Capabilities

- **Sync mode only** — Event Stream mode is not supported (same restriction as
  `Connector.FabricOpenMirroring` and `Connector.AmazonS3`).
- **Delta mode**, with soft-delete semantics inherited from `Connector.FileStorage.Common`
  (see "Delta & soft-delete" below) — the same mechanics `Connector.FabricOpenMirroring`
  relies on.
- Configuration fields: account identifier, user, private key (+ optional passphrase),
  database, schema, warehouse, role (optional), target table name.
- Loads data into Snowflake via the **Snowpipe Streaming REST API** (not the classic
  `MERGE INTO ... VALUES` row-by-row approach, not file-based `PUT`/`COPY INTO`), driven
  from the export job, not from `StoreData`.
- Both the transient landing table and the target table are **created and grown
  automatically** — one `VARCHAR` column per CluedIn property (`PersistVersion` as
  `NUMBER`), matching how the old `CluedIn.Connector.Snowflake` repo shapes its table,
  rather than a single `VARIANT` blob column.
- Warehouse/database/schema access is verified on connection check (`SHOW WAREHOUSES
  LIKE`/`SHOW DATABASES LIKE`/`SHOW SCHEMAS LIKE`, not `USE`, which the SQL API doesn't
  support); the warehouse check is skipped during health checks outside the first 5
  minutes of each hour, since referencing a warehouse can resume compute.
- `ArchiveContainer` drops the pipe, transient table, and target table it created.

## Architecture

### `IStorageClient` / `IStorageFileClient`

The base pipeline (`StorageExportEntitiesJobBase`) always drives its bookkeeping through
`IStorageClient`/`IStorageFileClient` regardless of writer, but delta bookkeeping
(`GetLastExportedFile`, `HasExported`) is entirely SQL-history-table-based, not
file-based. `SnowflakeStorageClient`/`SnowflakeStorageFileClient` are lightweight - they
don't talk to any blob storage. `GetFileMetadataAsync`/`FileExistsAsync` simply report
"not found", `GetBaseDirectoryPathAsync` returns a virtual path (`{Database}.{Schema}`)
for logging only, and `CreateDirectoryIfNotExistsAsync`/connection verification run a
lightweight Snowflake connectivity check (`SELECT 1`) instead. No real bytes are ever
written through this client — the real data path to Snowflake is entirely through the
Snowpipe Streaming REST API inside the custom `ISqlDataWriter`.

### Export flow (`SnowflakeExportEntitiesJob`)

On every export run:

1. Create the transient table if it doesn't exist yet, and add any columns for
   properties not seen before (`SnowflakeSqlBuilder.CreateTransientTableIfNotExists` /
   `GetAddMissingColumnsStatements`). The transient table (and its pipe) is **stable
   across runs**, not recreated per run — Snowpipe Streaming pipes are bound to a fixed
   target table via `COPY INTO`, so recreating the transient table with a unique name
   every run would mean recreating its pipe every run too.
   (`SnowflakeConnectorConfiguration.TransientTableName`/`PipeName` are deterministic:
   `{TableName}__CLUEDIN_TRANSIENT` / `{TableName}__CLUEDIN_PIPE`.)
2. Create the target table if it doesn't exist yet, and grow its columns the same way.
   `CreateContainer` (the usual place a connector provisions its target) is a no-op for
   every `Connector.FileStorage.Common`-based connector, so this happens here instead, on
   every run.
3. Create the pipe if it doesn't exist yet, bound to the transient table.
4. Truncate the transient table (clears anything a previous crashed run left behind).
5. `SnowflakeSnowpipeSqlDataWriter` reads delta rows from the SQL Server cache table via
   the `SqlDataReader` and streams them into the transient table via the Snowpipe
   Streaming REST API: open a channel, append rows in batches (each append carries a
   cumulative offset token), then **poll the channel's commit status until the offset is
   actually committed** before returning - appending only buffers rows; closing the
   channel immediately after an append does not wait for that data to commit, and an
   uncommitted append is simply lost.
6. `MergeTransientIntoTarget`: dedupe to the latest `PersistVersion` per entity (the same
   entity can appear multiple times in the transient table across one run), then apply
   insert/update/delete based on each row's `__ChangeType__`. `__ChangeType__` itself is
   transient-only bookkeeping - it's read from the transient table to decide which MERGE
   action to take, but is never a target table column and never written into it.
7. Truncate the transient table again (steady-state cleanup).

## Delta & soft-delete mechanics (inherited from `Connector.FileStorage.Common`, not reimplemented)

1. `StorageConnectorBase.StoreData` writes every entity change into a SQL Server
   **system-versioned temporal table**. When the entity is removed and soft-delete is in
   effect, it does an `UPDATE ... SET [__ChangeType__] = 'Removed'` (not a real `DELETE`)
   — this bumps the row's `ValidFrom` in the temporal table.
2. `StorageExportEntitiesJobBase` reads deltas with:
   `SELECT * FROM [table] FOR SYSTEM_TIME AS OF @asOfTime WHERE ValidFrom > @validFrom`.
   Because a soft-delete bumps `ValidFrom`, removed rows are naturally picked up as part
   of the delta, tagged `__ChangeType__ = Removed`.
3. `SqlDataWriterBase.ShouldSkip` already encodes: in delta mode, skip `Removed` rows only
   on the **initial** export; include them on every subsequent delta export.
4. Consequence: `SnowflakeConnectorConfiguration` only needs to set
   `IsStreamCacheEnabled = true` and `IsDeltaMode = true` (mirroring
   `OpenMirroringConnectorConfiguration`) and does **not** override `StoreData` or
   `IsSoftDelete`. All delta/soft-delete plumbing is inherited for free.

## Snowpipe Streaming REST API protocol notes

No Snowflake ADO.NET driver or extra NuGet dependency is used. Both the DDL/MERGE side
(the SQL API) and the Snowpipe Streaming ingestion side go over Snowflake's public REST
surface, authenticated with a hand-built RS256 JWT signed with
`System.Security.Cryptography.RSA`. This keeps the connector's only dependency on
Snowflake being `HttpClient`, and keeps both REST surfaces unit-testable behind
`ISnowflakeApiClient` with a mocked `HttpMessageHandler`, without needing a live account.

A few non-obvious protocol details, confirmed against a live account:

- **The account identifier used for the JWT and the HTTP host are not the same string.**
  The JWT `iss`/`sub` claims must use the bare account locator (everything before the
  first `.`), but the HTTP host needs the **full** account identifier including any
  region/cloud suffix (e.g. an account might be reachable at
  `<locator>.<region>.snowflakecomputing.com`, not just `<locator>.snowflakecomputing.com`
  - the bare form can 404). `SnowflakeApiClient` builds its host from the configured
  account directly (untouched) and only normalizes to the bare locator for JWT claims.
- The SQL API requires a `User-Agent` header on every request, or it rejects with
  "Invalid or empty User-Agent header set".
- `USE WAREHOUSE`/`USE DATABASE`/`USE SCHEMA` are **not supported** by the SQL API
  ("Command not supported by SQL API"), and specifying warehouse/database/schema in a
  statement's session context does not actually validate/resolve those objects (a bogus
  name silently "succeeds"). Connection verification instead uses `SHOW WAREHOUSES/
  DATABASES/SCHEMAS LIKE '<name>'`, which correctly returns zero rows for a
  nonexistent/unauthorized object.
- The Snowpipe Streaming REST API is a **completely separate deployment** from the SQL
  API, not just a different path prefix on the same host:
  - `GET /v2/streaming/hostname` on the **control host** (`{account}.snowflakecomputing.com`,
    authorized with the account JWT) returns the actual **ingest host** - a different
    hostname entirely, not derivable from the account name.
  - The account JWT must then be exchanged for a **scoped token** via `POST /oauth/token`
    on the control host (form-encoded
    `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&scope={ingestHost}`, still
    authorized with the account JWT) - a plain-text bearer token in the response body, not
    JSON-wrapped.
  - All channel operations go to the **ingest host**, authorized with the **scoped
    token** (not the JWT): open/close channel is
    `{method} /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}`
    (`PUT`/`DELETE`); append rows is
    `POST /v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows?continuationToken=...`
    with the body as **newline-delimited JSON** (`Content-Type: application/x-ndjson`, one
    row object per line).
  - Appending rows returns a channel status, but **does not itself confirm a commit** -
    the `startOffsetToken`/`endOffsetToken` query parameters must both be supplied on
    every append for Snowflake to commit the batch at all, and the caller must then poll
    `:bulk-channel-status` until `lastCommittedOffsetToken` matches before it's safe to
    close the channel (see step 5 of the export flow above).

`SnowflakeApiClient` does hostname discovery and scoped-token exchange itself (cached,
refreshed alongside the JWT), and unit tests (`SnowflakeApiClientTests.CreateStreamingHandler`)
mock both steps.

## Testing

- **Unit tests** (`test/unit/Connector.Snowflake.Tests.Unit`, 46 tests, no credentials
  required): JWT construction, SQL generation (transient/target table DDL, `ALTER TABLE
  ADD COLUMN`, truncate, `SHOW`/`DROP` statements, and the dedupe + insert/update/delete
  `MERGE`), configuration field mapping, and `SnowflakeApiClient` request construction
  against a mocked `HttpMessageHandler`.
- **Integration tests** (`test/integration/Connector.Snowflake.Tests.Integration`), gated
  on environment variables (see "Required environment variables" below) and skipped
  (rather than failed) when they're absent:
  - `SnowflakeApiClientIntegrationTests` — `SELECT 1` connectivity, a transient
    table/query round trip via the SQL API, and a full Snowpipe Streaming channel
    open/append/close round trip with commit polling.
  - `SnowflakeDeltaExportIntegrationTests` — proves that successive exports only ship the
    delta (rows actually changed since the last successful export) rather than a full
    resend of the whole stream cache each run, covering new inserts, an update to an
    existing entity, and a deletion, run in sequence against a live Snowflake account and
    a real SQL Server stream cache:
    1. Store 3 new entities (A, B, C), run export #1 (initial export) - asserts all 3
       rows land in the target table **and** the export history's `TotalRows` for that
       run is 3.
    2. Store 1 new entity (D), run export #2 - asserts D lands alongside A/B/C untouched,
       and `TotalRows` for that run is **1**, not 4 (proving new inserts are delta-only).
    3. Update entity A, run export #3 - asserts A's value changed in place (via the
       `MERGE`) while B/C/D are untouched, and `TotalRows` is **1** (proving updates are
       delta-only).
    4. Delete entity B, run export #4 - asserts B is gone from the target table (via the
       `MERGE`'s `WHEN MATCHED AND ChangeType='Removed' THEN DELETE` clause) while
       A/C/D are untouched, and `TotalRows` is **1** (proving deletions are delta-only).

    `TotalRows` (read directly off the SQL Server export-history table, not the target
    table) is the proof here, not just the target table's final contents - since the
    `MERGE` is idempotent, a full resend of every row would produce the *same* final
    target table state as a correct delta-only send, so only the per-run row count read
    off the export history table
    (`CacheTableHelper.GetExportHistoryTableName(streamId) + "_ExportHistory"`) can
    distinguish "sent everything again" from "sent only what changed".

    This test does **not** extend `StorageConnectorTestsBase<TConnector,...>` the way
    `AmazonS3ConnectorTests`/`OpenMirroringConnectorTests` do: that base class's own
    inherited `[Fact]`s assert exact CSV/Parquet file contents against a fixed,
    lowercase, file-based column set, which doesn't apply to Snowflake (no output file; a
    dynamic, upper-cased, one-column-per-property target table instead). It builds its
    own minimal Castle Windsor/Moq harness instead (mirroring
    `StorageConnectorTestsBase.SetupContainer`'s shape, condensed to what
    `SnowflakeConnector`/`SnowflakeExportEntitiesJob` actually touch).

### Required environment variables

Both integration test classes read credentials via `SnowflakeTestCredentials`, following
the same pattern `Connector.AmazonS3.Tests.Integration` uses for its S3 credentials -
nothing is ever hardcoded, and there are no defaults, so tests skip (rather than fail)
whenever any of these is unset:

| Variable | Required | Notes |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | Account identifier - include a region/cloud suffix if your deployment needs one. |
| `SNOWFLAKE_USER` | yes | |
| `SNOWFLAKE_PRIVATE_KEY` | yes | PEM-encoded RSA private key for key-pair auth. Accepts a raw PEM (real newlines), a PEM with literal `\n` escapes, or a base64-encoded PEM. |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | no | Only if the private key is encrypted. |
| `SNOWFLAKE_DATABASE` | yes | |
| `SNOWFLAKE_SCHEMA` | yes | |
| `SNOWFLAKE_WAREHOUSE` | yes | |
| `SNOWFLAKE_ROLE` | no | Blank uses the user's default role. |

There's no `SNOWFLAKE_TABLE` variable - target/transient table names are generated per
test run instead (`SnowflakeTestCredentials.TargetTable`, and each test's own scratch
table names), matching how `AzureDataLakeConnectorTests`/`AzureDataLakeStorageClientTests`
name their scratch file systems/directories (`$"xunit-{DateTime.Now.Ticks}"`) and
`OneLakeConnectorTests` names its scratch table (`Guid.NewGuid().ToString("N")`) - so
nothing needs cleaning up in Snowflake between runs, and concurrent runs don't collide.

`devonly.runsettings` (untracked, not committed - local-only) has a template
`<!-- Snowflake -->` section with these variable names.

### Environment/build note

`Connector.Snowflake.Tests.Integration.csproj` needs `IgnoresAccessChecksTo` entries for
`CluedIn.ComponentHealth`/`CluedIn.Streams` (plus the `IgnoresAccessChecksToGenerator`
package), matching `Connector.FileStorage.Common.Tests.Integration.csproj`, since
`SnowflakeDeltaExportIntegrationTests` references `IStreamLogService`/
`IComponentHealthService`-adjacent types that are `internal` in those external assemblies.
