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

- [ ] **Phase 0** — this plan document. _(this commit)_
- [ ] **Phase 1** — project scaffolding: `src/Connector.Snowflake` csproj, solution entry,
  `IConfigurationConstants`/`SnowflakeConfigurationConstants`, config UI fields, resource
  icon, `InstallComponents`.
- [ ] **Phase 2** — `SnowflakeConnectorConfiguration` (`IsStreamCacheEnabled = true`,
  `IsDeltaMode = true`, no `IsSoftDelete` override), `SnowflakeStorageFactory`,
  `SnowflakeConnector` (`GetSupportedModes() => [Sync]`, connection verification).
- [ ] **Phase 3** — `SnowflakeStorageClient`/`SnowflakeStorageFileClient` (lightweight,
  no blob storage, per design above).
- [ ] **Phase 4** — Snowpipe Streaming REST API client (auth, open channel, append rows,
  close channel) + transient table lifecycle (leftover cleanup, create fresh table).
- [ ] **Phase 5** — `SnowflakeSnowpipeSqlDataWriter : ISqlDataWriter` (streams delta rows
  from the reader into the transient table via the REST client).
- [ ] **Phase 6** — `SnowflakeExportEntitiesJob : StorageExportEntitiesJobBase` wiring the
  writer + `PostExportAsync` (dedupe-and-`MERGE` transient → target, drop transient table).
- [ ] **Phase 7** — Unit tests (`test/unit/Connector.Snowflake.Tests.Unit`).
- [ ] **Phase 8** — Integration tests (`test/integration/Connector.Snowflake.Tests.Integration`)
  against the real Snowflake account above, gated on environment variables.
- [ ] **Phase 9** — Docs (README/guide entry) + final pass, mark plan complete.

Each phase is committed separately on branch `feature/snowflake-connector`.
