# Connector.FileStorage.Common.Benchmarks

Benchmarks of the stream cache writes of `StorageConnectorBase.StoreData`: one write per entity against the entities
that are stored at the same time being written together (`Streams.<Connector>.CacheBatching.Enabled`).

They store entities through a real connector into a real stream cache table in SQL Server, so what is measured is the
connector's own code and the round trips to SQL Server, not a model of them. The storage target is not involved.

## Running

You need a SQL Server (and a database in it) to write the stream cache tables to. Give it as a base64 encoded connection
string, like the integration tests do:

```powershell
$env:INTEGRATIONTEST_STREAMCACHE = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("Server=localhost,1433;Database=CacheBatch;User Id=sa;Password=...;TrustServerCertificate=True;Encrypt=False"))
dotnet run -c Release --project tools/Connector.FileStorage.Common.Benchmarks -- --filter "*"
```

`--filter "*Batched*"` runs a part of it (`--list flat` lists the cases). The benchmarks run in the benchmark
process (no project is built per benchmark), and every iteration writes to a table of its own, that is dropped after it.

## What is measured

| Parameter | |
|---|---|
| `Mode` | `Single`: batching is off, every entity is written on a connection and transaction of its own. `Batched`: batching is on and writes what is there. `Batched50ms` and `Batched100ms`: batching is on and a batch waits 50 / 100 ms for more entities (the batcher waits in slices of 100 ms, so 50 ms is 100 ms). |
| `SoftDelete` | Always `true`: only the writes of a stream with soft deletes (delta mode, which adds the persist version check) are batched, hard delete writes always go one by one. |
| `Concurrency` | How many `StoreData` calls are in flight at the same time (80). This is what decides how many entities there are to write together. |
| `Entities` | The entities written in an iteration (5000). |

`Insert` writes entities the table does not have yet, `Update` writes a newer persist version of entities it has.

Next to the time and memory, columns say what it costs SQL Server in connections, sampled from a connection of
its own while an iteration runs (the connection pools are cleared before every iteration), and how the entities were batched:

| Column | |
|---|---|
| `Peak connections` | The most connections this process had open to SQL Server at the same time. |
| `New logins` | The connections logged in during the iteration (a pooled connection that is reused does not log in again). |
| `Batches` | The batches the `OperationBatcher` processed during the iteration (queued batches are merged up to the batch size) (`-` without batching). |
| `Mean batch`, `Batch p50`, `Batch p95`, `Batch max` | The number of entities in those batches. The batcher counts them when the connector turns on `CollectCacheBatchSizeMetrics`, which only the benchmark's connector does. A batch of one entity is written with the single entity path. |

Run it against a SQL Server that is as far away as the one the stream cache is in production. Writing one entity at a
time costs round trips, and what batching saves is round trips, so a SQL Server on the same machine hides most of what it
is for. The batching settings are the connector defaults: batch size 50 (`Streams.<Connector>.CacheBatching.BatchSize`) and 10 output threads (`Streams.<Connector>.CacheBatching.OutputThreads`).

A delay can be added to a SQL Server in Docker with `tc qdisc add dev eth0 root netem delay 2ms` (the container
needs `--cap-add NET_ADMIN`).

## Nightly run

The pipeline in `azure-pipelines.yml` runs the whole benchmark every night at 01:00 UTC on `develop` (the jobs are in
`build/benchmark.jobs.yml`). The schedule has `always: false`, so a night is skipped when `develop` has not changed since the last scheduled
run that succeeded. A scheduled run only benchmarks: it does not build, test or publish the connector. A run queued by hand does the same
when its `Run the cache write benchmark instead of the build` parameter is on, and builds as usual otherwise.

A second schedule, at 04:00 UTC and every night, does not benchmark. It cleans up what the integration tests leave behind
(`build/cleanup.jobs.yml`; every script has a `-DryRun` switch that only lists what it would delete):
- The containers that start with `xunit-fs-` in the storage account (`ADL2_ACCOUNTNAME`, `ADL2_ACCOUNTKEY`), `build/cleanup-storage.ps1`.
- All the tables (`-Target LakehouseTables`) and all the files (`-Target LakehouseFiles`) of the OneLake lakehouse `ONELAKE_ITEMNAME` of the
  workspace `ONELAKE_WORKSPACENAME`, with the service principal `ONELAKE_TENANTID`, `ONELAKE_CLIENTID` and `ONELAKE_CLIENTSECRET`,
  `build/cleanup-onelake.ps1`.
- All the files under `Tables` of the Open Mirroring mirrored database `FABRICOPENMIRRORING_MIRROREDDATABASENAME` of the workspace
  `ONELAKE_WORKSPACENAME`, with the service principal `FABRICOPENMIRRORING_TENANTID`, `FABRICOPENMIRRORING_CLIENTID` and
  `FABRICOPENMIRRORING_CLIENTSECRET`, `build/cleanup-onelake.ps1 -Target MirroredDatabaseTables`.
- The pipes and the tables named `XUNIT_...` in the Snowflake schema (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_DATABASE`,
  `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`, and optionally `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` and `SNOWFLAKE_ROLE`), `tools/Connector.Snowflake.Cleanup`
  (`--dry-run` only lists them). Nothing else in that schema is touched.

Both schedules start the same pipeline, and each of its jobs checks the name of the schedule that started it.

The SQL Server is created like the integration tests do, with `build/integration-test.ps1 -Action SetUp`: a SQL Server 2022
container on the build agent, with `INTEGRATIONTEST_STREAMCACHE` set to its connection string. As it is on the same machine as
the benchmark, the round trips are as cheap as they get, and the times are for comparing one night with the next, not for
what batching saves against a SQL Server that is far away.

The report of the run is on its summary tab, and the `benchmark-results` artifact has the markdown, csv and html reports.
