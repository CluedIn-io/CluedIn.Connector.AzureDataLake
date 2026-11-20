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
| `Mode` | `Single`: batching is off, every entity is written on a connection and transaction of its own. `Batched`: batching is on and writes what is there. `BatchedWaiting`: batching is on and a batch waits 100 ms (the shortest wait the batcher has) for more entities. |
| `SoftDelete` | Always `true`: only the writes of a stream with soft deletes (delta mode, which adds the persist version check) are batched, hard delete writes always go one by one. |
| `Concurrency` | How many `StoreData` calls are in flight at the same time (80). This is what decides how many entities there are to write together. |
| `Entities` | The entities written in an iteration (5000). |

`Insert` writes entities the table does not have yet, `Update` writes a newer persist version of entities it has.

Next to the time and memory, two columns say what it costs SQL Server in connections, sampled from a connection of
its own while an iteration runs (the connection pools are cleared before every iteration):

| Column | |
|---|---|
| `Peak connections` | The most connections this process had open to SQL Server at the same time. |
| `New logins` | The connections logged in during the iteration (a pooled connection that is reused does not log in again). |

Run it against a SQL Server that is as far away as the one the stream cache is in production. Writing one entity at a
time costs round trips, and what batching saves is round trips, so a SQL Server on the same machine hides most of what it
is for. The batching settings are the connector defaults: batch size 50 (`Streams.<Connector>.CacheBatching.BatchSize`) and 10 output threads (`Streams.<Connector>.CacheBatching.OutputThreads`).

A delay can be added to a SQL Server in Docker with `tc qdisc add dev eth0 root netem delay 2ms` (the container
needs `--cap-add NET_ADMIN`).
