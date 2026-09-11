# Migrating a Connector to Multi-Version Targeting

This document tracks the migration of `CluedIn.Connector.AzureDataLake` from a single published
package to the multi-version targeting pattern. Modeled on the other repos already migrated in
this effort.

Unlike every other repo in this effort, the **code side of this migration was already done** by a
separate, unrelated piece of work before this document existed — see "Prior state" below. Only the
pipeline side was missing.

---

## Overview

| CluedIn version | .NET TFM | Package suffix |
|---|---|---|
| 4.6.0 | net6.0 | `.460` |
| 4.7.0 | net6.0 | `.470` |
| 4.8.0 | net6.0 | `.480` |
| 5.0.0-beta.* | net10.0 | `.500` |

4.6.0 is included here, unlike most other repos in this effort — `release/4.6.7` is still an
actively released line (tag `4.6.7`, 2026-08-10), and `Directory.Build.props` already defines a
`CLUEDIN_V46_OR_GREATER` constant used to gate `AzureDataLakeExtendedConfigurationProvider.v46.cs`
/ `StreamRepositoryExtensions.v46.cs` (the pre-4.7 `IStreamRepository` API shape).

Branch: `feature/multi-version-packaging` (off `develop`).

---

## Prior state

This repo used to ship as three separate release branches (`release/4.6.7`, `release/4.7.4`,
`release/4.8.1`), each with its own `Packages.props` pinning `_CluedIn` to that line. A separate,
earlier effort ("Consolidate different versions of connectors that work for different versions of
cluedin into a single code-path", merged 2026-07-15) folded all three release lines' code into
`develop` as a single codebase: shared classes plus small partial-class extension files split by
version applicability (`*.v46.cs`, `*.v47_to_Latest.cs`), gated by the same `CLUEDIN_V46_OR_GREATER`
/ `CLUEDIN_V47_OR_GREATER` / `CLUEDIN_V48_OR_GREATER` / `CLUEDIN_V50_OR_GREATER` `DefineConstants`
convention this whole effort uses elsewhere - `Directory.Build.props` and `Packages.props` were
already correct and needed no changes here.

What that consolidation *didn't* do was touch the pipeline: `azure-pipelines.yml` kept using the
single-version `crawler.build.yml` steps-template, building and publishing one package for
whatever `_CluedIn` resolved to (`5.0.0-*`, i.e. only the 5.0 line). There was also a stale,
abandoned attempt at exactly this pipeline migration - PR #238 ("Migrate to multi-version connector
packaging (Option B)"), opened 2026-07-13, two days *before* the consolidation above merged. Its
branch still carried the pre-consolidation split-file layout and was 90 commits behind `develop` by
the time this document was written. Closed as superseded rather than merged/rebased, since the code
side it was trying to migrate had already been done differently and better in the meantime.

---

## Step 1 — Pipeline template (`azure-pipelines.yml`)

Status: **Done**

Replaced the single-version `crawler.build.yml` steps-template with `crawler.build.jobs.yml`,
moved `pool:` inside the template call, added `probeCluedInVersion`, and added
`multiVersionCluedInTargets` for 4.6.0/4.7.0/4.8.0/5.0.0-beta.*.

Carried over the existing `createIntegrationEnvironmentScriptFilePath`/
`deleteIntegrationEnvironmentScriptFilePath` (`./build/integration-test.ps1`, `-Action SetUp` /
`-Action TearDown`) unchanged - `runIntegrationTests` stays defaulted to `true` as it already was.
Also kept the `AzureDataLakeConnector_BuildLock` variable group + `lockBehavior: sequential`
(unrelated to this migration - serializes pipeline runs against a shared external resource).

---

## Step 2 — `Directory.Build.props` / `Packages.props`

Status: **Already done** (by the prior consolidation effort, not this migration). No changes
needed - both already condition on `_CluedIn` directly (`VersionLessThan`/`VersionGreaterThanOrEquals`
against `5.0.0` for TFM, and the four `_OR_GREATER` constants for `DefineConstants`), which the
`crawler.build.jobs.yml` per-target `_CluedIn` override drives correctly without needing the
`CluedInMultiVersionTargetFramework` indirection used in other repos.

---

## Step 3 — `GitVersion.yml`

Status: **Done**

Reset `next-version` from `'5.0'` to `1.0`, added `ignore: commits-before: 2026-09-10T00:00:00`
(highest reachable tag: `5.0.0-alpha.200`, 2026-09-02T07:09:30+08:00).

**Not verified locally against the pinned GitVersion.Tool 5.9.0** - a scratch `dotnet tool install
... --version 5.9.0` on this machine reproducibly fails with `The type initializer for
'LibGit2Sharp.Core.NativeMethods' threw an exception` on every repo tried, including
`CluedIn.Connector.Dataverse.V2` (whose already-CI-verified `GitVersion.yml` uses the identical
pattern), so this is a local native-loader problem on this machine, not something about this
repo's content. Relying on this PR's own CI build to confirm the resolved version instead.

---

## Step 4 — CI verification

Status: **Pending** - see PR.
