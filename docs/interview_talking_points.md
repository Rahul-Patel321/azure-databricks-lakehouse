# Interview Talking Points

## 30-Second Summary

"I built a Bronze/Silver/Gold lakehouse pipeline on Azure — ADF lands raw sales data into ADLS Gen2, Databricks notebooks move it through Bronze, an incrementally-merged Silver, and an aggregated Gold layer in Delta Lake, and Synapse Serverless SQL exposes Gold as external tables for Power BI. It's the same medallion pattern I used for HDFC Digital Banking and Bing Analytics — just generalized to a sales dataset."

## Design Decisions You Should Be Able to Defend

**Why medallion (Bronze/Silver/Gold) instead of one big transformation?**
Each layer has a single responsibility: Bronze preserves raw data for replay, Silver is where data quality and business rules live, Gold is purely for consumption. If a bug ships in a transformation, you fix it and reprocess from Bronze — you never touch the source of truth.

**Why incremental processing with a watermark instead of full reload each run?**
Reprocessing all of Bronze every run gets slower every month as data grows — it doesn't scale. Reading only records newer than the last watermark keeps the Silver job's runtime roughly constant regardless of total table size.

**Why MERGE instead of append into Silver?**
Idempotency — if the job fails partway and reruns, MERGE means no duplicate rows. Append-only would double-count records on every retry.

**Why OPTIMIZE + ZORDER on Gold specifically, not Bronze or Silver?**
Gold is what Power BI queries directly and what users notice latency on. ZORDER co-locates data by the columns dashboards actually filter on (region, date) so those queries skip far more irrelevant data. Running it on Bronze would be wasted effort — that layer isn't queried interactively.

**Why note Auto Loader as an enhancement instead of implementing it?**
Auto Loader (`cloudFiles`) is a Databricks-runtime-specific feature — it can't run outside an actual Databricks cluster. Being explicit about that trade-off (batch read for portability vs. Auto Loader for production efficiency) shows you understand the tool's actual constraints, not just its marketing description.

## Challenges You'd Talk Through

- **Late-arriving or corrected source records**: since Silver merges on `order_id`, a corrected record for an old order updates in place — but if the correction changes historical Gold aggregates, those aggregates need to be recomputed, not just appended to.
- **Small file problem**: incremental Silver merges and Bronze appends both produce many small files over time; `OPTIMIZE` needs to run on a schedule, not just once, or Gold query performance degrades again.
- **Schema drift from source systems**: `mergeSchema` on Bronze absorbs new columns automatically, but Silver's explicit business logic needs a deliberate decision about when to start using a new field — silently ignoring it or breaking on it are both bad defaults.

## What You'd Improve at Production Scale

- Replace the batch Bronze read with Auto Loader for true incremental, efficient file discovery instead of listing the whole raw directory each run.
- Add Databricks Unity Catalog for centralized governance/lineage across Bronze/Silver/Gold instead of path-based Delta tables.
- Schedule `OPTIMIZE`/`VACUUM` as their own ADF-triggered jobs rather than running `OPTIMIZE` inline after every Gold write.
- Add data quality expectations (e.g., Delta Live Tables expectations or a Great Expectations suite) between Bronze and Silver instead of ad hoc filters.
