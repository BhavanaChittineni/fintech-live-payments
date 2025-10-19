# Financial Transaction Analytics — Real-Time (AWS + Snowflake + Power BI)

## Quick Start
1) Create S3 bucket, e.g., `fintech-bhavana` and folder `data/transactions/`.
2) Python virtualenv + install:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```
3) Stream micro-batches to S3:
```bash
python src/generator_to_s3.py --bucket <YOUR_BUCKET> --prefix data/transactions --batch-size 200 --interval 15
```
4) In Snowflake, open `snowflake/setup.sql` and replace placeholders:
- `<YOUR_AWS_IAM_ROLE_ARN>` and `<YOUR_BUCKET_NAME>`
- Run statements to create integration, stage, table, pipe, views.
5) Power BI:
- Connect to Snowflake (DirectQuery)
- Import MODEL views: `KPIS_60M`, `REVENUE_PER_MINUTE_24H`, `TRANSACTIONS_CLEAN`
- Apply theme `powerbi/fintech_minimal_light.json`
- Build Executive KPIs, Trends, Breakdowns pages.

Power Bi dashboard:
A lightweight, near-real-time dashboard showing:
- **KPIs (last 60m):** transactions, decline rate, avg ticket, refund rate, total amount  
- **24h revenue trend** (per minute)
- **Top merchants (24h)**
- **Channel mix** (CARD / WALLET / UPI / BANK_TRANSFER)
- **Status breakdown** (Approved / Declined / Refunded)

**Stack:** Snowflake (stages, pipe, views) → Power BI (DirectQuery)  
**Data:** Synthetic transaction stream (optional generator)

---

## Architecture

```mermaid
flowchart LR
  A[S3 bucket<br/>/data/transactions] -- auto-ingest --> B[Snowflake PIPE]
  B --> C[RAW.TRANSACTIONS_RAW]
  C --> D[SILVER.V_TRANSACTIONS_CLEAN]
  D --> E[GOLD Views<br/>V_KPIS_60M, V_REVENUE_PER_MINUTE_24H,<br/>V_MERCHANT_MINUTE_24H]
  E -- DirectQuery --> F[Power BI Report]
