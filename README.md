```text
# Banking-Lakehouse

[Sources] → [Event Hub / Azure SQL] → [Bronze] → [Silver] → [Gold] → [Consumption]
                                       ↑          ↑          ↑
                                    dbt tests  dbt tests  dbt tests + contracts
                 ───────────── Unity Catalog (Governance + Lineage) ─────────────
                 ────────────── Spark DLT Pipelines + dbt Core ──────────────────


Basic Structure:
banking-lakehouse/
├── ingestion/
│   ├── streaming/          ← Spark DLT notebooks (transactions, ATM)
│   └── batch/              ← ADF pipelines or LakeFlow configs
├── dbt_banking/
│   ├── models/
│   │   ├── bronze/         ← sources.yml + freshness
│   │   ├── silver/         ← staging + intermediate models
│   │   └── gold/           ← mart models + contracts
│   ├── tests/
│   │   ├── generic/        ← reusable test macros
│   │   └── singular/       ← balance reconciliation SQL tests
│   ├── macros/             ← PII masking, SCD Type 2 helpers
│   └── exposures.yml       ← document Power BI, ML consumers
├── synthetic_data/
│   └── generate.py         ← Faker-based data generators
└── databricks/
    └── workflows/          ← Job orchestration YAMLs

1. 📥 DATA SOURCES & INGESTION
🔴 Streaming (Azure Event Hub → Spark Structured Streaming)
transactions  -> Stream  -> Card swipes, ACH, wire transfers
atm_events  ->  Stream  -> Withdrawals, errors, sessions

📦 Batch (Azure SQL / ADLS → LakeFlow Connect or ADF)
user_profiles  -> Batch daily  -> Core banking CRM
accounts  -> Batch daily  -> Account master
audit_logs  -> Batch daily  -> User activity, login events
vendor_balances  -> Batch daily  -> Interbank / 3rd party
historical_transactions  -> One-time  -> Migration load

2. 🥉 BRONZE LAYER (Raw Landing)
Rule: Append-only, schema-on-read, no transformations. CDF (Change Data Feed) enabled.
01_bronze.transactions  -> Streaming
01_bronze.atm_events  -> Streaming
01_bronze.user_profiles  -> Batch
01_bronze.accounts  -> Batch
01_bronze.audit_logs  -> Batch
01_bronze.vendor_balances  -> Batch
01_bronze.historical_transactions  -> One-time

3. 🥈 SILVER LAYER (Cleansed & Enriched)   - TBD
