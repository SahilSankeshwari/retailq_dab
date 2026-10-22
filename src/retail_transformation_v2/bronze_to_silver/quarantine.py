# ── Centralized quarantine for enforceable data-quality rejections ────
#
# DESIGN (replaces the transactions-only quarantine_transactions table):
# One common materialized view, retail_silver.quarantine_records,
# captures records rejected by the ENFORCEABLE (expect_or_drop) rules of all
# five Silver tables. Warning-only (expect) rules are intentionally NOT
# captured here — those rows stay in Silver by design.
#
# The rejection filters below must mirror the expect_or_drop decorators in
# the corresponding bronze_to_silver files exactly:
#   transactions.py    : non-null transaction_id; parseable transaction_timestamp
#   product_catalog.py : non-null/non-empty product_id, product_name; non-null launch_date
#   account.py         : non-null id
#   opportunity.py     : non-null id
#   inventory.py       : non-null inventory_id
# If an enforced rule changes in those files, update the matching filter and
# reason chain here.
#
# Materialized view (batch read, full recompute each pipeline update):
#   - reads CDC-style Bronze targets safely (MERGE/DELETE change commits are
#     picked up by recompute, unlike an append-only streaming read);
#   - full recompute replaces the table each refresh, so repeated pipeline
#     updates / job reruns never accumulate duplicate quarantine records.
#
# record_data is a VARIANT holding the COMPLETE original Bronze row (built via
# parse_json(to_json(struct))) — nulls are preserved (ignoreNullFields=false),
# so the exact rejected values remain inspectable. Metadata columns that only
# exist for the Blob source (_source_file, _ingestion_timestamp) are NULL
# for the other sources.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _to_quarantine(rejected, source_table, source_system, source_file=None, ingestion_ts=None):
    """Project a rejected-records DataFrame into the common quarantine schema.

    `rejected` must carry a `_q_reason` column holding the first failed
    enforceable rule. Helper columns prefixed with `_q_` are excluded from
    `record_data` so the variant holds only original source columns.
    """
    record_cols = [c for c in rejected.columns if not c.startswith("_q_")]
    struct_expr = "struct(" + ", ".join(record_cols) + ")"
    return rejected.select(
        F.lit(source_table).alias("source_table"),
        F.lit(source_system).alias("source_system"),
        F.col("_q_reason").alias("rejection_reason"),
        # Complete original record as VARIANT; NULL fields are kept explicitly
        F.expr(
            f"parse_json(to_json({struct_expr}, Map('ignoreNullFields', 'false')))"
        ).alias("record_data"),
        (F.col(source_file) if source_file else F.lit(None).cast("string")).alias("source_file"),
        (F.col(ingestion_ts) if ingestion_ts else F.lit(None).cast("timestamp")).alias("ingestion_timestamp"),
        F.current_timestamp().alias("quarantined_at"),
    )


@dp.materialized_view(
    name="retail_silver.quarantine_records",
    comment="Centralized quarantine: records rejected by enforceable (expect_or_drop) "
            "data-quality rules across all five Silver source tables. Warning-only "
            "violations are NOT captured (they remain in Silver). Full recompute each "
            "refresh — no duplicate quarantine records on reruns.",
)
def quarantine_records():
    # ── 1) Blob Storage — transactions ────────────────────────────────────
    txn = (
        spark.read.table("blob_bronze.transactions")
        .withColumn(
            "_q_parsed_ts",
            F.expr("try_to_timestamp(transaction_timestamp, 'dd-MMM-yyyy hh.mm.ss a')"),
        )
        .filter(F.col("transaction_id").isNull() | F.col("_q_parsed_ts").isNull())
        .withColumn(
            "_q_reason",
            F.when(F.col("transaction_id").isNull(), F.lit("NULL_TRANSACTION_ID"))
            .when(F.col("_q_parsed_ts").isNull(), F.lit("UNPARSEABLE_TIMESTAMP"))
            .otherwise(F.lit("UNKNOWN")),
        )
    )
    txn_q = _to_quarantine(
        txn, "transactions", "Blob Storage",
        source_file="_source_file", ingestion_ts="_ingestion_timestamp",
    )

    # ── 2) PostgreSQL — product_catalog ──────────────────────────────────
    pc = (
        spark.read.table("postgres_bronze.product_catalog")
        .filter(
            (F.col("product_id").isNull() | (F.length(F.trim(F.col("product_id"))) == 0))
            | (F.col("product_name").isNull() | (F.length(F.trim(F.col("product_name"))) == 0))
            | F.col("launch_date").isNull()
        )
        .withColumn(
            "_q_reason",
            F.when(
                F.col("product_id").isNull() | (F.length(F.trim(F.col("product_id"))) == 0),
                F.lit("NULL_PRODUCT_ID"),
            )
            .when(
                F.col("product_name").isNull() | (F.length(F.trim(F.col("product_name"))) == 0),
                F.lit("NULL_PRODUCT_NAME"),
            )
            .when(F.col("launch_date").isNull(), F.lit("NULL_LAUNCH_DATE"))
            .otherwise(F.lit("UNKNOWN")),
        )
    )
    pc_q = _to_quarantine(pc, "product_catalog", "PostgreSQL")

    # ── 3) Salesforce — account ───────────────────────────────────────────
    acct = (
        spark.read.table("saleforce_bronze.account")
        .filter(F.col("Id").isNull())
        .withColumn("_q_reason", F.lit("NULL_ID"))
    )
    acct_q = _to_quarantine(acct, "account", "Salesforce")

    # ── 4) Salesforce — opportunity ────────────────────────────────────────
    opp = (
        spark.read.table("saleforce_bronze.opportunity")
        .filter(F.col("Id").isNull())
        .withColumn("_q_reason", F.lit("NULL_ID"))
    )
    opp_q = _to_quarantine(opp, "opportunity", "Salesforce")

    # ── 5) PostgreSQL — inventory ────────────────────────────────────────
    inv = (
        spark.read.table("postgres_bronze.inventory")
        .filter(F.col("inventory_id").isNull())
        .withColumn("_q_reason", F.lit("NULL_INVENTORY_ID"))
    )
    inv_q = _to_quarantine(inv, "inventory", "PostgreSQL")

    return (
        txn_q
        .unionByName(pc_q)
        .unionByName(acct_q)
        .unionByName(opp_q)
        .unionByName(inv_q)
    )
