# ── PostgreSQL CDC Limitation ──────────────────────────────────────────
# The PostgreSQL managed ingestion pipeline uses cursor-based incremental
# extraction. This captures INSERT and UPDATE operations but does NOT
# propagate DELETE operations from the source. If a record is deleted in
# PostgreSQL, it will persist in the Bronze streaming table and continue
# to appear in Silver.
#
# SCD Type: 1 (upsert — latest state only, no history)
# Delete propagation: NOT SUPPORTED by current connector configuration
#
# Recommended mitigation: Periodic reconciliation comparing source keys
# with Bronze keys to identify and remove orphaned records. This requires
# direct source access to the PostgreSQL table.
#
# Last verified: Oct 22, 2026 — no DELETE operations observed in Bronze
# table history (3 MERGE operations, 0 DELETE).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="retail_silver.inventory",
    comment="Inventory data with profiled columns and data quality checks"
)
@dp.expect_or_drop("non-null inventory_id", "inventory_id IS NOT NULL")
@dp.expect("valid stock_quantity", "stock_quantity > 0")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("non-null store_id", "store_id IS NOT NULL")
def inventory_clean():
    # Read source table as batch (materialized view): picks up all updates/deletes
    # from the CDC-style bronze target on each pipeline refresh
    source_df = spark.read.table("postgres_bronze.inventory")
    
    # Select relevant columns with lowercase naming
    return (
        source_df.select(
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),
            F.when(
                F.col("stock_quantity") < F.col("reorder_level"),
                "LOW_STOCK"
            ).otherwise("HEALTHY").alias("inventory_status"),
            F.col("warehouse_location"),
            F.col("last_stock_update")
        )
    )