# ── PostgreSQL CDC Limitation ──────────────────────────────────────────
# The PostgreSQL managed ingestion pipeline uses cursor-based incremental
# extraction. This captures INSERT and UPDATE operations but does NOT
# propagate DELETE operations from the source. If a record is deleted in
# PostgreSQL, it will persist in the Bronze streaming table and continue
# to appear in Silver (including as an active SCD Type 2 version).
#
# SCD Type: 2 (full history with __START_AT/__END_AT)
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
    name="retail_silver.product_catalog",
    comment="Silver layer product catalog with standardized data and data quality rules"
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
@dp.expect("valid_category", "category IS NOT NULL")
@dp.expect("valid_price", "unit_price > 0")
@dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
@dp.expect("valid_supplier", "supplier_name IS NOT NULL")
def product_catalog():
    return (
        spark.read.table("postgres_bronze.product_catalog")
        .select(
            # Standardize product_id: trim and uppercase
            F.upper(F.trim(F.col("product_id"))).alias("product_id"),
            
            # Standardize product_name: trim and title case
            F.initcap(F.trim(F.col("product_name"))).alias("product_name"),
            
            # Standardize category: trim and title case
            F.initcap(F.trim(F.col("category"))).alias("category"),
            
            # Standardize subcategory: trim and title case, handle nulls
            F.when(F.col("subcategory").isNotNull(), 
                   F.initcap(F.trim(F.col("subcategory"))))
             .otherwise(F.lit("Unknown")).alias("subcategory"),
            
            # Standardize brand: trim and title case, handle nulls
            F.when(F.col("brand").isNotNull(), 
                   F.initcap(F.trim(F.col("brand"))))
             .otherwise(F.lit("Unknown")).alias("brand"),
            
            # Standardize price: round to 2 decimal places
            F.round(F.col("unit_price"), 2).alias("unit_price"),
            
            # Standardize supplier_name: trim and title case
            F.initcap(F.trim(F.col("supplier_name"))).alias("supplier_name"),
            
            # Keep dates and timestamps as-is
            F.col("launch_date"),
            F.when(F.col("unit_price") > 50000, "PREMIUM")
         .when(F.col("unit_price") > 10000, "MID_RANGE")
         .otherwise("BUDGET")
         .alias("product_segment"),
            
            # Keep CDC tracking columns with correct names
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),
            
            # Derive is_active from end_at: null means current/active record
            F.when(F.col("__END_AT").isNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_active"),
            
            # Keep updated_at timestamp
            F.col("updated_at"),
            
            # Add processing timestamp for audit trail
            F.current_timestamp().alias("processed_at")
        )
    )