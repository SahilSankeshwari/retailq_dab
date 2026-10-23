from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="retail_silver.transactions", comment="Transaction data with profiled columns and data quality checks")
@dp.expect_or_drop("non-null transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("parseable transaction_timestamp", "transaction_timestamp IS NOT NULL")
@dp.expect("valid quantity", "quantity > 0")
@dp.expect("valid selling_price", "selling_price >= 0")
@dp.expect("valid discount_amount", "discount_amount >= 0")
@dp.expect("non-null product_id", "product_id IS NOT NULL")
@dp.expect("valid payment_mode", "payment_mode IN ('UPI','Card','Cash','Net Banking')")
def transactions_clean():
    # Read the original bronze table as an append-only stream: the CSV
    # source only ever INSERTs new files/rows (verified via DESCRIBE HISTORY
    # — create + blind appends only, written by the 01_blob_to_bronze
    # notebook), so a streaming table is the correct incremental consumer.
    # Each pipeline update processes only new rows appended by the notebook
    # instead of recomputing the full history like a materialized view.
    source_df = spark.readStream.table("blob_bronze.transactions")

    # Select relevant columns, excluding _rescued_data
    return source_df.select(
        F.col("transaction_id"),
        F.col("opportunity_name"),
        F.col("product_id"),
        F.col("store_id"),
        F.col("quantity").cast("int"),
        F.col("selling_price").cast("int"),
        (F.col("quantity").cast("int") * F.col("selling_price").cast("int")).alias("gross_amount"),
        F.col("discount_amount").cast("int"),
        F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
        F.col("payment_mode"),
        F.col("sales_channel"),
    )


# ── Quarantine ────────────────────────────────────────────────────────
# The transactions-specific quarantine table (quarantine_transactions)
# has been replaced by the centralized quarantine_records materialized
# view in quarantine.py, which captures enforceable-rule rejections from
# all five Silver source tables (transactions included).
