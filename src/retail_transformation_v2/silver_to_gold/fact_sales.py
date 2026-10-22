# ── Opportunity Join Risk (Documented Limitation) ──────────────────────
# The transaction CSV source does NOT include opportunity_id. The join
# to silver.opportunity uses normalized opportunity name:
#   upper(trim(transaction.opportunity_name)) == upper(trim(opportunity.name))
#
# Risk: If two different opportunities share the same normalized name,
# the deduplication guard (row_number by last_modified_date DESC) keeps
# only the most recently modified one, potentially associating
# transactions with the wrong opportunity.
#
# The dedup guard prevents row fanout (duplicate fact rows) but does NOT
# guarantee join correctness when names collide.
#
# Recommended fix: Coordinate with the source owner to add a stable
# opportunity_id to the CSV export. This would allow a direct join on
# opportunity_id instead of name.
#
# Current status: WORKING with clean data (0 unmatched, 0 duplicates).
# Last verified: Oct 22, 2026.

from pyspark.sql.functions import upper, trim, col, row_number
from pyspark.sql.window import Window
from pyspark import pipelines as dp

@dp.table(name="retail_gold.fact_sales")
def fact_sales():
    transactions_df = spark.read.table("retail_silver.transactions")
    opportunity_df = spark.read.table("retail_silver.opportunity")

    # Defensive dedup: transactions join to opportunities by NAME (the only
    # key the CSV source provides). If the same opportunity name ever appears
    # twice, keep only the most recently modified version so the join cannot
    # multiply fact rows. With clean data this is a no-op.
    name_window = Window.partitionBy(upper(trim(col("name")))) \
        .orderBy(col("last_modified_date").desc_nulls_last())
    opportunity_dedup = (
        opportunity_df
        .withColumn("_row_num", row_number().over(name_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    joined_df = transactions_df.alias("t").join(
        opportunity_dedup.alias("o"),
        upper(trim(transactions_df.opportunity_name)) == upper(trim(opportunity_dedup.name)),
        how="left"
    )
    
    # Select important columns (customize as needed)
    selected_df = joined_df.select(
        "t.transaction_id",
        "t.opportunity_name",
        "t.product_id",
        "t.store_id",
        "t.quantity",
        "t.selling_price",
        "t.discount_amount",
        "t.transaction_timestamp",
        col("t.transaction_timestamp").cast("date").alias("transaction_date"),
        "t.payment_mode",
        "t.sales_channel",
        "o.name",
        "o.stage_name",
        "o.owner_id",
        "o.amount",
        col("o.account_id").alias("customer_id")
    )
    
    return selected_df