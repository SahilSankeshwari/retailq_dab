import os
from datetime import date

import pytest
from product_transform import transform_product_catalog
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.mark.skipif(
    os.getenv("DATABRICKS_TESTS") != "1",
    reason="Requires Databricks serverless compute",
)
def test_product_catalog_standardization(spark):
    schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("supplier_name", StringType(), True),
            StructField("launch_date", DateType(), True),
            StructField("__START_AT", StringType(), True),
            StructField("__END_AT", StringType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )

    source = spark.createDataFrame(
        [
            (
                " p-101 ",
                "  WIRELESS mouse ",
                " computer accessories ",
                None,
                None,
                123.456,
                " acme supplier ",
                date(2026, 1, 1),
                "v1",
                None,
                None,
            ),
            (
                "p-102",
                "Premium Keyboard",
                "Electronics",
                "Keyboards",
                "Acme",
                60001.0,
                "Acme Supplier",
                date(2026, 2, 1),
                "v2",
                "v3",
                None,
            ),
        ],
        schema=schema,
    )

    result = transform_product_catalog(source).collect()

    assert result[0]["product_id"] == "P-101"
    assert result[0]["product_name"] == "Wireless Mouse"
    assert result[0]["subcategory"] == "Unknown"
    assert result[0]["brand"] == "Unknown"
    assert result[0]["unit_price"] == 123.46
    assert result[0]["product_segment"] == "BUDGET"
    assert result[0]["is_active"] is True

    assert result[1]["product_segment"] == "PREMIUM"
    assert result[1]["is_active"] is False
