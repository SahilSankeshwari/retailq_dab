from cleaning import standardize_product_id


def test_product_id_is_uppercase():
    assert standardize_product_id("prod-101") == "PROD-101"


def test_product_id_removes_whitespace():
    assert standardize_product_id("  prod-101  ") == "PROD-101"


def test_product_id_handles_null():
    assert standardize_product_id(None) is None
