import pytest

from cleaning import standardize_product_id


@pytest.mark.parametrize(
    ("input_id", "expected"),
    [
        ("prod-101", "PROD-101"),
        ("  prod-202  ", "PROD-202"),
        ("AbC-303", "ABC-303"),
        ("", ""),
        (None, None),
    ],
)
def test_standardize_product_id(input_id, expected):
    assert standardize_product_id(input_id) == expected
