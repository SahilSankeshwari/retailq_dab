import pytest
from data_quality import valid_transaction_amount


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        (100, True),
        (0, True),
        (-25, False),
        (None, False),
    ],
)
def test_valid_transaction_amount(amount, expected):
    assert valid_transaction_amount(amount) is expected
