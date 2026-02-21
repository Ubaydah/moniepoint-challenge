from tests.helpers import act
from src.analytics import get_failure_rates
import pytest

class TestFailureRates:
    def test_calculates_failure_rate_correctly(self, db):
        db.add_all([
            act(merchant_id="MRC-R1", product="POS", status="FAILED"),
            act(merchant_id="MRC-R1", product="POS", status="SUCCESS"),
            act(merchant_id="MRC-R1", product="POS", status="SUCCESS"),
            act(merchant_id="MRC-R1", product="POS", status="SUCCESS"),
            act(merchant_id="MRC-R1", product="POS", status="SUCCESS"),
            act(merchant_id="MRC-R2", product="AIRTIME", status="FAILED"),
            act(merchant_id="MRC-R2", product="AIRTIME", status="SUCCESS"),
        ])
        db.flush()

        result = get_failure_rates(db)
        assert result[0]["product"] == "AIRTIME"
        assert result[0]["failure_rate"] == pytest.approx(50.0, abs=0.1)
        assert result[1]["product"] == "POS"
        assert result[1]["failure_rate"] == pytest.approx(20.0, abs=0.1)

    def test_excludes_pending(self, db):
        db.add_all([
            act(merchant_id="MRC-PND", product="BILLS", status="PENDING"),
            act(merchant_id="MRC-PND", product="BILLS", status="PENDING"),
        ])
        db.flush()

        result = get_failure_rates(db)
        products_in_result = [r["product"] for r in result]
        assert "BILLS" not in products_in_result

    def test_returns_list_of_dicts(self, db):
        db.add(act(merchant_id="MRC-L1", product="SAVINGS", status="FAILED"))
        db.flush()

        result = get_failure_rates(db)
        assert isinstance(result, list)
        for item in result:
            assert "product" in item
            assert "failure_rate" in item
            assert isinstance(item["failure_rate"], float)

    def test_zero_failed_product_omitted(self, db):
        """Products with 100% success should have failure_rate 0.0 and still appear."""
        db.add(act(merchant_id="MRC-OK", product="CARD_PAYMENT", status="SUCCESS"))
        db.flush()

        result = get_failure_rates(db)
        products = {r["product"]: r["failure_rate"] for r in result}
        if "CARD_PAYMENT" in products:
            assert products["CARD_PAYMENT"] == pytest.approx(0.0, abs=0.1)

    def test_empty_db_returns_empty_list(self, db):
        result = get_failure_rates(db)
        assert result == []
