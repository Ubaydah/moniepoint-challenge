from tests.helpers import act
import pytest

from src.analytics import get_top_merchant


class TestTopMerchant:
    def test_returns_highest_volume_merchant(self, db):
        rows = [
            act(merchant_id="MRC-000001", product="POS", status="SUCCESS", amount=5000.00),
            act(merchant_id="MRC-000001", product="POS", status="SUCCESS", amount=3000.00),
            act(merchant_id="MRC-000002", product="POS", status="SUCCESS", amount=10000.00),
        ]
        db.add_all(rows)
        db.flush()

        result = get_top_merchant(db)
        assert result["merchant_id"] == "MRC-000002"
        assert result["total_volume"] == pytest.approx(10000.00, rel=1e-2)

    def test_only_counts_success_status(self, db):
        db.add_all([
            act(merchant_id="MRC-AAA", product="POS", status="FAILED", amount=99999.00),
            act(merchant_id="MRC-BBB", product="POS", status="SUCCESS", amount=1.00),
        ])
        db.flush()

        result = get_top_merchant(db)
        assert result["merchant_id"] == "MRC-BBB"

    def test_empty_db_returns_defaults(self, db):
        result = get_top_merchant(db)
        assert result["merchant_id"] is None
        assert result["total_volume"] == 0.00

    def test_volume_rounded_to_2_decimals(self, db):
        db.add(act(merchant_id="MRC-RND", product="POS", status="SUCCESS", amount=100.555))
        db.flush()

        result = get_top_merchant(db)
        assert isinstance(result["total_volume"], float)
