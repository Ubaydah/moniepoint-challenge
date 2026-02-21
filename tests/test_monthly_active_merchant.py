from tests.helpers import _ts, act
from src.analytics import get_monthly_active_merchants

class TestMonthlyActiveMerchants:
    def test_counts_unique_merchants_per_month(self, db):
        db.add_all([
            # January — 2 unique merchants
            act(merchant_id="MRC-01", product="POS", status="SUCCESS", event_timestamp=_ts(2024, 1, 5)),
            act(merchant_id="MRC-01", product="POS", status="SUCCESS", event_timestamp=_ts(2024, 1, 10)),  
            act(merchant_id="MRC-02", product="POS", status="SUCCESS", event_timestamp=_ts(2024, 1, 15)),
            # February — 1 unique merchant
            act(merchant_id="MRC-01", product="POS", status="SUCCESS", event_timestamp=_ts(2024, 2, 1)),
        ])
        db.flush()

        result = get_monthly_active_merchants(db)
        assert result["2024-01"] == 2
        assert result["2024-02"] == 1

    def test_excludes_failed_and_pending(self, db):
        db.add_all([
            act(merchant_id="MRC-X1", product="POS", status="FAILED", event_timestamp=_ts(2024, 3, 1)),
            act(merchant_id="MRC-X2", product="POS", status="PENDING", event_timestamp=_ts(2024, 3, 1)),
        ])
        db.flush()

        result = get_monthly_active_merchants(db)
        assert result.get("2024-03", 0) == 0

    def test_non_active_months_included_as_zero(self, db):
        """Months between min and max with zero activity should still appear as 0."""
        db.add_all([
            act(merchant_id="MRC-G", product="POS", status="SUCCESS", event_timestamp=_ts(2024, 1, 1)),
            act(merchant_id="MRC-G", product="POS", status="SUCCESS", event_timestamp=_ts(2024, 3, 1)),
        ])
        db.flush()

        result = get_monthly_active_merchants(db)
        assert "2024-02" in result
        assert result["2024-02"] == 0

    def test_empty_db_returns_empty_dict(self, db):
        result = get_monthly_active_merchants(db)
        assert result == {}

