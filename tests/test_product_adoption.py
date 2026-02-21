from tests.helpers import act
from src.analytics import get_product_adoption


class TestProductAdoption:
    def test_returns_all_known_products(self, db):
        db.add(act(merchant_id="MRC-Z1", product="POS", status="SUCCESS"))
        db.flush()

        result = get_product_adoption(db)
        expected_products = {"POS", "AIRTIME", "BILLS", "CARD_PAYMENT", "SAVINGS", "MONIEBOOK", "KYC"}
        assert set(result.keys()) == expected_products

    def test_counts_unique_merchants(self, db):
        db.add_all([
            act(merchant_id="MRC-P1", product="AIRTIME", status="SUCCESS"),
            act(merchant_id="MRC-P1", product="AIRTIME", status="FAILED"),  
            act(merchant_id="MRC-P2", product="AIRTIME", status="SUCCESS"),
        ])
        db.flush()

        result = get_product_adoption(db)
        assert result["AIRTIME"] == 2

    def test_sorted_descending_by_count(self, db):
        db.add_all([
            act(merchant_id="MRC-S1", product="BILLS", status="SUCCESS"),
            act(merchant_id="MRC-S2", product="BILLS", status="SUCCESS"),
            act(merchant_id="MRC-S3", product="BILLS", status="SUCCESS"),
            act(merchant_id="MRC-S1", product="SAVINGS", status="SUCCESS"),
        ])
        db.flush()

        result = get_product_adoption(db)
        counts = list(result.values())
        for i in range(len(counts) - 1):
            assert counts[i] >= counts[i + 1]

    def test_no_status_filter_applied(self, db):
        """Product adoption counts all merchants regardless of status."""
        db.add_all([
            act(merchant_id="MRC-F1", product="KYC", status="FAILED"),
            act(merchant_id="MRC-F2", product="KYC", status="PENDING"),
        ])
        db.flush()

        result = get_product_adoption(db)
        assert result["KYC"] == 2
