from tests.helpers import act
from src.analytics import get_kyc_funnel

class TestKycFunnel:
    def test_returns_correct_funnel_counts(self, db):
        db.add_all([
            # 3 submitted docs
            act(merchant_id="MRC-K1", product="KYC", status="SUCCESS", event_type="DOCUMENT_SUBMITTED"),
            act(merchant_id="MRC-K2", product="KYC", status="SUCCESS", event_type="DOCUMENT_SUBMITTED"),
            act(merchant_id="MRC-K3", product="KYC", status="SUCCESS", event_type="DOCUMENT_SUBMITTED"),
            # 2 completed verification
            act(merchant_id="MRC-K1", product="KYC", status="SUCCESS", event_type="VERIFICATION_COMPLETED"),
            act(merchant_id="MRC-K2", product="KYC", status="SUCCESS", event_type="VERIFICATION_COMPLETED"),
            # 1 tier upgrade
            act(merchant_id="MRC-K1", product="KYC", status="SUCCESS", event_type="TIER_UPGRADE"),
        ])
        db.flush()

        result = get_kyc_funnel(db)
        assert result["documents_submitted"] == 3
        assert result["verifications_completed"] == 2
        assert result["tier_upgrades"] == 1

    def test_excludes_failed_kyc_events(self, db):
        db.add_all([
            act(merchant_id="MRC-KF", product="KYC", status="FAILED", event_type="DOCUMENT_SUBMITTED"),
            act(merchant_id="MRC-KF", product="KYC", status="SUCCESS", event_type="VERIFICATION_COMPLETED"),
        ])
        db.flush()

        result = get_kyc_funnel(db)
        assert result["documents_submitted"] == 0
        assert result["verifications_completed"] == 1

    def test_empty_db_returns_zeros(self, db):
        result = get_kyc_funnel(db)
        assert result == {"documents_submitted": 0, "verifications_completed": 0, "tier_upgrades": 0}

    def test_duplicate_merchant_counted_once_per_stage(self, db):
        """A single merchant with multiple DOCUMENT_SUBMITTED events should count once."""
        db.add_all([
            act(merchant_id="MRC-DUP", product="KYC", status="SUCCESS", event_type="DOCUMENT_SUBMITTED"),
            act(merchant_id="MRC-DUP", product="KYC", status="SUCCESS", event_type="DOCUMENT_SUBMITTED"),
        ])
        db.flush()

        result = get_kyc_funnel(db)
        assert result["documents_submitted"] == 1