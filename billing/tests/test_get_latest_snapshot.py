import datetime
from django.test import TestCase
from billing.utils import get_latest_snapshot_for_date, get_cost_entries_for_date
from billing.models import CostReportSnapshot, CostEntry, Subscription, Resource, Meter, Customer, BillingBlobSource

class GetLatestSnapshotForDateTest(TestCase):
    def setUp(self):
        # Create a customer
        self.customer = Customer.objects.create(
            tenant_id="test-tenant",
            name="Test Customer"
        )

        # Create a subscription, resource, and meter
        self.subscription = Subscription.objects.create(
            subscription_id="test-subscription",
            name="Test Subscription",
            customer=self.customer
        )
        self.resource = Resource.objects.create(
            resource_id="test-resource",
            resource_name="Test Resource",
            name="Test Resource",
            resource_group="Test Resource Group",
            location="Test Location"
        )
        self.meter = Meter.objects.create(
            meter_id="test-meter",
            name="Test Meter",
            category="Test Category",
            subcategory="Test Subcategory",
            service_family="Test Service Family",
            unit="Test Unit"
        )

        # Create a billing source
        self.source = BillingBlobSource.objects.create(
            name="test-source",
            base_folder="test/folder",
            is_active=True
        )

        # Create a snapshot
        self.snapshot = CostReportSnapshot.objects.create(
            run_id="test-run",
            report_date=datetime.date(2023, 1, 1),
            file_name="test-file.csv",
            source=self.source,
            status=CostReportSnapshot.Status.COMPLETE
        )

        # Create cost entries for January 1, 2023
        CostEntry.objects.create(
            snapshot=self.snapshot,
            date=datetime.date(2023, 1, 1),
            subscription=self.subscription,
            resource=self.resource,
            meter=self.meter,
            cost_in_usd=100.00,
            cost_in_billing_currency=100.00,
            billing_currency="USD",
            quantity=1.0,
            unit_price=100.00,
            payg_price=100.00,
            pricing_model="Pay-As-You-Go",
            charge_type="Usage",
            publisher_name="Test Publisher"
        )

    def test_no_entries_for_date(self):
        """Test that None is returned when there are no entries for a date."""
        # Try to get snapshot for a date with no entries
        result = get_latest_snapshot_for_date(datetime.date(2023, 1, 2))
        self.assertIsNone(result, "Should return None when no entries exist for date")

    def test_with_entries_for_date(self):
        """Test that a snapshot is returned when entries exist for a date."""
        # Try to get snapshot for a date with entries
        result = get_latest_snapshot_for_date(datetime.date(2023, 1, 1))
        self.assertIsNotNone(result, "Should return a snapshot when entries exist for date")
        self.assertEqual(result.id, self.snapshot.id, "Should return the correct snapshot")

    def test_multiple_snapshots_for_same_date(self):
        """Test that the latest snapshot is returned when there are multiple snapshots for the same date."""
        # Create another snapshot for the same date
        snapshot2 = CostReportSnapshot.objects.create(
            run_id="test-run-2",
            report_date=datetime.date(2023, 1, 1),
            file_name="test-file-2.csv",
            source=self.source,
            status=CostReportSnapshot.Status.COMPLETE
        )

        # Create cost entries for the second snapshot
        CostEntry.objects.create(
            snapshot=snapshot2,
            date=datetime.date(2023, 1, 1),
            subscription=self.subscription,
            resource=self.resource,
            meter=self.meter,
            cost_in_usd=200.00,
            cost_in_billing_currency=200.00,
            billing_currency="USD",
            quantity=2.0,
            unit_price=100.00,
            payg_price=100.00,
            pricing_model="Pay-As-You-Go",
            charge_type="Usage",
            publisher_name="Test Publisher"
        )

        # Try to get snapshot for January 1, 2023
        result = get_latest_snapshot_for_date(datetime.date(2023, 1, 1))
        self.assertIsNotNone(result, "Should return a snapshot when entries exist for date")
        self.assertEqual(result.id, snapshot2.id, "Should return the latest snapshot")

    def test_get_cost_entries_for_date(self):
        """Test that get_cost_entries_for_date returns the correct entries."""
        # Get cost entries for January 1, 2023
        entries = get_cost_entries_for_date(datetime.date(2023, 1, 1))
        self.assertEqual(entries.count(), 1, "Should return one entry for the date")
        self.assertEqual(entries.first().cost_in_usd, 100.00, "Should return the correct entry")

        # Test with no entries for a date
        entries = get_cost_entries_for_date(datetime.date(2023, 1, 2))
        self.assertEqual(entries.count(), 0, "Should return no entries for a date with no entries")
