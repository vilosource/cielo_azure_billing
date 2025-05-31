import datetime
from django.test import TestCase
from billing.utils import get_latest_snapshots, get_latest_snapshots_for_date, get_cost_entries_for_date
from billing.models import CostReportSnapshot, CostEntry, Subscription, Resource, Meter, Customer, BillingBlobSource

class GetLatestSnapshotsTest(TestCase):
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

        # Create a snapshot for January 1, 2023
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

    def test_no_snapshots_for_date(self):
        """Test that no snapshots are returned when there are no entries for a date."""
        # Try to get snapshots for a date with no entries
        snapshots, missing = get_latest_snapshots(datetime.date(2023, 1, 2))
        self.assertEqual(len(snapshots), 0, "Should return no snapshots when no entries exist for date")
        self.assertEqual(len(missing), 1, "Should mark the source as missing")
        self.assertEqual(missing[0]['name'], self.source.name, "Should mark the correct source as missing")
        self.assertEqual(missing[0]['reason'], 'no entries for date', "Should indicate no entries for date")

    def test_with_snapshots_for_date(self):
        """Test that snapshots are returned when entries exist for a date."""
        # Try to get snapshots for a date with entries
        snapshots, missing = get_latest_snapshots(datetime.date(2023, 1, 1))
        self.assertEqual(len(snapshots), 1, "Should return a snapshot when entries exist for date")
        self.assertEqual(snapshots[0].id, self.snapshot.id, "Should return the correct snapshot")
        self.assertEqual(len(missing), 0, "Should have no missing sources")

    def test_snapshot_without_entries_for_date(self):
        """Test that snapshots are skipped when they don't contain entries for the date."""
        # Create a snapshot without entries for January 2, 2023
        snapshot2 = CostReportSnapshot.objects.create(
            run_id="test-run-2",
            report_date=datetime.date(2023, 1, 2),
            file_name="test-file-2.csv",
            source=self.source,
            status=CostReportSnapshot.Status.COMPLETE
        )

        # Create cost entries for January 2, 2023
        CostEntry.objects.create(
            snapshot=snapshot2,
            date=datetime.date(2023, 1, 2),
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

        # Try to get snapshots for January 1, 2023
        snapshots, missing = get_latest_snapshots(datetime.date(2023, 1, 1))
        self.assertEqual(len(snapshots), 1, "Should return a snapshot when entries exist for date")
        self.assertEqual(snapshots[0].id, self.snapshot.id, "Should return the correct snapshot")
        self.assertEqual(len(missing), 0, "Should have no missing sources")

        # Try to get snapshots for January 2, 2023
        snapshots, missing = get_latest_snapshots(datetime.date(2023, 1, 2))
        self.assertEqual(len(snapshots), 1, "Should return a snapshot when entries exist for date")
        self.assertEqual(snapshots[0].id, snapshot2.id, "Should return the correct snapshot")
        self.assertEqual(len(missing), 0, "Should have no missing sources")

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

        # Try to get snapshots for January 1, 2023
        snapshots, missing = get_latest_snapshots(datetime.date(2023, 1, 1))
        self.assertEqual(len(snapshots), 1, "Should return a snapshot when entries exist for date")
        self.assertEqual(snapshots[0].id, snapshot2.id, "Should return the latest snapshot")
        self.assertEqual(len(missing), 0, "Should have no missing sources")

    def test_get_cost_entries_for_date(self):
        """Test that get_cost_entries_for_date returns the correct entries."""
        # Get cost entries for January 1, 2023
        entries = get_cost_entries_for_date(datetime.date(2023, 1, 1))
        self.assertEqual(entries.count(), 1, "Should return one entry for the date")
        self.assertEqual(entries.first().cost_in_usd, 100.00, "Should return the correct entry")

        # Test with no entries for a date
        entries = get_cost_entries_for_date(datetime.date(2023, 1, 2))
        self.assertEqual(entries.count(), 0, "Should return no entries for a date with no entries")
