import datetime
from django.test import TestCase
from billing.models import BillingBlobSource, CostReportSnapshot, Subscription, Customer, Resource, Meter, CostEntry

class AvailableReportDatesTests(TestCase):
    def setUp(self):
        self.source1 = BillingBlobSource.objects.create(name='s1', base_folder='f1')
        self.source2 = BillingBlobSource.objects.create(name='s2', base_folder='f2')
        customer = Customer.objects.create(tenant_id='t1')
        self.sub = Subscription.objects.create(subscription_id='sub1', name='Sub1', customer=customer)
        self.res = Resource.objects.create(resource_id='r1')
        self.meter = Meter.objects.create(meter_id='m1', name='m1', category='cat', unit='u')

        # older snapshot for source1
        snap_old = CostReportSnapshot.objects.create(run_id='old', report_date=datetime.date(2024,1,2), file_name='f1', source=self.source1, status=CostReportSnapshot.Status.COMPLETE)
        CostEntry.objects.create(snapshot=snap_old, date=datetime.date(2024,1,2), subscription=self.sub, resource=self.res, meter=self.meter, cost_in_usd=1, quantity=1, unit_price=1)

        # latest snapshot for source1
        snap_new = CostReportSnapshot.objects.create(run_id='new', report_date=datetime.date(2024,1,8), file_name='f2', source=self.source1, status=CostReportSnapshot.Status.COMPLETE)
        CostEntry.objects.create(snapshot=snap_new, date=datetime.date(2024,1,8), subscription=self.sub, resource=self.res, meter=self.meter, cost_in_usd=1, quantity=1, unit_price=1)

        # snapshot for source2
        snap2 = CostReportSnapshot.objects.create(run_id='s2', report_date=datetime.date(2024,1,5), file_name='f3', source=self.source2, status=CostReportSnapshot.Status.COMPLETE)
        CostEntry.objects.create(snapshot=snap2, date=datetime.date(2024,1,5), subscription=self.sub, resource=self.res, meter=self.meter, cost_in_usd=2, quantity=1, unit_price=1)

    def test_available_dates_for_month(self):
        resp = self.client.get('/api/costs/available-report-dates/?month=2024-01')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data['month'], '2024-01')
        self.assertEqual(data['available_dates'], ['2024-01-05', '2024-01-08'])

