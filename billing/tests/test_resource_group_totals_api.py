import datetime
from django.test import TestCase
from billing.models import (
    BillingBlobSource,
    CostReportSnapshot,
    Customer,
    Subscription,
    Resource,
    Meter,
    CostEntry,
)


class ResourceGroupTotalsAPITests(TestCase):
    def setUp(self):
        self.source1 = BillingBlobSource.objects.create(name='s1', base_folder='f1')
        customer = Customer.objects.create(tenant_id='t1')
        self.sub = Subscription.objects.create(subscription_id='sub1', name='Sub1', customer=customer)
        self.res1 = Resource.objects.create(resource_id='r1', resource_group='rg1', resource_name='res1')
        self.res2 = Resource.objects.create(resource_id='r2', resource_group='rg1', resource_name='res2')
        self.meter = Meter.objects.create(meter_id='m1', name='m1', category='cat', unit='u')
        date = datetime.date(2025, 5, 28)
        snap = CostReportSnapshot.objects.create(run_id='run', report_date=date, file_name='f', source=self.source1, status=CostReportSnapshot.Status.COMPLETE)
        CostEntry.objects.create(snapshot=snap, date=date, subscription=self.sub, resource=self.res1, meter=self.meter, cost_in_usd=5, quantity=1, unit_price=1)
        CostEntry.objects.create(snapshot=snap, date=date, subscription=self.sub, resource=self.res2, meter=self.meter, cost_in_usd=3, quantity=1, unit_price=1)

    def test_totals(self):
        url = '/api/costs/resource-group-totals/?resource_group=rg1&date=2025-05-28'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data['resource_group'], 'rg1')
        names = {d['resource_name'] for d in data['data']}
        self.assertEqual(names, {'res1', 'res2'})

    def test_missing_resource_group(self):
        resp = self.client.get('/api/costs/resource-group-totals/')
        self.assertEqual(resp.status_code, 400)

