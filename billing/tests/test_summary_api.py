import datetime
from django.test import TestCase
from django.urls import reverse
from billing.models import (
    BillingBlobSource,
    CostReportSnapshot,
    Customer,
    Subscription,
    Resource,
    Meter,
    CostEntry,
)


class SubscriptionSummaryAPITests(TestCase):
    def setUp(self):
        self.source1 = BillingBlobSource.objects.create(name='s1', base_folder='f1')
        self.source2 = BillingBlobSource.objects.create(name='s2', base_folder='f2')
        customer = Customer.objects.create(tenant_id='t1')
        self.sub1 = Subscription.objects.create(subscription_id='sub1', name='Sub1', customer=customer)
        self.sub2 = Subscription.objects.create(subscription_id='sub2', name='Sub2', customer=customer)
        self.res = Resource.objects.create(resource_id='r1')
        self.meter = Meter.objects.create(meter_id='m1', name='m1', category='Virtual Machines', unit='h')
        date = datetime.date(2024, 1, 1)
        snap1 = CostReportSnapshot.objects.create(run_id='r1', report_date=date, file_name='f1', source=self.source1, status=CostReportSnapshot.Status.COMPLETE)
        snap2 = CostReportSnapshot.objects.create(run_id='r2', report_date=date, file_name='f2', source=self.source2, status=CostReportSnapshot.Status.COMPLETE)
        CostEntry.objects.create(snapshot=snap1, date=date, subscription=self.sub1, resource=self.res, meter=self.meter, cost_in_usd=1, quantity=1, unit_price=1)
        CostEntry.objects.create(snapshot=snap2, date=date, subscription=self.sub2, resource=self.res, meter=self.meter, cost_in_usd=2, quantity=1, unit_price=1)

    def test_subscription_summary(self):
        url = '/api/costs/subscription-summary/?date=2024-01-01'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()['data']
        sub_ids = {item['subscription_id'] for item in data}
        self.assertEqual(sub_ids, {'sub1', 'sub2'})

    def test_filter_by_subscription(self):
        url = '/api/costs/subscription-summary/?date=2024-01-01&subscription_id=sub1'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        data = resp.json()['data']
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['subscription_id'], 'sub1')

    def test_latest_snapshot_per_subscription(self):
        date = datetime.date(2024, 1, 1)
        newer = CostReportSnapshot.objects.create(
            run_id='r3',
            report_date=date,
            file_name='f3',
            source=self.source1,
            status=CostReportSnapshot.Status.COMPLETE,
        )
        CostEntry.objects.create(
            snapshot=newer,
            date=date,
            subscription=self.sub1,
            resource=self.res,
            meter=self.meter,
            cost_in_usd=5,
            quantity=1,
            unit_price=1,
        )

        resp = self.client.get('/api/costs/subscription-summary/?date=2024-01-01')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()['data']
        self.assertEqual(len(data), 2)
        for item in data:
            if item['subscription_id'] == 'sub1':
                self.assertEqual(item['total_usd'], '5.0000')


