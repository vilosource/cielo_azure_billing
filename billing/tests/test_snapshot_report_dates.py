import datetime
from django.test import TestCase
from billing.models import CostReportSnapshot


class SnapshotReportDatesAPITests(TestCase):
    def setUp(self):
        CostReportSnapshot.objects.create(
            run_id='r1',
            report_date=datetime.date(2025, 5, 1),
            file_name='f1',
            status=CostReportSnapshot.Status.COMPLETE,
        )
        CostReportSnapshot.objects.create(
            run_id='r2',
            report_date=datetime.date(2025, 5, 10),
            file_name='f2',
            status=CostReportSnapshot.Status.COMPLETE,
        )
        # duplicate date should be collapsed
        CostReportSnapshot.objects.create(
            run_id='r3',
            report_date=datetime.date(2025, 5, 10),
            file_name='f3',
            status=CostReportSnapshot.Status.COMPLETE,
        )
        # incomplete snapshot ignored
        CostReportSnapshot.objects.create(
            run_id='r4',
            report_date=datetime.date(2025, 5, 15),
            file_name='f4',
            status=CostReportSnapshot.Status.IN_PROGRESS,
        )
        # null report_date ignored
        CostReportSnapshot.objects.create(
            run_id='r5',
            report_date=None,
            file_name='f5',
            status=CostReportSnapshot.Status.COMPLETE,
        )

    def test_available_snapshot_report_dates(self):
        resp = self.client.get('/api/snapshots/available-report-dates/')
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(
            data['available_report_dates'],
            ['2025-05-01', '2025-05-10'],
        )

