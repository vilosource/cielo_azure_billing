import json
import datetime
from django.test import TestCase
from billing.services import CostCsvImporter
from billing.models import CostReportSnapshot, Subscription, CostEntry, Resource

class CostCsvImporterTests(TestCase):
    def _write_csv(self, path, rows):
        import csv
        fieldnames = [
            'customerTenantId', 'SubscriptionId', 'subscriptionName', 'ResourceId',
            'productOrderName', 'resourceGroupName', 'resourceLocation',
            'meterId', 'meterName', 'meterCategory', 'meterSubCategory',
            'serviceFamily', 'unitOfMeasure', 'date', 'costInUsd'
        ]
        with open(path, 'w', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def _write_manifest(self, path, run_id, end_date):
        data = {
            'runInfo': {
                'runId': run_id,
                'endDate': f"{end_date.isoformat()}T00:00:00Z"
            }
        }
        with open(path, 'w') as fh:
            json.dump(data, fh)

    def test_multi_subscription_import(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = f"{tmp}/cost.csv"
            manifest_path = f"{tmp}/manifest.json"
            self._write_csv(csv_path, [
                {
                    'customerTenantId': 't1',
                    'SubscriptionId': 'sub1',
                    'subscriptionName': 'Sub One',
                    'ResourceId': '/r1',
                    'productOrderName': 'prod',
                    'resourceGroupName': 'rg',
                    'resourceLocation': 'loc',
                    'meterId': 'm1',
                    'meterName': 'Meter',
                    'meterCategory': 'cat',
                    'meterSubCategory': 'sub',
                    'serviceFamily': 'fam',
                    'unitOfMeasure': 'u',
                    'date': '01/01/2024',
                    'costInUsd': '1'
                },
                {
                    'customerTenantId': 't1',
                    'SubscriptionId': 'sub2',
                    'subscriptionName': 'Sub Two',
                    'ResourceId': '/r2',
                    'productOrderName': 'prod',
                    'resourceGroupName': 'rg',
                    'resourceLocation': 'loc',
                    'meterId': 'm1',
                    'meterName': 'Meter',
                    'meterCategory': 'cat',
                    'meterSubCategory': 'sub',
                    'serviceFamily': 'fam',
                    'unitOfMeasure': 'u',
                    'date': '01/01/2024',
                    'costInUsd': '2'
                },
            ])
            run_id = 'run1'
            self._write_manifest(manifest_path, run_id, datetime.date(2024,1,1))
            importer = CostCsvImporter(csv_path, run_id=run_id, report_date=datetime.date(2024,1,1))
            importer.import_file()

            self.assertEqual(CostReportSnapshot.objects.count(), 1)
            self.assertEqual(Subscription.objects.count(), 2)
            self.assertEqual(CostEntry.objects.count(), 2)

            snap = CostReportSnapshot.objects.first()
            self.assertEqual(snap.status, CostReportSnapshot.Status.COMPLETE)

    def test_latest_per_subscription(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            # first snapshot with sub1 and sub2
            csv1 = f"{tmp}/c1.csv"
            self._write_csv(csv1, [{
                'customerTenantId': 't1',
                'SubscriptionId': 'sub1',
                'subscriptionName': 'Sub One',
                'ResourceId': '/r1',
                'productOrderName': 'prod',
                'resourceGroupName': 'rg',
                'resourceLocation': 'loc',
                'meterId': 'm1',
                'meterName': 'Meter',
                'meterCategory': 'cat',
                'meterSubCategory': 'sub',
                'serviceFamily': 'fam',
                'unitOfMeasure': 'u',
                'date': '01/01/2024',
                'costInUsd': '1'
            }, {
                'customerTenantId': 't1',
                'SubscriptionId': 'sub2',
                'subscriptionName': 'Sub Two',
                'ResourceId': '/r2',
                'productOrderName': 'prod',
                'resourceGroupName': 'rg',
                'resourceLocation': 'loc',
                'meterId': 'm1',
                'meterName': 'Meter',
                'meterCategory': 'cat',
                'meterSubCategory': 'sub',
                'serviceFamily': 'fam',
                'unitOfMeasure': 'u',
                'date': '01/01/2024',
                'costInUsd': '2'
            }])
            importer = CostCsvImporter(csv1, run_id='run1', report_date=datetime.date(2024,1,1))
            importer.import_file()

            # second snapshot for same subscription sub1
            csv2 = f"{tmp}/c2.csv"
            self._write_csv(csv2, [{
                'customerTenantId': 't1',
                'SubscriptionId': 'sub1',
                'subscriptionName': 'Sub One',
                'ResourceId': '/r1',
                'productOrderName': 'prod',
                'resourceGroupName': 'rg',
                'resourceLocation': 'loc',
                'meterId': 'm1',
                'meterName': 'Meter',
                'meterCategory': 'cat',
                'meterSubCategory': 'sub',
                'serviceFamily': 'fam',
                'unitOfMeasure': 'u',
                'date': '01/02/2024',
                'costInUsd': '2'
            }])
            importer = CostCsvImporter(csv2, run_id='run2', report_date=datetime.date(2024,1,2))
            importer.import_file()

            latest = CostReportSnapshot.objects.latest_per_subscription()
            ids = [s.run_id for s in latest]
            self.assertIn('run2', ids)
            self.assertIn('run1', ids)
            # ensure run2 is returned instead of run1 for sub1
            self.assertEqual(ids.count('run2'), 1)

            for snap in CostReportSnapshot.objects.all():
                self.assertEqual(snap.status, CostReportSnapshot.Status.COMPLETE)

    def test_for_day_filters_status(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = f"{tmp}/cost.csv"
            self._write_csv(csv_path, [{
                'customerTenantId': 't1',
                'SubscriptionId': 'sub1',
                'subscriptionName': 'Sub One',
                'ResourceId': '/r1',
                'productOrderName': 'prod',
                'resourceGroupName': 'rg',
                'resourceLocation': 'loc',
                'meterId': 'm1',
                'meterName': 'Meter',
                'meterCategory': 'cat',
                'meterSubCategory': 'sub',
                'serviceFamily': 'fam',
                'unitOfMeasure': 'u',
                'date': '01/01/2024',
                'costInUsd': '1'
            }])
            importer = CostCsvImporter(csv_path, run_id='run1', report_date=datetime.date(2024,1,1))
            importer.import_file()

            snap = CostReportSnapshot.objects.first()
            self.assertEqual(CostReportSnapshot.objects.for_day(datetime.date(2024,1,1)).count(), 1)

            snap.status = CostReportSnapshot.Status.FAILED
            snap.save(update_fields=['status'])

            self.assertEqual(CostReportSnapshot.objects.for_day(datetime.date(2024,1,1)).count(), 0)

    def test_resource_name_extracted(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = f"{tmp}/cost.csv"
            self._write_csv(csv_path, [{
                'customerTenantId': 't1',
                'SubscriptionId': 'sub1',
                'subscriptionName': 'Sub One',
                'ResourceId': '/some/path/res1',
                'productOrderName': 'prod',
                'resourceGroupName': 'rg',
                'resourceLocation': 'loc',
                'meterId': 'm1',
                'meterName': 'Meter',
                'meterCategory': 'cat',
                'meterSubCategory': 'sub',
                'serviceFamily': 'fam',
                'unitOfMeasure': 'u',
                'date': '01/01/2024',
                'costInUsd': '1'
            }])
            importer = CostCsvImporter(csv_path, run_id='run1', report_date=datetime.date(2024,1,1))
            importer.import_file()

            resource = Resource.objects.get(resource_id='/some/path/res1')
            self.assertEqual(resource.resource_name, 'res1')

    def test_resource_group_normalized(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = f"{tmp}/cost.csv"
            self._write_csv(csv_path, [{
                'customerTenantId': 't1',
                'SubscriptionId': 'sub1',
                'subscriptionName': 'Sub One',
                'ResourceId': '/r1',
                'productOrderName': 'prod',
                'resourceGroupName': ' MyRG ',
                'resourceLocation': 'loc',
                'meterId': 'm1',
                'meterName': 'Meter',
                'meterCategory': 'cat',
                'meterSubCategory': 'sub',
                'serviceFamily': 'fam',
                'unitOfMeasure': 'u',
                'date': '01/01/2024',
                'costInUsd': '1'
            }])
            importer = CostCsvImporter(csv_path, run_id='run1', report_date=datetime.date(2024,1,1))
            importer.import_file()

            res = Resource.objects.get(resource_id='/r1')
            self.assertEqual(res.resource_group, 'myrg')

