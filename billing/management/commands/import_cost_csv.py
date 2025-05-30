import logging
import json
import datetime
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource
from billing.services import CostCsvImporter

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import an Azure cost CSV file'

    def add_arguments(self, parser):
        parser.add_argument('--file', type=str, required=True, help='Path to the CSV file')
        parser.add_argument('--manifest', type=str, required=True, help='Path to the manifest.json')
        parser.add_argument('--source-name', type=str, help='Optional BillingBlobSource name')

    def handle(self, *args, **options):
        file_path = options['file']
        manifest_path = options['manifest']
        source_name = options.get('source_name')
        logger.info('Starting CSV import command with file: %s', file_path)

        run_id = None
        report_date = None
        source = None
        try:
            with open(manifest_path, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            run_id = data.get('runInfo', {}).get('runId')
            report_raw = data.get('runInfo', {}).get('endDate')
            if report_raw:
                report_date = datetime.date.fromisoformat(report_raw.split('T')[0])
        except Exception as exc:
            logger.error('Failed to parse manifest %s: %s', manifest_path, exc)

        if source_name:
            source = BillingBlobSource.objects.filter(name=source_name).first()

        try:
            importer = CostCsvImporter(file_path, run_id=run_id, report_date=report_date, source=source)
            count = importer.import_file()
            logger.info('Command completed successfully. Imported %s entries', count)
            self.stdout.write(self.style.SUCCESS(f'Imported {count} entries from {file_path}'))
        except Exception as e:
            logger.error('Command failed: %s', e)
            self.stdout.write(self.style.ERROR(f'Import failed: {str(e)}'))
