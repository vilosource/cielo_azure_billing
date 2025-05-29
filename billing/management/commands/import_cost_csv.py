import logging
from django.core.management.base import BaseCommand
from billing.services import CostCsvImporter

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import an Azure cost CSV file'

    def add_arguments(self, parser):
        parser.add_argument('--file', type=str, required=True, help='Path to the CSV file')

    def handle(self, *args, **options):
        file_path = options['file']
        logger.info('Starting CSV import command with file: %s', file_path)

        try:
            importer = CostCsvImporter(file_path)
            count = importer.import_file()
            logger.info('Command completed successfully. Imported %s entries', count)
            self.stdout.write(self.style.SUCCESS(f'Imported {count} entries from {file_path}'))
        except Exception as e:
            logger.error('Command failed: %s', e)
            self.stdout.write(self.style.ERROR(f'Import failed: {str(e)}'))
