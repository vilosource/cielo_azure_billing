from django.core.management.base import BaseCommand
from billing.services import CostCsvImporter


class Command(BaseCommand):
    help = 'Import an Azure cost CSV file'

    def add_arguments(self, parser):
        parser.add_argument('--file', type=str, required=True, help='Path to the CSV file')

    def handle(self, *args, **options):
        importer = CostCsvImporter(options['file'])
        count = importer.import_file()
        self.stdout.write(self.style.SUCCESS(f'Imported {count} entries from {options["file"]}'))
