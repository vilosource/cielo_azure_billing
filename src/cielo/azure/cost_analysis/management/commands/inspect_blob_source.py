import logging
from datetime import datetime
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "List available export runs for a BillingBlobSource"

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-name", required=True, help="BillingBlobSource name"
        )
        parser.add_argument(
            "--billing-period", help="Billing period in YYYYMMDD-YYYYMMDD format"
        )

    def handle(self, **options):
        name = options["source_name"]
        period = options.get("billing_period")

        logger.info(
            f"Starting inspection for source: {name}, billing period: {period}"
        )

        self._print_header()
        
        source = self._get_source(name)
        if not source:
            return

        self._print_source_info(source, period)

        try:
            self._process_inspection(source, period)
        except Exception as exc:
            self._handle_inspection_error(exc)

    def _print_header(self):
        """Print the report header"""
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Azure Billing Source Inspection Report"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")

    def _get_source(self, name):
        """Get and validate the billing source"""
        source = BillingBlobSource.objects.filter(name=name).first()
        if not source:
            error_msg = f"Source '{name}' not found"
            logger.error(error_msg)
            self.stdout.write(self.style.ERROR(f"❌ {error_msg}"))
            self.stdout.write("")
            self.stdout.write("Available sources:")
            for s in BillingBlobSource.objects.all():
                status_icon = "✅" if s.is_active else "❌"
                self.stdout.write(f"  {status_icon} {s.name} ({s.base_folder})")
            return None
        return source

    def _print_source_info(self, source, period):
        """Print source information"""
        self.stdout.write(self.style.SUCCESS("📋 Source Information:"))
        self.stdout.write(f"  Name: {source.name}")
        self.stdout.write(f"  Base Folder: {source.base_folder}")
        self.stdout.write(f"  Status: {'Active' if source.is_active else 'Inactive'}")
        self.stdout.write(f"  Last Import: {source.last_imported_at or 'Never'}")
        self.stdout.write(f"  Last Attempt: {source.last_attempted_at or 'Never'}")
        self.stdout.write(f"  Current Status: {source.status or 'N/A'}")
        if period:
            self.stdout.write(f"  Billing Period: {period}")
        self.stdout.write("")

        logger.info(
            f"Found source: {source.name}, base_folder: {source.base_folder}, is_active: {source.is_active}, status: {source.status}"
        )

    def _process_inspection(self, source, period):
        """Main inspection logic using model methods"""
        self.stdout.write(self.style.SUCCESS("🔍 Connecting to Azure Storage..."))
        
        # Use the new model method
        inspection_result = source.inspect_available_runs(billing_period=period)
        
        self._print_blob_summary(inspection_result)
        self._print_detailed_file_listing(inspection_result)

        if not inspection_result['manifests']:
            self._handle_no_manifests(inspection_result)
            return

        self._print_runs_info(inspection_result['runs_data'])
        self._print_final_summary(inspection_result['runs_data'])

    def _print_blob_summary(self, inspection_result):
        """Print blob summary statistics"""
        blob_details = inspection_result['blob_details']
        total_size = sum(blob.size for blob in 
                        blob_details['manifests'] + blob_details['csv_files'] + blob_details['other_files'])
        manifest_size = sum(blob.size for blob in blob_details['manifests'])
        csv_size = sum(blob.size for blob in blob_details['csv_files'])
        other_size = sum(blob.size for blob in blob_details['other_files'])
        
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("📊 Storage Account Summary:"))
        self.stdout.write(f"  Total files found: {inspection_result['total_blobs']}")
        self.stdout.write(f"  📋 Manifest files: {inspection_result['manifests']} ({BillingBlobSource.format_bytes(manifest_size)})")
        self.stdout.write(f"  📄 CSV/Data files: {inspection_result['csv_files']} ({BillingBlobSource.format_bytes(csv_size)})")
        self.stdout.write(f"  📁 Other files: {inspection_result['other_files']} ({BillingBlobSource.format_bytes(other_size)})")
        self.stdout.write(f"  💾 Total size: {BillingBlobSource.format_bytes(total_size)}")
        self.stdout.write("")

    def _print_detailed_file_listing(self, inspection_result):
        """Print detailed file listing organized by type"""
        blob_details = inspection_result['blob_details']
        
        if not inspection_result['total_blobs']:
            return
            
        self.stdout.write(self.style.SUCCESS("📁 Detailed File Listing:"))
        self.stdout.write("")
        
        self._print_file_category("📋 Manifest Files:", blob_details['manifests'])
        self._print_file_category("📄 CSV/Data Files:", blob_details['csv_files'])
        self._print_file_category("📁 Other Files:", blob_details['other_files'])

    def _print_file_category(self, title, files):
        """Print files for a specific category"""
        if not files:
            return
            
        self.stdout.write(self.style.SUCCESS(title))
        for blob in sorted(files, key=lambda x: x.name):
            modified = blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "Unknown"
            file_icon = self._get_file_icon(blob.name)
            self.stdout.write(f"  {file_icon} {blob.name}")
            self.stdout.write(f"      Size: {BillingBlobSource.format_bytes(blob.size)}, Modified: {modified}")
        self.stdout.write("")

    def _get_file_icon(self, filename):
        """Get appropriate emoji icon for file type"""
        if filename.endswith("manifest.json"):
            return "📋"
        elif filename.endswith(".csv.gz"):
            return "🗜️"
        elif filename.endswith(".csv"):
            return "📄"
        else:
            return "📁"

    def _handle_no_manifests(self, inspection_result):
        """Handle case when no manifests are found"""
        self.stdout.write(self.style.WARNING("⚠️  No export runs found"))
        if inspection_result['total_blobs']:
            self.stdout.write("Found blobs but no manifest.json files:")
            blob_details = inspection_result['blob_details']
            all_blobs = blob_details['manifests'] + blob_details['csv_files'] + blob_details['other_files']
            for blob in all_blobs[:10]:  # Show first 10
                self.stdout.write(f"  📄 {blob.name} ({BillingBlobSource.format_bytes(blob.size)})")
            if len(all_blobs) > 10:
                self.stdout.write(f"  ... and {len(all_blobs) - 10} more")
        logger.warning("No runs found")

    def _print_runs_info(self, runs_data):
        """Print information for all runs"""
        self.stdout.write(self.style.SUCCESS("📋 Processing Export Runs:"))
        self.stdout.write("")

        for run_data in runs_data:
            self._print_run_info(run_data)

    def _print_run_info(self, run_data):
        """Print information for a single run"""
        status_icon = "✅" if run_data['imported'] else "⏳"
        self.stdout.write(f"  {status_icon} Run ID: {run_data['run_id']}")
        self.stdout.write(f"     End Date: {run_data['end_date']}")
        self.stdout.write(f"     Size: {BillingBlobSource.format_bytes(run_data['size'])}")
        self.stdout.write(f"     Last Modified: {run_data['last_modified']}")
        self.stdout.write(f"     Imported: {'Yes' if run_data['imported'] else 'No'}")
        self.stdout.write("")

    def _print_final_summary(self, runs_data):
        """Print the final summary report"""
        imported_count = sum(1 for run in runs_data if run['imported'])
        
        self.stdout.write(self.style.SUCCESS("📈 Final Summary:"))
        self.stdout.write(f"  Total export runs: {len(runs_data)}")
        self.stdout.write(f"  Already imported: {imported_count}")
        self.stdout.write(f"  Pending import: {len(runs_data) - imported_count}")
        
        if runs_data:
            latest_run = max(runs_data, key=lambda x: x['last_modified'] if x['last_modified'] else datetime.min.replace(tzinfo=datetime.now().tzinfo))
            oldest_run = min(runs_data, key=lambda x: x['last_modified'] if x['last_modified'] else datetime.max.replace(tzinfo=datetime.now().tzinfo))
            
            self.stdout.write(f"  Latest run: {latest_run['end_date']} ({latest_run['run_id']})")
            self.stdout.write(f"  Oldest run: {oldest_run['end_date']} ({oldest_run['run_id']})")
        
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("✅ Inspection completed successfully!"))

    def _handle_inspection_error(self, exc):
        """Handle inspection errors"""
        logger.error(
            f"Inspection failed with exception: {type(exc).__name__}: {exc}",
            exc_info=True,
        )
        self.stdout.write(self.style.ERROR(f"❌ Inspection failed: {exc}"))
