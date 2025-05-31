import json
import logging
from datetime import datetime
from urllib.parse import urlparse

from django.core.management.base import BaseCommand

from billing.models import BillingBlobSource, CostReportSnapshot

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient, ContainerClient
except Exception:  # pragma: no cover - libs optional for offline env
    DefaultAzureCredential = None
    BlobClient = None
    ContainerClient = None

logger = logging.getLogger(__name__)

# File type constants
MANIFEST_EXTENSION = "manifest.json"
CSV_EXTENSIONS = (".csv", ".csv.gz")


def parse_base_folder(base):
    logger.debug(f"Parsing base folder: {base}")
    parsed = urlparse(base)
    logger.debug(
        f"Parsed URL - scheme: {parsed.scheme}, netloc: {parsed.netloc}, path: {parsed.path}"
    )

    path = parsed.path.lstrip("/")
    parts = path.split("/", 1)
    container = parts[0]
    prefix = ""
    if len(parts) > 1:
        prefix = parts[1].rstrip("/") + "/"

    container_url = f"{parsed.scheme}://{parsed.netloc}/{container}"
    logger.debug(
        f"Extracted container: {container}, prefix: {prefix}, container_url: {container_url}"
    )

    return container_url, prefix


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

        if DefaultAzureCredential is None:
            self._print_azure_sdk_error()
            return

        try:
            self._process_azure_inspection(source, period)
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

    def _print_azure_sdk_error(self):
        """Print Azure SDK installation error"""
        error_msg = "Azure SDK not installed"
        logger.error(error_msg)
        self.stdout.write(self.style.ERROR(f"❌ {error_msg}"))
        self.stdout.write("Please install Azure SDK: pip install azure-identity azure-storage-blob")

    def _process_azure_inspection(self, source, period):
        """Main Azure inspection logic"""
        self.stdout.write(self.style.SUCCESS("🔍 Connecting to Azure Storage..."))
        logger.info("Initializing Azure credentials...")
        cred = DefaultAzureCredential()
        logger.info("Azure credentials initialized successfully")

        container_url, prefix = parse_base_folder(source.base_folder)
        logger.info(f"Creating container client for URL: {container_url}")
        client = ContainerClient.from_container_url(container_url, credential=cred)

        listing_prefix = prefix
        if period:
            listing_prefix = f"{prefix}{period}/"

        self.stdout.write(f"🔎 Searching for files with prefix: {listing_prefix}")
        logger.info(f"Listing blobs with prefix: {listing_prefix}")

        blobs = client.list_blobs(name_starts_with=listing_prefix)
        blob_list = list(blobs)
        logger.info(f"Found {len(blob_list)} total blobs")

        manifests = [b for b in blob_list if b.name.endswith(MANIFEST_EXTENSION)]
        csv_files = [b for b in blob_list if b.name.endswith(CSV_EXTENSIONS)]
        other_files = [b for b in blob_list if not b.name.endswith(MANIFEST_EXTENSION) and not b.name.endswith(CSV_EXTENSIONS)]
        
        logger.info(f"Found {len(manifests)} manifest files, {len(csv_files)} CSV files, {len(other_files)} other files")

        self._print_blob_summary(blob_list, manifests, csv_files, other_files)
        self._print_detailed_file_listing(blob_list, manifests, csv_files, other_files)

        if not manifests:
            self._handle_no_manifests(blob_list)
            return

        runs_data = self._process_manifests(manifests, container_url)
        self._print_final_summary(runs_data)

    def _print_blob_summary(self, blob_list, manifests, csv_files, other_files):
        """Print blob summary statistics"""
        total_size = sum(blob.size for blob in blob_list)
        manifest_size = sum(blob.size for blob in manifests)
        csv_size = sum(blob.size for blob in csv_files)
        other_size = sum(blob.size for blob in other_files)
        
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("📊 Storage Account Summary:"))
        self.stdout.write(f"  Total files found: {len(blob_list)}")
        self.stdout.write(f"  📋 Manifest files: {len(manifests)} ({self._format_bytes(manifest_size)})")
        self.stdout.write(f"  📄 CSV/Data files: {len(csv_files)} ({self._format_bytes(csv_size)})")
        self.stdout.write(f"  📁 Other files: {len(other_files)} ({self._format_bytes(other_size)})")
        self.stdout.write(f"  💾 Total size: {self._format_bytes(total_size)}")
        self.stdout.write("")

    def _print_detailed_file_listing(self, blob_list, manifests, csv_files, other_files):
        """Print detailed file listing organized by type"""
        if not blob_list:
            return
            
        self.stdout.write(self.style.SUCCESS("📁 Detailed File Listing:"))
        self.stdout.write("")
        
        # Show manifest files
        if manifests:
            self.stdout.write(self.style.SUCCESS("📋 Manifest Files:"))
            for blob in sorted(manifests, key=lambda x: x.name):
                modified = blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "Unknown"
                self.stdout.write(f"  📋 {blob.name}")
                self.stdout.write(f"      Size: {self._format_bytes(blob.size)}, Modified: {modified}")
            self.stdout.write("")
        
        # Show CSV/data files  
        if csv_files:
            self.stdout.write(self.style.SUCCESS("📄 CSV/Data Files:"))
            for blob in sorted(csv_files, key=lambda x: x.name):
                modified = blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "Unknown"
                file_icon = "🗜️" if blob.name.endswith(".gz") else "📄"
                self.stdout.write(f"  {file_icon} {blob.name}")
                self.stdout.write(f"      Size: {self._format_bytes(blob.size)}, Modified: {modified}")
            self.stdout.write("")
        
        # Show other files
        if other_files:
            self.stdout.write(self.style.SUCCESS("📁 Other Files:"))
            for blob in sorted(other_files, key=lambda x: x.name):
                modified = blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "Unknown"
                self.stdout.write(f"  📁 {blob.name}")
                self.stdout.write(f"      Size: {self._format_bytes(blob.size)}, Modified: {modified}")
            self.stdout.write("")
        
    def _print_detailed_file_listing(self, blob_list, manifests, csv_files, other_files):
        """Print detailed file listing organized by type"""
        if not blob_list:
            return
            
        self.stdout.write(self.style.SUCCESS("📁 Detailed File Listing:"))
        self.stdout.write("")
        
        self._print_file_category("📋 Manifest Files:", manifests)
        self._print_file_category("📄 CSV/Data Files:", csv_files)
        self._print_file_category("📁 Other Files:", other_files)
        self._print_directory_structure(blob_list)

    def _print_file_category(self, title, files):
        """Print files for a specific category"""
        if not files:
            return
            
        self.stdout.write(self.style.SUCCESS(title))
        for blob in sorted(files, key=lambda x: x.name):
            modified = blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "Unknown"
            file_icon = self._get_file_icon(blob.name)
            self.stdout.write(f"  {file_icon} {blob.name}")
            self.stdout.write(f"      Size: {self._format_bytes(blob.size)}, Modified: {modified}")
        self.stdout.write("")

    def _get_file_icon(self, filename):
        """Get appropriate emoji icon for file type"""
        if filename.endswith(MANIFEST_EXTENSION):
            return "📋"
        elif filename.endswith(".csv.gz"):
            return "🗜️"
        elif filename.endswith(".csv"):
            return "📄"
        else:
            return "📁"

    def _print_directory_structure(self, blob_list):
        """Print directory structure view"""
        if not blob_list:
            return
            
        self.stdout.write(self.style.SUCCESS("🌳 Directory Structure:"))
        
        # Build directory tree
        dirs = {}
        for blob in blob_list:
            parts = blob.name.split('/')
            current = dirs
            
            # Build nested structure
            for i, part in enumerate(parts[:-1]):  # Exclude filename
                if part not in current:
                    current[part] = {'_files': [], '_dirs': {}}
                current = current[part]['_dirs']
            
            # Add file to final directory
            if len(parts) > 1:
                final_dir = parts[-2]
                if final_dir not in current:
                    current[final_dir] = {'_files': [], '_dirs': {}}
                current[final_dir]['_files'].append(blob)
            else:
                # File in root
                if '_root' not in dirs:
                    dirs['_root'] = {'_files': [], '_dirs': {}}
                dirs['_root']['_files'].append(blob)
        
        self._print_tree(dirs, "")

    def _print_tree(self, tree, indent):
        """Recursively print directory tree"""
        for name, content in sorted(tree.items()):
            if name == '_root':
                continue
                
            self.stdout.write(f"{indent}📁 {name}/")
            
            # Print files in this directory
            for file_blob in sorted(content.get('_files', []), key=lambda x: x.name):
                filename = file_blob.name.split('/')[-1]
                icon = self._get_file_icon(filename)
                size = self._format_bytes(file_blob.size)
                self.stdout.write(f"{indent}  {icon} {filename} ({size})")
            
            # Recursively print subdirectories
            if content.get('_dirs'):
                self._print_tree(content['_dirs'], indent + "  ")
        
        # Handle root files
        if '_root' in tree and tree['_root']['_files']:
            for file_blob in sorted(tree['_root']['_files'], key=lambda x: x.name):
                filename = file_blob.name
                icon = self._get_file_icon(filename)
                size = self._format_bytes(file_blob.size)
                self.stdout.write(f"{indent}{icon} {filename} ({size})")
        
        self.stdout.write("")

    def _handle_no_manifests(self, blob_list):
        """Handle case when no manifests are found"""
        self.stdout.write(self.style.WARNING("⚠️  No export runs found"))
        if blob_list:
            self.stdout.write("Found blobs but no manifest.json files:")
            for blob in blob_list[:10]:  # Show first 10
                self.stdout.write(f"  📄 {blob.name} ({self._format_bytes(blob.size)})")
            if len(blob_list) > 10:
                self.stdout.write(f"  ... and {len(blob_list) - 10} more")
        logger.warning("No runs found")

    def _process_manifests(self, manifests, container_url):
        """Process all manifest files and return runs data"""
        runs_data = []
        
        self.stdout.write(self.style.SUCCESS("📋 Processing Export Runs:"))
        self.stdout.write("")

        for i, blob in enumerate(manifests, 1):
            logger.info(f"Processing manifest {i}/{len(manifests)}: {blob.name}")

            manifest_url = f"{container_url}/{blob.name}"
            logger.debug(f"Manifest URL: {manifest_url}")

            bclient = BlobClient.from_blob_url(manifest_url, credential=DefaultAzureCredential())
            logger.debug("Downloading manifest blob...")

            manifest_data = json.loads(bclient.download_blob().readall())
            logger.debug(f"Manifest data keys: {list(manifest_data.keys())}")

            run_info = manifest_data.get("runInfo", {})
            run_id = run_info.get("runId")
            end_date = run_info.get("endDate")

            logger.debug(f"Extracted run_id: {run_id}, end_date: {end_date}")

            imported = CostReportSnapshot.objects.filter(run_id=run_id).exists()
            logger.debug(f"Import status for run_id {run_id}: {imported}")

            run_data = {
                'run_id': run_id,
                'end_date': end_date,
                'size': blob.size,
                'imported': imported,
                'blob_name': blob.name,
                'last_modified': blob.last_modified
            }
            runs_data.append(run_data)

            self._print_run_info(run_data)

        return runs_data

    def _print_run_info(self, run_data):
        """Print information for a single run"""
        status_icon = "✅" if run_data['imported'] else "⏳"
        self.stdout.write(f"  {status_icon} Run ID: {run_data['run_id']}")
        self.stdout.write(f"     End Date: {run_data['end_date']}")
        self.stdout.write(f"     Size: {self._format_bytes(run_data['size'])}")
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

    def _format_bytes(self, size_bytes):
        """Convert bytes to human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
