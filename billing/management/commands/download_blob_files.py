import logging
import os
import json
from datetime import datetime
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Download files from all active BillingBlobSources to local directories named by run ID"

    def add_arguments(self, parser):
        parser.add_argument(
            "--source-name", 
            help="BillingBlobSource name (if not provided, will process all active sources)"
        )
        parser.add_argument(
            "--billing-period", 
            help="Billing period in YYYYMMDD-YYYYMMDD format"
        )
        parser.add_argument(
            "--output-dir", 
            default="./blob_downloads",
            help="Base directory to store downloaded files (default: ./blob_downloads)"
        )
        parser.add_argument(
            "--overwrite", 
            action="store_true", 
            help="Overwrite existing files if they already exist"
        )
        parser.add_argument(
            "--skip-csv", 
            action="store_true", 
            help="Skip downloading the large CSV files, download only manifests"
        )

    def handle(self, **options):
        source_name = options.get("source_name")
        period = options.get("billing_period")
        base_output_dir = options["output_dir"]
        overwrite = options.get("overwrite", False)
        skip_csv = options.get("skip_csv", False)

        # Create base output directory if it doesn't exist
        os.makedirs(base_output_dir, exist_ok=True)

        # Get sources to process
        sources = self._get_sources(source_name)
        if not sources:
            self.stdout.write(self.style.ERROR(f"No active billing sources found."))
            return

        self._print_header(sources, period, base_output_dir)
        
        # Process each source
        for source in sources:
            self.stdout.write(self.style.SUCCESS(f"Processing source: {source.name}"))
            try:
                self._process_source(source, period, base_output_dir, overwrite, skip_csv)
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"Error processing source {source.name}: {exc}"))
                logger.exception(f"Error processing source {source.name}")
                continue

    def _get_sources(self, source_name):
        """Get billing sources to process"""
        if source_name:
            sources = BillingBlobSource.objects.filter(name=source_name, is_active=True)
            if not sources:
                self.stdout.write(self.style.ERROR(f"No active billing source found with name: {source_name}"))
            return sources
        else:
            return BillingBlobSource.objects.filter(is_active=True)

    def _print_header(self, sources, period, output_dir):
        """Print the command header"""
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Azure Billing Source File Downloader"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        
        source_names = ", ".join([source.name for source in sources])
        self.stdout.write(f"Sources: {source_names}")
        self.stdout.write(f"Billing Period: {period or 'All available'}")
        self.stdout.write(f"Output Directory: {output_dir}")
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")

    def _process_source(self, source, period, base_output_dir, overwrite, skip_csv):
        """Process downloads for a single billing source"""
        self.stdout.write(f"Examining source: {source.name} (base folder: {source.base_folder})")
        
        # Inspect available runs to get manifest information
        inspection_result = source.inspect_available_runs(period)
        runs_data = inspection_result.get('runs_data', [])
        
        if not runs_data:
            self.stdout.write(self.style.WARNING(f"No runs found for source {source.name}"))
            return
        
        self.stdout.write(f"Found {len(runs_data)} runs")
        
        # Process each run
        for run_info in runs_data:
            run_id = run_info.get('run_id')
            if not run_id:
                self.stdout.write(self.style.WARNING("Found run with missing run_id, skipping"))
                continue
                
            # Create directory for this run
            run_dir = os.path.join(base_output_dir, run_id)
            os.makedirs(run_dir, exist_ok=True)
            
            # Process this run
            self._process_run(source, run_info, run_dir, overwrite, skip_csv)

    def _process_run(self, source, run_info, run_dir, overwrite, skip_csv):
        """Process downloads for a single run"""
        run_id = run_info.get('run_id')
        blob_name = run_info.get('blob_name')
        end_date = run_info.get('end_date')
        
        self.stdout.write(f"Processing run: {run_id} (end date: {end_date})")
        
        # Download manifest
        manifest_path = os.path.join(run_dir, "manifest.json")
        if not os.path.exists(manifest_path) or overwrite:
            # Get manifest data
            blob_list, container_url = source.list_blobs()
            
            # Find the manifest blob
            manifest_blob = None
            for blob in blob_list:
                if blob.name == blob_name:
                    manifest_blob = blob
                    break
            
            if not manifest_blob:
                self.stdout.write(self.style.ERROR(f"Could not find manifest blob: {blob_name}"))
                return
            
            # Download and save manifest
            manifest_info = source.get_manifest_data(manifest_blob, container_url)
            manifest_data = manifest_info['manifest_data']
            
            with open(manifest_path, "w", encoding="utf-8") as fh:
                json.dump(manifest_data, fh, indent=2)
            
            self.stdout.write(self.style.SUCCESS(f"  ✓ Manifest saved to {manifest_path}"))
            
            # Download CSV if needed
            if not skip_csv:
                try:
                    csv_blob_data, csv_blob_name = source.download_csv_blob(manifest_data, container_url)
                    
                    # Save CSV file
                    csv_filename = os.path.basename(csv_blob_name)
                    csv_path = os.path.join(run_dir, csv_filename)
                    
                    with open(csv_path, "wb") as fh:
                        fh.write(csv_blob_data)
                    
                    download_size = BillingBlobSource.format_bytes(len(csv_blob_data))
                    self.stdout.write(self.style.SUCCESS(f"  ✓ CSV file ({download_size}) saved to {csv_path}"))
                    
                    # If it's a gzipped file, also save decompressed version
                    if csv_filename.endswith('.gz'):
                        import gzip
                        decompressed_path = os.path.join(run_dir, os.path.splitext(csv_filename)[0])
                        
                        with gzip.open(csv_path, "rb") as f_in, open(decompressed_path, "wb") as f_out:
                            f_out.write(f_in.read())
                            
                        self.stdout.write(self.style.SUCCESS(f"  ✓ Decompressed CSV saved to {decompressed_path}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  ✗ Error downloading CSV: {e}"))
        else:
            self.stdout.write(f"  ✓ Manifest already exists at {manifest_path} (use --overwrite to replace)")
            
        # Save run metadata
        metadata_path = os.path.join(run_dir, "run_metadata.json")
        if not os.path.exists(metadata_path) or overwrite:
            with open(metadata_path, "w", encoding="utf-8") as fh:
                json.dump({
                    "run_id": run_id,
                    "end_date": str(end_date) if end_date else None,
                    "blob_name": blob_name,
                    "source_name": source.name,
                    "base_folder": source.base_folder,
                    "download_timestamp": datetime.now().isoformat(),
                }, fh, indent=2)
            
            self.stdout.write(self.style.SUCCESS(f"  ✓ Run metadata saved to {metadata_path}"))
        else:
            self.stdout.write(f"  ✓ Run metadata already exists at {metadata_path} (use --overwrite to replace)")
