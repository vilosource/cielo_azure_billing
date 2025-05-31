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
        parser.add_argument(
            "--list-only", 
            action="store_true", 
            help="Only list available files without downloading them"
        )

    def handle(self, **options):
        # Extract options
        source_name = options.get("source_name")
        period = options.get("billing_period")
        base_output_dir = options["output_dir"]
        overwrite = options.get("overwrite", False)
        skip_csv = options.get("skip_csv", False)
        list_only = options.get("list_only", False)

        # Print command title
        self.stdout.write(self.style.SUCCESS("=" * 80))
        if list_only:
            self.stdout.write(self.style.SUCCESS("Azure Billing Source File Listing"))
        else:
            self.stdout.write(self.style.SUCCESS("Azure Billing Source File Downloader"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")

        # Print configuration details
        self._print_config(period, base_output_dir, list_only, overwrite, skip_csv)
        
        # Display all system sources first
        self._print_all_system_sources(source_name)
        
        # Get sources to process
        sources = self._get_sources(source_name)
        if not sources:
            self.stdout.write(self.style.ERROR("No active billing sources found to process."))
            return

        # Create base output directory if it doesn't exist (unless we're only listing)
        if not list_only:
            os.makedirs(base_output_dir, exist_ok=True)
            
        # Process each source
        self._process_sources(sources, period, base_output_dir, overwrite, skip_csv, list_only)
        
    def _print_config(self, period, output_dir, list_only, overwrite, skip_csv):
        """Print configuration details"""
        self.stdout.write(self.style.SUCCESS("🔧 Configuration:"))
        self.stdout.write(f"  Billing Period: {period or 'All available'}")
        if not list_only:
            self.stdout.write(f"  Output Directory: {output_dir}")
            self.stdout.write(f"  Overwrite Existing Files: {'Yes' if overwrite else 'No'}")
            self.stdout.write(f"  Skip CSV Downloads: {'Yes' if skip_csv else 'No'}")
        self.stdout.write(f"  Operation Mode: {'List Only' if list_only else 'Download'}")
        self.stdout.write("")
    
    def _print_all_system_sources(self, requested_source_name=None):
        """Print a table of all sources in the system"""
        all_sources = BillingBlobSource.objects.all().order_by('name')
        
        if not all_sources:
            self.stdout.write(self.style.WARNING("No billing sources defined in the system."))
            return
            
        self.stdout.write(self.style.SUCCESS("📂 Available Blob Sources:"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(f"{'Name':<20} | {'Status':<10} | {'Last Import':<20} | {'Base Folder'}")
        self.stdout.write("-" * 80)
        
        for source in all_sources:
            status_icon = "✅" if source.is_active else "❌"
            status = f"{status_icon} {'Active' if source.is_active else 'Inactive'}"
            last_import = source.last_imported_at or "Never"
            
            # Highlight the requested source if specified
            if requested_source_name and source.name == requested_source_name:
                self.stdout.write(self.style.SUCCESS(f"{source.name:<20} | {status:<10} | {last_import!s:<20} | {source.base_folder}"))
            else:
                self.stdout.write(f"{source.name:<20} | {status:<10} | {last_import!s:<20} | {source.base_folder}")
        
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")
        
    def _process_sources(self, sources, period, base_output_dir, overwrite, skip_csv, list_only):
        """Process all selected sources"""
        self.stdout.write(self.style.SUCCESS(f"Processing {len(sources)} active source(s)..."))
        self.stdout.write("")
        
        # Initialize statistics
        stats = {
            'sources_processed': 0,
            'sources_successful': 0,
            'sources_failed': 0,
            'runs_processed': 0,
            'runs_failed': 0,
            'files_downloaded': 0,
            'files_skipped': 0,
            'errors': []
        }
        
        for idx, source in enumerate(sources, 1):
            self.stdout.write(self.style.SUCCESS(f"[{idx}/{len(sources)}] Processing: {source.name}"))
            stats['sources_processed'] += 1
            
            try:
                if list_only:
                    self._list_source_files(source, period)
                    stats['sources_successful'] += 1
                else:
                    source_stats = self._process_source(source, period, base_output_dir, overwrite, skip_csv)
                    stats['sources_successful'] += 1
                    
                    # Update statistics
                    if source_stats:
                        stats['runs_processed'] += source_stats.get('runs_processed', 0)
                        stats['runs_failed'] += source_stats.get('runs_failed', 0)
                        stats['files_downloaded'] += source_stats.get('files_downloaded', 0)
                        stats['files_skipped'] += source_stats.get('files_skipped', 0)
                        
            except Exception as exc:
                error_msg = f"Error processing source {source.name}: {exc}"
                stats['sources_failed'] += 1
                stats['errors'].append(error_msg)
                self.stdout.write(self.style.ERROR(error_msg))
                logger.exception(f"Error processing source {source.name}")
            
            self.stdout.write("")
        
        # Print final summary
        self._print_final_summary(stats, list_only)
    
    def _print_final_summary(self, stats, list_only):
        """Print a final summary of processing results"""
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("📊 Final Summary"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        
        self.stdout.write(f"Sources processed: {stats['sources_processed']}")
        self.stdout.write(f"Sources successful: {stats['sources_successful']}")
        
        if not list_only:
            self.stdout.write(f"Runs processed: {stats['runs_processed']}")
            self.stdout.write(f"Files downloaded: {stats['files_downloaded']}")
            self.stdout.write(f"Files skipped (already existed): {stats['files_skipped']}")
        
        if stats['sources_failed'] > 0:
            self.stdout.write("")
            self.stdout.write(self.style.ERROR(f"Sources failed: {stats['sources_failed']}"))
            for error in stats['errors']:
                self.stdout.write(self.style.ERROR(f"  • {error}"))
        
        # Overall status
        self.stdout.write("")
        if stats['sources_failed'] == 0:
            self.stdout.write(self.style.SUCCESS("✅ All sources processed successfully!"))
        else:
            self.stdout.write(self.style.WARNING(f"⚠️ {stats['sources_failed']} out of {stats['sources_processed']} sources had errors"))
        
        self.stdout.write(self.style.SUCCESS("=" * 80))

    def _get_sources(self, source_name):
        """Get billing sources to process"""
        if source_name:
            sources = BillingBlobSource.objects.filter(name=source_name, is_active=True)
            if not sources:
                self.stdout.write(self.style.ERROR(f"No active billing source found with name: {source_name}"))
                self._show_available_sources()
            return sources
        else:
            return BillingBlobSource.objects.filter(is_active=True)
            
    def _show_available_sources(self):
        """Show all available sources in the system"""
        all_sources = BillingBlobSource.objects.all().order_by('name')
        
        if not all_sources:
            self.stdout.write(self.style.WARNING("No billing sources defined in the system."))
            return
            
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Available Sources in System:"))
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(f"{'Name':<20} | {'Status':<10} | {'Last Import':<20}")
        self.stdout.write("-" * 60)
        
        for source in all_sources:
            status = "✅ Active" if source.is_active else "❌ Inactive"
            last_import = source.last_imported_at or "Never"
            self.stdout.write(f"{source.name:<20} | {status:<10} | {last_import!s:<20}")
        
        self.stdout.write("")

    def _print_header(self, sources, period, output_dir, list_only=False):
        """Print the command header"""
        self.stdout.write(self.style.SUCCESS("=" * 80))
        if list_only:
            self.stdout.write(self.style.SUCCESS("Azure Billing Source File Listing"))
        else:
            self.stdout.write(self.style.SUCCESS("Azure Billing Source File Downloader"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        
        # Print configuration details
        self.stdout.write(self.style.SUCCESS("🔧 Configuration:"))
        self.stdout.write(f"  Billing Period: {period or 'All available'}")
        if not list_only:
            self.stdout.write(f"  Output Directory: {output_dir}")
        self.stdout.write(f"  Mode: {'List Only' if list_only else 'Download'}")
        self.stdout.write("")
        
        # Print detailed sources information
        self.stdout.write(self.style.SUCCESS("📂 Available Blob Sources:"))
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(f"{'Name':<20} | {'Status':<10} | {'Last Import':<20} | {'Base Folder'}")
        self.stdout.write("=" * 80)
        
        for source in sources:
            status = "✅ Active" if source.is_active else "❌ Inactive"
            last_import = source.last_imported_at or "Never"
            self.stdout.write(f"{source.name:<20} | {status:<10} | {last_import!s:<20} | {source.base_folder}")
        
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")

    def _process_source(self, source, period, base_output_dir, overwrite, skip_csv):
        """Process downloads for a single billing source"""
        self.stdout.write(f"Examining source: {source.name} (base folder: {source.base_folder})")
        
        # Initialize source-level statistics
        source_stats = {
            'runs_processed': 0,
            'runs_failed': 0,
            'files_downloaded': 0,
            'files_skipped': 0
        }
        
        # Inspect available runs to get manifest information
        inspection_result = source.inspect_available_runs(period)
        runs_data = inspection_result.get('runs_data', [])
        
        if not runs_data:
            self.stdout.write(self.style.WARNING(f"No runs found for source {source.name}"))
            return source_stats
        
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
            
            try:
                # Process this run and track files downloaded/skipped
                run_stats = self._process_run(source, run_info, run_dir, overwrite, skip_csv)
                source_stats['runs_processed'] += 1
                
                if run_stats:
                    source_stats['files_downloaded'] += run_stats.get('files_downloaded', 0)
                    source_stats['files_skipped'] += run_stats.get('files_skipped', 0)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing run {run_id}: {e}"))
                source_stats['runs_failed'] += 1
                
        return source_stats

    def _process_run(self, source, run_info, run_dir, overwrite, skip_csv):
        """Process downloads for a single run"""
        run_id = run_info.get('run_id')
        blob_name = run_info.get('blob_name')
        end_date = run_info.get('end_date')
        
        # Initialize run statistics
        run_stats = {
            'files_downloaded': 0,
            'files_skipped': 0
        }
        
        self.stdout.write(f"Processing run: {run_id} (end date: {end_date})")
        
        # Get blobs data
        blob_list, container_url = source.list_blobs()
        
        # Find the manifest blob
        manifest_blob = self._find_manifest_blob(blob_list, blob_name)
        if not manifest_blob:
            self.stdout.write(self.style.ERROR(f"Could not find manifest blob: {blob_name}"))
            return run_stats
        
        # Process the manifest
        manifest_result = self._process_manifest(source, manifest_blob, container_url, run_dir, overwrite)
        if manifest_result:
            manifest_info = manifest_result['manifest_info']
            if manifest_result['status'] == 'downloaded':
                run_stats['files_downloaded'] += 1
            elif manifest_result['status'] == 'skipped':
                run_stats['files_skipped'] += 1
        else:
            return run_stats
            
        # Process CSV if needed
        if not skip_csv:
            csv_result = self._process_csv(source, manifest_info, container_url, run_dir, overwrite)
            if csv_result:
                if csv_result.get('status') == 'downloaded':
                    run_stats['files_downloaded'] += 1
                    # If decompressed, count as another file
                    if csv_result.get('decompressed', False):
                        run_stats['files_downloaded'] += 1
                elif csv_result.get('status') == 'skipped':
                    run_stats['files_skipped'] += 1
            
        # Save run metadata
        metadata_result = self._save_run_metadata(run_info, run_id, end_date, blob_name, container_url, 
                                               manifest_info, source, run_dir, overwrite)
        if metadata_result:
            if metadata_result.get('status') == 'downloaded':
                run_stats['files_downloaded'] += 1
            elif metadata_result.get('status') == 'skipped':
                run_stats['files_skipped'] += 1
        
        return run_stats
    
    def _find_manifest_blob(self, blob_list, blob_name):
        """Find a specific manifest blob in the blob list"""
        for blob in blob_list:
            if blob.name == blob_name:
                return blob
        return None
    
    def _process_manifest(self, source, manifest_blob, container_url, run_dir, overwrite):
        """Download and process the manifest file"""
        manifest_path = os.path.join(run_dir, "manifest.json")
        
        # Skip if file exists and we're not overwriting
        if os.path.exists(manifest_path) and not overwrite:
            self.stdout.write(f"  ✓ Manifest already exists at {manifest_path} (use --overwrite to replace)")
            # Return existing manifest data if possible
            try:
                with open(manifest_path, 'r', encoding='utf-8') as f:
                    manifest_data = json.load(f)
                return {
                    'status': 'skipped',
                    'manifest_info': {'manifest_data': manifest_data}
                }
            except Exception:
                return None
        
        # Download and save manifest
        manifest_info = source.get_manifest_data(manifest_blob, container_url)
        manifest_data = manifest_info['manifest_data']
        
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest_data, fh, indent=2)
        
        self.stdout.write(self.style.SUCCESS(f"  ✓ Manifest saved to {manifest_path}"))
        return {
            'status': 'downloaded',
            'manifest_info': manifest_info
        }
    
    def _process_csv(self, source, manifest_info, container_url, run_dir, overwrite):
        """Download and process the CSV file"""
        manifest_data = manifest_info['manifest_data']
        result = {'status': None, 'decompressed': False}
        
        try:
            csv_blob_data, csv_blob_name = source.download_csv_blob(manifest_data, container_url)
            
            # Save CSV file
            csv_filename = os.path.basename(csv_blob_name)
            csv_path = os.path.join(run_dir, csv_filename)
            
            # Check if file exists and skip if not overwriting
            if os.path.exists(csv_path) and not overwrite:
                self.stdout.write(f"  ✓ CSV file already exists at {csv_path} (use --overwrite to replace)")
                result['status'] = 'skipped'
                return result
            
            # Save the CSV file
            with open(csv_path, "wb") as fh:
                fh.write(csv_blob_data)
            
            download_size = BillingBlobSource.format_bytes(len(csv_blob_data))
            self.stdout.write(self.style.SUCCESS(f"  ✓ CSV file ({download_size}) saved to {csv_path}"))
            result['status'] = 'downloaded'
            
            # Handle decompression if needed
            if csv_filename.endswith('.gz'):
                decompressed = self._decompress_if_needed(csv_path, run_dir)
                if decompressed:
                    result['decompressed'] = True
            
            return result
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  ✗ Error downloading CSV: {e}"))
            result['status'] = 'error'
            return result
    
    def _decompress_if_needed(self, csv_path, run_dir):
        """Decompress gzipped files if needed"""
        csv_filename = os.path.basename(csv_path)
        if not csv_filename.endswith('.gz'):
            return False
            
        try:
            import gzip
            decompressed_path = os.path.join(run_dir, os.path.splitext(csv_filename)[0])
            
            with gzip.open(csv_path, "rb") as f_in, open(decompressed_path, "wb") as f_out:
                f_out.write(f_in.read())
                
            self.stdout.write(self.style.SUCCESS(f"  ✓ Decompressed CSV saved to {decompressed_path}"))
            return True
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  ✗ Error decompressing file: {e}"))
            return False
    
    def _save_run_metadata(self, run_info, run_id, end_date, blob_name, container_url, 
                        manifest_info, source, run_dir, overwrite):
        """Save detailed run metadata to a JSON file"""
        metadata_path = os.path.join(run_dir, "run_metadata.json")
        
        # Skip if file exists and we're not overwriting
        if os.path.exists(metadata_path) and not overwrite:
            self.stdout.write(f"  ✓ Run metadata already exists at {metadata_path} (use --overwrite to replace)")
            return {'status': 'skipped'}
        
        # Initialize metadata dict
        metadata = {
            "run_id": run_id,
            "end_date": str(end_date) if end_date else None,
            "blob_name": blob_name,
            "manifest_url": f"{container_url}/{blob_name}",
            "source_name": source.name,
            "base_folder": source.base_folder,
            "download_timestamp": datetime.now().isoformat(),
        }
        
        # Add CSV URL and additional info if manifest data is available
        if 'manifest_data' in manifest_info:
            run_info_from_manifest = manifest_info['manifest_data'].get('runInfo', {})
            
            # Extract CSV URL if available
            blob_data = manifest_info['manifest_data'].get('blobs', [{}])[0]
            if blob_data.get('blobName'):
                csv_url = f"{container_url}/{blob_data.get('blobName')}"
                metadata["csv_url"] = csv_url
            
            # Add additional metadata from manifest
            metadata["report_name"] = run_info_from_manifest.get("reportName")
            metadata["submitted_time"] = run_info_from_manifest.get("submittedTime")
            metadata["start_date"] = run_info_from_manifest.get("startDate")
            metadata["end_date"] = run_info_from_manifest.get("endDate")
            metadata["report_type"] = run_info_from_manifest.get("reportType")
        
        # Save the metadata file
        with open(metadata_path, "w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2)
        
        self.stdout.write(self.style.SUCCESS(f"  ✓ Enhanced run metadata saved to {metadata_path}"))
        return {'status': 'downloaded'}
    
    def _list_source_files(self, source, period):
        """List all files for a billing source with full URLs"""
        self.stdout.write(f"Listing files for source: {source.name} (base folder: {source.base_folder})")
        
        # Get blob list
        blob_list, container_url = source.list_blobs(period)
        
        # Group files by type
        manifest_files = []
        csv_files = []
        gz_files = []
        tar_gz_files = []
        other_files = []
        
        for blob in blob_list:
            full_url = f"{container_url}/{blob.name}"
            file_info = {
                'name': blob.name,
                'url': full_url,
                'size': BillingBlobSource.format_bytes(blob.size),
                'last_modified': blob.last_modified.strftime("%Y-%m-%d %H:%M:%S") if blob.last_modified else "Unknown"
            }
            
            if blob.name.endswith('manifest.json'):
                manifest_files.append(file_info)
            elif blob.name.endswith('.csv'):
                csv_files.append(file_info)
            elif blob.name.endswith('.gz') and not blob.name.endswith('.tar.gz'):
                gz_files.append(file_info)
            elif blob.name.endswith('.tar.gz'):
                tar_gz_files.append(file_info)
            else:
                other_files.append(file_info)
        
        # Print file summaries
        self.stdout.write(self.style.SUCCESS("\nFile Summary:"))
        self.stdout.write(f"  Total files: {len(blob_list)}")
        self.stdout.write(f"  Manifest files: {len(manifest_files)}")
        self.stdout.write(f"  CSV files: {len(csv_files)}")
        self.stdout.write(f"  GZ files: {len(gz_files)}")
        self.stdout.write(f"  TAR.GZ files: {len(tar_gz_files)}")
        self.stdout.write(f"  Other files: {len(other_files)}")
        self.stdout.write("")
        
        # Print detailed listings for each file type
        self._print_file_list("📋 Manifest Files:", manifest_files)
        self._print_file_list("📄 CSV Files:", csv_files)
        self._print_file_list("🗜️ GZ Files:", gz_files)
        self._print_file_list("📦 TAR.GZ Files:", tar_gz_files, show_urls=True)
        self._print_file_list("📁 Other Files:", other_files)
    
    def _print_file_list(self, title, files, show_urls=False):
        """Print a formatted list of files"""
        if not files:
            return
            
        self.stdout.write(self.style.SUCCESS(title))
        for file_info in sorted(files, key=lambda x: x['name']):
            self.stdout.write(f"  • {file_info['name']}")
            self.stdout.write(f"      Size: {file_info['size']}, Modified: {file_info['last_modified']}")
            if show_urls:
                self.stdout.write(f"      URL: {file_info['url']}")
        self.stdout.write("")
