# billing/management/commands/download_blobs.py
import os
import json
import gzip
import logging
from datetime import datetime
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """
    Django management command to download or list Azure billing blob files
    from BillingBlobSource entries.
    """
    help = "Download or list files from BillingBlobSource entries"

    def add_arguments(self, parser):
        parser.add_argument("--source-name", help="Specific BillingBlobSource to process")
        parser.add_argument("--billing-period", help="YYYYMMDD-YYYYMMDD")
        parser.add_argument("--output-dir", default="./blob_downloads")
        parser.add_argument("--overwrite", action="store_true")
        parser.add_argument("--skip-csv", action="store_true")
        parser.add_argument("--list-only", action="store_true")

    def handle(self, **options):
        """Entrypoint for command-line arguments and service invocation."""
        service = BlobDownloadService(
            output_dir=options["output_dir"],
            overwrite=options["overwrite"],
            skip_csv=options["skip_csv"],
            list_only=options["list_only"],
            stdout=self.stdout,
        )

        sources = BillingBlobSource.objects.filter(
            name=options["source_name"] if options["source_name"] else None,
            is_active=True
        ) if options["source_name"] else BillingBlobSource.objects.filter(is_active=True)

        if not sources:
            self.stdout.write(self.style.ERROR("No active billing sources found."))
            return

        service.process_sources(sources, options["billing_period"])

        # Print summary output
        self.stdout.write("📋 Summary Report:")
        self.stdout.write("=" * 60)
        for item in service.summary:
            if 'error' in item:
                self.stdout.write(f"❌ Source {item['source']} failed: {item['error']}")
            else:
                self.stdout.write(f"✅ Source: {item['source']}, Run: {item['run_id']}")
                self.stdout.write(f"   - Manifest: {item['manifest_path']}")
                if item.get('csv_path'):
                    self.stdout.write(f"   - CSV: {item['csv_path']}")
                if item.get('report_name'):
                    self.stdout.write(f"   - Report: {item['report_name']} ({item.get('report_type')})")
                if item.get('start_date'):
                    self.stdout.write(f"   - Period: {item['start_date']} to {item.get('end_date')}")
        self.stdout.write("=" * 60)


class BlobDownloadService:
    """
    Orchestrates the processing of billing blob sources,
    including listing or downloading associated billing data files.
    Maintains a summary of actions taken per source for reporting.
    """
    def __init__(self, output_dir, overwrite, skip_csv, list_only, stdout):
        self.summary = []
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.skip_csv = skip_csv
        self.list_only = list_only
        self.stdout = stdout

    def process_sources(self, sources, period):
        os.makedirs(self.output_dir, exist_ok=True)
        for source in sources:
            self.stdout.write(f"Processing: {source.name}")
            try:
                if self.list_only:
                    self._list_files(source, period)
                else:
                    runs = source.inspect_available_runs(period).get("runs_data", [])
                    for run in runs:
                        RunProcessor(
                            source, run, self.output_dir,
                            self.overwrite, self.skip_csv,
                            self.stdout,
                            self.summary
                        ).process()
            except Exception as e:
                self.stdout.write(f"Error: {e}")
                logger.exception(f"Failed to process source {source.name}")

    def _list_files(self, source, period):
        blob_list, container_url = source.list_blobs(period)
        self.stdout.write(f"Listing files for {source.name}:")
        for blob in blob_list:
            self.stdout.write(f" - {container_url}/{blob.name}")


class RunProcessor:
    """
    Handles the processing of a single billing run, including manifest,
    CSV data, and metadata management.
    Records outcomes into a shared summary list.

    Handles the processing of a single billing run, including manifest,
    CSV data, and metadata management.
    """
    def __init__(self, source, run_info, output_dir, overwrite, skip_csv, stdout, summary):
        self.summary = summary
        self.source = source
        self.run_info = run_info
        self.run_id = run_info.get("run_id")
        self.output_dir = os.path.join(output_dir, self.run_id)
        os.makedirs(self.output_dir, exist_ok=True)
        self.overwrite = overwrite
        self.skip_csv = skip_csv
        self.stdout = stdout

    def process(self):
        summary_entry = {"source": self.source.name, "run_id": self.run_id}
        manifest_path = os.path.join(self.output_dir, "manifest.json")
        summary_entry["manifest_path"] = manifest_path
        blob_list, container_url = self.source.list_blobs()
        blob_name = self.run_info.get("blob_name")

        manifest_blob = next((b for b in blob_list if b.name == blob_name), None)
        if not manifest_blob:
            self.stdout.write(f"Manifest {blob_name} not found.")
            return

        manifest_info = manifest_info = ManifestHandler(
            self.source, manifest_blob, container_url, self.output_dir,
            self.overwrite, self.stdout
        ).handle()
        summary_entry.update({
            "report_name": manifest_info.get("manifest_data", {}).get("runInfo", {}).get("reportName"),
            "report_type": manifest_info.get("manifest_data", {}).get("runInfo", {}).get("reportType"),
            "start_date": manifest_info.get("manifest_data", {}).get("runInfo", {}).get("startDate"),
            "end_date": manifest_info.get("manifest_data", {}).get("runInfo", {}).get("endDate")
        })

        if not self.skip_csv:
            CsvHandler(
            self.source, manifest_info, container_url, self.output_dir,
            self.overwrite, self.stdout
        ).handle()
        summary_entry["csv_path"] = os.path.join(
            self.output_dir,
            os.path.basename(manifest_info.get("manifest_data", {}).get("blobs", [{}])[0].get("blobName", ""))
        )

        MetadataHandler(
            self.source, self.run_info, manifest_info, container_url,
            self.output_dir, self.overwrite, self.stdout
        ).handle()
        self.summary.append(summary_entry)


class ManifestHandler:
    """
    Responsible for retrieving and saving the manifest.json file
    associated with a billing run.
    """
    def __init__(self, source, blob, container_url, output_dir, overwrite, stdout):
        self.source = source
        self.blob = blob
        self.container_url = container_url
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.stdout = stdout

    def handle(self):
        path = os.path.join(self.output_dir, "manifest.json")
        if os.path.exists(path) and not self.overwrite:
            self.stdout.write("Manifest exists, skipping.")
            with open(path) as f:
                return {"manifest_data": json.load(f)}

        data = self.source.get_manifest_data(self.blob, self.container_url)
        with open(path, "w") as f:
            json.dump(data["manifest_data"], f, indent=2)
        self.stdout.write(f"Saved manifest to {path}")
        return data


class CsvHandler:
    """
    Downloads and optionally decompresses CSV data referenced in the manifest.
    """
    def __init__(self, source, manifest_info, container_url, output_dir, overwrite, stdout):
        self.source = source
        self.manifest_info = manifest_info
        self.container_url = container_url
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.stdout = stdout

    def handle(self):
        csv_blob_data, blob_name = self.source.download_csv_blob(
            self.manifest_info["manifest_data"], self.container_url
        )
        path = os.path.join(self.output_dir, os.path.basename(blob_name))

        if os.path.exists(path) and not self.overwrite:
            self.stdout.write("CSV exists, skipping.")
            return

        with open(path, "wb") as f:
            f.write(csv_blob_data)
        self.stdout.write(f"Saved CSV to {path}")

        if path.endswith(".gz"):
            self._decompress(path)

    def _decompress(self, path):
        target = os.path.splitext(path)[0]
        with gzip.open(path, 'rb') as f_in, open(target, 'wb') as f_out:
            f_out.write(f_in.read())
        self.stdout.write(f"Decompressed to {target}")


class MetadataHandler:
    """
    Generates and stores a metadata JSON file for the billing run,
    including information extracted from the manifest.
    """
    def __init__(self, source, run_info, manifest_info, container_url, output_dir, overwrite, stdout):
        self.source = source
        self.run_info = run_info
        self.manifest_info = manifest_info
        self.container_url = container_url
        self.output_dir = output_dir
        self.overwrite = overwrite
        self.stdout = stdout

    def handle(self):
        path = os.path.join(self.output_dir, "run_metadata.json")
        if os.path.exists(path) and not self.overwrite:
            self.stdout.write("Metadata exists, skipping.")
            return

        data = {
            "run_id": self.run_info.get("run_id"),
            "blob_name": self.run_info.get("blob_name"),
            "end_date": str(self.run_info.get("end_date")),
            "manifest_url": f"{self.container_url}/{self.run_info.get('blob_name')}",
            "download_timestamp": datetime.now().isoformat(),
            "source_name": self.source.name,
            "base_folder": self.source.base_folder,
        }

        manifest = self.manifest_info.get("manifest_data", {})
        if manifest:
            run_info = manifest.get("runInfo", {})
            data.update({
                "report_name": run_info.get("reportName"),
                "start_date": run_info.get("startDate"),
                "submitted_time": run_info.get("submittedTime"),
                "report_type": run_info.get("reportType"),
            })

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        self.stdout.write(f"Saved metadata to {path}")
