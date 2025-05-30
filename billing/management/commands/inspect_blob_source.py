import json
import logging
from urllib.parse import urlparse

from django.core.management.base import BaseCommand

from billing.models import BillingBlobSource, ImportSnapshot

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient, ContainerClient
except Exception:  # pragma: no cover - libs optional for offline env
    DefaultAzureCredential = None
    BlobClient = None
    ContainerClient = None

logger = logging.getLogger(__name__)


def parse_base_folder(base):
    parsed = urlparse(base)
    path = parsed.path.lstrip("/")
    parts = path.split("/", 1)
    container = parts[0]
    prefix = ""
    if len(parts) > 1:
        prefix = parts[1].rstrip("/") + "/"
    container_url = f"{parsed.scheme}://{parsed.netloc}/{container}"
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

    def handle(self, *args, **options):
        name = options["source_name"]
        period = options.get("billing_period")
        source = BillingBlobSource.objects.filter(name=name).first()
        if not source:
            self.stdout.write(self.style.ERROR(f"Source {name} not found"))
            return

        if DefaultAzureCredential is None:
            self.stderr.write("Azure SDK not installed")
            return

        try:
            cred = DefaultAzureCredential()
            container_url, prefix = parse_base_folder(source.base_folder)
            client = ContainerClient.from_container_url(container_url, credential=cred)

            listing_prefix = prefix
            if period:
                listing_prefix = f"{prefix}{period}/"

            blobs = client.list_blobs(name_starts_with=listing_prefix)
            manifests = [b for b in blobs if b.name.endswith("manifest.json")]
            if not manifests:
                self.stdout.write("No runs found")
                return

            for blob in manifests:
                manifest_url = f"{container_url}/{blob.name}"
                bclient = BlobClient.from_blob_url(manifest_url, credential=cred)
                manifest_data = json.loads(bclient.download_blob().readall())
                run_id = manifest_data.get("runInfo", {}).get("runId")
                end_date = manifest_data.get("runInfo", {}).get("endDate")
                imported = ImportSnapshot.objects.filter(run_id=run_id).exists()
                self.stdout.write(
                    f"run_id={run_id} end_date={end_date} size={blob.size} imported={imported}"
                )
        except Exception as exc:
            logger.error("Inspection failed: %s", exc)
            self.stderr.write(str(exc))
