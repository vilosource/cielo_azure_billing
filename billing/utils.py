from .models import CostReportSnapshot
from .models import BillingBlobSource


def get_latest_snapshot_for_date(billing_date):
    """Return the newest snapshot that contains cost entries for billing_date."""
    return (
        CostReportSnapshot.objects.filter(
            costentry__date=billing_date,
            status=CostReportSnapshot.Status.COMPLETE,
    )
        .order_by('-created_at')
        .first()
    )


def get_latest_snapshots(date=None):
    """Return latest completed snapshot per active source matching date if provided.

    Returns tuple of (snapshots, missing_sources).
    """
    snapshots = []
    missing = []
    for source in BillingBlobSource.objects.filter(is_active=True):
        qs = CostReportSnapshot.objects.filter(
            source=source,
            status=CostReportSnapshot.Status.COMPLETE,
        )
        if date:
            qs = qs.filter(costentry__date=date)
        snap = qs.order_by('-created_at').first()
        if snap:
            snapshots.append(snap)
        else:
            reason = 'no snapshot for date' if date else 'no snapshot'
            missing.append({'name': source.name, 'reason': reason})
    return snapshots, missing

