from .models import CostReportSnapshot

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

