from .models import ImportSnapshot

def get_latest_snapshot_for_date(billing_date):
    """Return the newest snapshot that contains cost entries for billing_date."""
    return (
        ImportSnapshot.objects.filter(costentry__date=billing_date)
        .order_by('-created_at')
        .first()
    )

