import logging

from .models import CostReportSnapshot, BillingBlobSource, CostEntry

logger = logging.getLogger(__name__)

def get_latest_snapshot_for_date(billing_date):
    """
    Return the newest snapshot that contains cost entries for the specified billing date.

    This function first checks if there are any cost entries for the given date.
    If entries exist, it returns the most recent snapshot that includes those entries.
    If no entries exist for the date, it returns None.

    Args:
        billing_date (date): The date for which to find the latest snapshot.

    Returns:
        CostReportSnapshot or None: The newest snapshot containing cost entries for the date,
        or None if no entries exist for the date.
    """
    logger.debug("Fetching latest snapshot for date: %s", billing_date)

    # First check if there are any cost entries for this date
    if not CostEntry.objects.filter(date=billing_date).exists():
        logger.debug("No cost entries found for date: %s", billing_date)
        return None

    snapshot = (
        CostReportSnapshot.objects.filter(
            costentry__date=billing_date,
            status=CostReportSnapshot.Status.COMPLETE,
        )
        .order_by('-created_at')
        .first()
    )

    if snapshot:
        logger.debug("Found snapshot %s for date %s", snapshot.id, billing_date)
    else:
        logger.debug("No snapshot found for date %s", billing_date)

    return snapshot

def get_latest_snapshots_for_date(target_date):
    """
    Get the latest snapshots for each active billing source for the specified report date.

    This function iterates through all active billing sources and for each source,
    finds the latest snapshot with the target report date. If there are multiple
    snapshots for a source, it selects the one with the highest ID.

    Args:
        target_date (date): The report date for which to find the latest snapshots.

    Returns:
        list: A list of the latest CostReportSnapshot objects for each active source
        that has a snapshot for the target date.
    """
    from django.db.models import Max

    # Get all active sources
    active_sources = BillingBlobSource.objects.filter(is_active=True)

    # For each source, get the latest snapshot with the target report_date
    latest_snapshots = []
    for source in active_sources:
        # Get all snapshots for this source with the target report_date
        snapshots = CostReportSnapshot.objects.filter(
            source=source,
            report_date=target_date,
            status=CostReportSnapshot.Status.COMPLETE,
        )

        if snapshots.exists():
            # If there are multiple snapshots, get the one with the highest ID
            latest_snapshot = snapshots.order_by('-id').first()
            latest_snapshots.append(latest_snapshot)
        else:
            # No snapshot found for this source and date
            logger.debug("No snapshot found for source %s with report_date %s", source.name, target_date)

    return latest_snapshots

def get_latest_snapshots(date=None):
    """
    Return the latest completed snapshot per active source, optionally filtered by date.

    If a date is provided, this function returns the latest snapshots per source
    for that date. It also returns information about sources that don't have
    snapshots for the specified date.

    If no date is provided, it returns the most recent snapshot for each active
    source, regardless of date.

    Args:
        date (date, optional): The date to filter snapshots by. Defaults to None.

    Returns:
        tuple: A tuple containing:
            - list: A list of the latest CostReportSnapshot objects
            - list: A list of dictionaries with information about sources that are missing
                    snapshots for the specified date
    """
    snapshots = []
    missing = []

    if date:
        # If a date is provided, get the latest snapshots per source for that date
        snapshots = get_latest_snapshots_for_date(date)

        # Get all active sources
        active_sources = BillingBlobSource.objects.filter(is_active=True)
        # Check which sources are missing
        source_ids = [s.source_id for s in snapshots]
        missing_sources = active_sources.exclude(id__in=source_ids)
        missing = [{'name': src.name, 'reason': 'no entries for date'} for src in missing_sources]
    else:
        # Original logic for when no date is provided
        active_sources = BillingBlobSource.objects.filter(is_active=True)
        logger.debug("Processing %s active sources", active_sources.count())

        for source in active_sources:
            latest_snapshot = (
                CostReportSnapshot.objects.filter(
                    source=source,
                    status=CostReportSnapshot.Status.COMPLETE,
                )
                .order_by('-created_at')
                .first()
            )

            if latest_snapshot:
                snapshots.append(latest_snapshot)
                logger.debug("Resolved snapshot %s for source %s", latest_snapshot.id, source.name)
            else:
                missing.append({'name': source.name, 'reason': 'no snapshot'})
                logger.info("No snapshot for source %s: no snapshot", source.name)

    logger.debug("Found %s snapshots, %s sources missing", len(snapshots), len(missing))
    return snapshots, missing

def latest_snapshot_ids_for_date(date):
    """
    Return the IDs of the latest snapshots for each active billing source for the specified date.

    This function uses get_latest_snapshots_for_date to get the latest snapshots
    for each source, then extracts and returns their IDs.

    Args:
        date (date): The date for which to find the latest snapshot IDs.

    Returns:
        list: A list of snapshot IDs for the latest snapshots for each source.

    Example usage to sum billing costs for each resource:

    ```python
    from billing.utils import latest_snapshot_ids_for_date
    from billing.models import CostEntry
    from django.db.models import Sum
    import datetime

    # Get latest snapshot IDs for a specific date
    date = datetime.date(2023, 1, 1)
    snapshot_ids = latest_snapshot_ids_for_date(date)

    # Use SQL joins and aggregation to sum costs by resource
    resource_totals = CostEntry.objects.filter(
        snapshot_id__in=snapshot_ids
    ).values('resource_id').annotate(
        total_cost=Sum('cost_in_usd')
    ).order_by('resource_id')

    # Print total costs for each resource
    for entry in resource_totals:
        print(f"Resource {entry['resource_id']}: ${entry['total_cost']:.2f}")
    ```
    """
    logger.debug("Getting latest snapshot IDs for date: %s", date)
    snapshots = get_latest_snapshots_for_date(date)
    snapshot_ids = [s.id for s in snapshots]
    logger.debug("Found %s snapshot IDs for date %s", len(snapshot_ids), date)
    return snapshot_ids

def get_cost_entries_for_date(date=None):
    """
    Get cost entries for the latest snapshots, optionally filtered by date.

    This function encapsulates the logic for getting the latest snapshots for a given date
    and filtering cost entries based on these snapshots. It returns a queryset of cost entries
    that belong to the latest snapshots.

    Args:
        date (date, optional): The date to filter snapshots by. Defaults to None.

    Returns:
        QuerySet: A queryset of CostEntry objects that belong to the latest snapshots.
    """
    if date:
        # Get snapshot IDs for the specific date
        snapshot_ids = latest_snapshot_ids_for_date(date)
        logger.debug(f"Using {len(snapshot_ids)} snapshots specific to date {date}")
        return CostEntry.objects.filter(snapshot_id__in=snapshot_ids, date=date)
    else:
        # Get all latest snapshots
        snapshots, _ = get_latest_snapshots()
        snapshot_ids = [s.id for s in snapshots]
        logger.debug(f"Using {len(snapshot_ids)} latest snapshots (no date filter)")
        return CostEntry.objects.filter(snapshot_id__in=snapshot_ids)
