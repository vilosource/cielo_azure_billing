# API Reference

This document describes the REST API provided by the Cielo Azure Billing project.

## Authentication

The API uses token authentication via Django REST Framework. When the setting `API_AUTH_DISABLED` is `True`, authentication is skipped and all endpoints are public. Otherwise a token is required.

## Endpoints

### CostReportSnapshot
- **Base path**: `/api/snapshots/`
- Standard CRUD operations are available.
- A snapshot represents a single imported cost CSV file and includes fields such as `run_id`, `report_date` and `status`.

### Customer
- **Base path**: `/api/customers/`
- Manage Azure tenants that own subscriptions.

### Subscription
- **Base path**: `/api/subscriptions/`
- Each subscription belongs to a customer and can be filtered in the cost endpoints.

### Resource
- **Base path**: `/api/resources/`
- Represents an Azure resource referenced in cost reports.

### Meter
- **Base path**: `/api/meters/`
- Lookup table for meter metadata used by billing entries.

### CostEntry
- **Base path**: `/api/cost-entries/`
- Lists imported billing line items. When the query parameter `date` is provided the view resolves the most recent snapshot that includes that billing date.
- Supports filtering by subscription, resource group, meter category and more. See `docs/CostsFilteringGuide.md` for details.
- The endpoint also exposes an `aggregate/` action. Provide one or more `group_by` parameters (e.g. `resourceGroupName`, `subscriptionName`) to receive aggregated totals in USD.

### Subscription Summary
- **Path**: `/api/costs/subscription-summary/`
- Returns aggregated costs grouped by subscription. Accepts the common filtering parameters.

### Virtual Machines Summary
- **Path**: `/api/costs/virtual-machines-summary/`
- Same as the subscription summary but automatically filters by `meter_category=Virtual Machines`.

### Resource Group Summary
- **Path**: `/api/costs/resource-group-summary/`
- Aggregates costs by resource group.

### Meter Category Summary
- **Path**: `/api/costs/meter-category-summary/`
- Aggregates costs by top level meter category (e.g. compute or storage).

### Region Summary
- **Path**: `/api/costs/region-summary/`
- Aggregates costs by Azure region.

### Resource Group Totals
- **Path**: `/api/costs/resource-group-totals/`
- Requires `resource_group` parameter. Returns total cost per resource inside that group.

### Available Report Dates
- **Path**: `/api/costs/available-report-dates/`
- Returns billing dates that exist for a specified month (defaults to the current month).

### Snapshot Report Dates
- **Path**: `/api/snapshots/available-report-dates/`
- Lists `report_date` values from completed snapshots. These dates can be used with the summary endpoints.

## How Cost Summaries Are Calculated

All summary endpoints inherit from `BaseSummaryView` located in `billing/views.py`. The `get` method performs the following steps:

1. Resolve the latest completed `CostReportSnapshot` for each active `BillingBlobSource` using `get_latest_snapshots()`.
2. If a `date` is supplied, determine the newest snapshot for each subscription on that day and filter `CostEntry` rows to those IDs.
3. Apply additional filters based on the query parameters.
4. Group the remaining entries by the configured fields and sum `cost_in_usd` and `cost_in_billing_currency`.
5. Cache the result for subsequent requests.

The relevant implementation is shown below:

```python
# billing/views.py
class BaseSummaryView(APIView):
    ...
    def get(self, request):
        cache = get_cache_backend()
        date_str = request.GET.get('date')
        ...
        snapshots, missing = get_latest_snapshots(date)
        if date:
            ids = latest_snapshot_ids_for_date(date)
            queryset = CostEntry.objects.filter(snapshot_id__in=ids, date=date)
        else:
            queryset = CostEntry.objects.filter(snapshot_id__in=[s.id for s in snapshots])
        filterset = self.filterset_class(request.GET, queryset=queryset)
        data = (
            filterset.qs
            .annotate(**{f"_{k}": v for k, v in self.group_by.items()})
            .values(*[f"_{k}" for k in self.group_by])
            .annotate(
                total_usd=Sum('cost_in_usd'),
                total_billing=Sum('cost_in_billing_currency'),
            )
            .order_by()
        )
```

Snapshots are selected through the helper function:

```python
# billing/utils.py
def get_latest_snapshots(date=None):
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
```

To locate the newest snapshot for each subscription on a particular day, the helper below is used:

```python
def latest_snapshot_ids_for_date(date):
    return (
        CostEntry.objects.filter(
            date=date,
            snapshot__status=CostReportSnapshot.Status.COMPLETE,
        )
        .values("subscription_id")
        .annotate(latest_id=Max("snapshot_id"))
        .values_list("latest_id", flat=True)
    )
```

The API response from a summary endpoint includes the date used, counts of sources included or missing and the aggregated data list.
