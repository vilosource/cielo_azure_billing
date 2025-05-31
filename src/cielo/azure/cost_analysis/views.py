import datetime
import calendar
import logging
from rest_framework import viewsets
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Sum, F
from django_filters.rest_framework import DjangoFilterBackend
from .models import BillingBlobSource, CostReportSnapshot, Customer, Subscription, Resource, Meter, CostEntry

# Configure module logger
logger = logging.getLogger(__name__)
from .serializers import (
    CostReportSnapshotSerializer,
    CustomerSerializer,
    SubscriptionSerializer,
    ResourceSerializer,
    MeterSerializer,
    CostEntrySerializer,
)
from .permissions import PublicEndpointPermission
from .filters import CostEntryFilter, CostSummaryFilter
from .utils import (
    get_latest_snapshot_for_date,
    get_latest_snapshots,
    latest_snapshot_ids_for_date,
    get_cost_entries_for_date,
)
from caching import get_cache_backend
from drf_spectacular.utils import extend_schema, OpenApiParameter

# Shared filter parameter docs for cost summary endpoints
SUMMARY_FILTER_PARAMETERS = [
    OpenApiParameter(name="date", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="subscription_id", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="resource_group", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="location", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="meter_category", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="meter_subcategory", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="pricing_model", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="publisher_name", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="resource_name", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="min_cost", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="max_cost", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="source_id", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="tag_key", location=OpenApiParameter.QUERY, required=False, type=str),
    OpenApiParameter(name="tag_value", location=OpenApiParameter.QUERY, required=False, type=str),
]

@extend_schema(
    description="Create and inspect snapshot records for imported Azure cost reports.")
class CostReportSnapshotViewSet(viewsets.ModelViewSet):
    """CRUD interface for :class:`CostReportSnapshot` objects."""
    queryset = CostReportSnapshot.objects.all()
    serializer_class = CostReportSnapshotSerializer
    permission_classes = [PublicEndpointPermission]

    def list(self, request, *args, **kwargs):
        start_time = datetime.datetime.now()
        logger.info("Listing CostReportSnapshots")
        response = super().list(request, *args, **kwargs)
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(f"CostReportSnapshot list completed in {duration:.2f}s, returning {len(response.data)} records")
        return response

    def retrieve(self, request, *args, **kwargs):
        logger.info(f"Retrieving CostReportSnapshot with ID: {kwargs.get('pk')}")
        return super().retrieve(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        logger.info("Creating new CostReportSnapshot")
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        logger.info(f"Updating CostReportSnapshot with ID: {kwargs.get('pk')}")
        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        logger.warning(f"Deleting CostReportSnapshot: {instance.id} ({instance.file_name})")
        return super().destroy(request, *args, **kwargs)

@extend_schema(description="Manage Azure customers that own subscriptions.")
class CustomerViewSet(viewsets.ModelViewSet):
    """API endpoint for managing :class:`Customer` objects."""
    queryset = Customer.objects.all()
    serializer_class = CustomerSerializer
    permission_classes = [PublicEndpointPermission]

    def list(self, request, *args, **kwargs):
        logger.info("Listing Customers")
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        logger.info(f"Retrieving Customer with ID: {kwargs.get('pk')}")
        return super().retrieve(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        logger.info(f"Creating new Customer with tenant_id: {request.data.get('tenant_id')}")
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        logger.info(f"Updating Customer with ID: {kwargs.get('pk')}")
        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        logger.warning(f"Deleting Customer: {instance.tenant_id} ({instance.name})")
        return super().destroy(request, *args, **kwargs)

@extend_schema(description="Manage Azure subscriptions that incur costs.")
class SubscriptionViewSet(viewsets.ModelViewSet):
    """API endpoint for :class:`Subscription` data."""
    queryset = Subscription.objects.all()
    serializer_class = SubscriptionSerializer
    permission_classes = [PublicEndpointPermission]

    def list(self, request, *args, **kwargs):
        logger.info("Listing Subscriptions")
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        logger.info(f"Retrieving Subscription with ID: {kwargs.get('pk')}")
        return super().retrieve(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        logger.info(f"Creating new Subscription: {request.data.get('name')} for customer: {request.data.get('customer')}")
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        logger.info(f"Updating Subscription with ID: {kwargs.get('pk')}")
        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        logger.warning(f"Deleting Subscription: {instance.subscription_id} ({instance.name})")
        return super().destroy(request, *args, **kwargs)

@extend_schema(description="Query and update Azure resources referenced in cost reports.")
class ResourceViewSet(viewsets.ModelViewSet):
    """CRUD operations for :class:`Resource` models."""
    queryset = Resource.objects.all()
    serializer_class = ResourceSerializer
    permission_classes = [PublicEndpointPermission]

    def list(self, request, *args, **kwargs):
        logger.info("Listing Resources")
        # Log filter parameters if they exist
        filters = request.query_params
        if filters:
            logger.debug(f"Resource list filters: {dict(filters)}")
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        logger.info(f"Retrieving Resource with ID: {kwargs.get('pk')}")
        return super().retrieve(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        logger.info(f"Creating new Resource: {request.data.get('resource_name', 'Unnamed')}")
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        logger.info(f"Updating Resource with ID: {kwargs.get('pk')}")
        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        logger.warning(f"Deleting Resource: {instance.resource_id}")
        return super().destroy(request, *args, **kwargs)

@extend_schema(description="Lookup Azure meter metadata used for billing entries.")
class MeterViewSet(viewsets.ModelViewSet):
    """API endpoint for :class:`Meter` objects."""
    queryset = Meter.objects.all()
    serializer_class = MeterSerializer
    permission_classes = [PublicEndpointPermission]

    def list(self, request, *args, **kwargs):
        logger.info("Listing Meters")
        # Log filter parameters if they exist
        filters = request.query_params
        if filters:
            logger.debug(f"Meter list filters: {dict(filters)}")
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        logger.info(f"Retrieving Meter with ID: {kwargs.get('pk')}")
        return super().retrieve(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        logger.info(f"Creating new Meter: {request.data.get('name')} ({request.data.get('meter_id')})")
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        logger.info(f"Updating Meter with ID: {kwargs.get('pk')}")
        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        logger.warning(f"Deleting Meter: {instance.meter_id} ({instance.name})")
        return super().destroy(request, *args, **kwargs)

@extend_schema(
    description="Detailed billing line items from imported cost reports.",
    parameters=[
        OpenApiParameter(name="resourceGroupName", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="subscriptionName", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="meterCategory", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="meterSubCategory", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="serviceFamily", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="resourceLocation", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="chargeType", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="pricingModel", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="publisherName", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="costCenter", location=OpenApiParameter.QUERY, required=False, type=str),
        OpenApiParameter(name="tags", location=OpenApiParameter.QUERY, required=False, type=str),
    ]
)
class CostEntryViewSet(viewsets.ModelViewSet):
    """CRUD and aggregated access to individual billing entries."""
    queryset = CostEntry.objects.all()
    serializer_class = CostEntrySerializer
    permission_classes = [PublicEndpointPermission]
    filter_backends = [DjangoFilterBackend]
    filterset_class = CostEntryFilter

    def get_queryset(self):
        queryset = super().get_queryset()
        date_str = self.request.query_params.get('date')
        if date_str:
            try:
                billing_date = datetime.date.fromisoformat(date_str)
                logger.debug(f"Filtering CostEntry by date: {billing_date}")
            except ValueError:
                logger.warning(f"Invalid date format in request: {date_str}")
                return queryset.none()

            snapshot = get_latest_snapshot_for_date(billing_date)
            if snapshot:
                logger.debug(f"Found snapshot {snapshot.id} for date {billing_date}")
                queryset = queryset.filter(snapshot=snapshot, date=billing_date)
            else:
                logger.warning(f"No snapshot found for date {billing_date}")
                queryset = queryset.none()
        return queryset

    @action(detail=False)
    def aggregate(self, request):
        group_by = request.query_params.getlist('group_by')
        logger.info(f"CostEntry aggregate request with group_by: {group_by}")

        if not group_by:
            logger.warning("Aggregate request missing required group_by parameter")
            return Response({'detail': 'group_by parameter required'}, status=400)

        queryset = self.get_queryset()
        mapping = {
            'resourceGroupName': 'resource__resource_group',
            'subscriptionName': 'subscription__name',
            'meterCategory': 'meter__category',
            'meterSubCategory': 'meter__subcategory',
            'serviceFamily': 'meter__service_family',
            'resourceLocation': 'resource__location',
            'chargeType': 'charge_type',
            'pricingModel': 'pricing_model',
            'publisherName': 'publisher_name',
            'costCenter': 'cost_center',
        }
        fields = [mapping.get(f) for f in group_by if mapping.get(f)]
        if not fields:
            logger.warning(f"Invalid group_by fields: {group_by}")
            return Response({'detail': 'invalid group_by fields'}, status=400)

        logger.debug(f"Aggregating by fields: {fields}")
        data = (
            queryset.values(*fields)
            .annotate(total_cost=Sum('cost_in_usd'))
            .order_by()
        )
        result = list(data)
        logger.debug(f"Aggregate query returned {len(result)} results")
        return Response(result)

class BaseSummaryView(APIView):
    """Base class for cost summary endpoints aggregating `CostEntry` data."""
    permission_classes = [PublicEndpointPermission]
    filterset_class = CostSummaryFilter
    group_by = None  # tuple of (queryset field expressions, response keys)

    def get_cache_key(self, request, date):
        params = sorted(request.GET.items())
        key = f"{request.path}|{date}|{params}"
        return key

    def get_filter_data(self, request):
        """Return query parameters for filterset initialization."""
        return request.GET

    def get(self, request):
        """Resolve latest snapshots, apply filters and aggregate grouped data."""
        view_name = self.__class__.__name__
        logger.info(f"{view_name} request received")

        cache = get_cache_backend()
        date_str = request.GET.get('date')
        date = None
        if date_str:
            try:
                date = datetime.date.fromisoformat(date_str)
                logger.debug(f"{view_name} request for date: {date}")
            except ValueError:
                logger.warning(f"{view_name} received invalid date: {date_str}")
                return Response({'detail': 'invalid date'}, status=400)

        cache_key = self.get_cache_key(request, date_str)
        cached = cache.get(cache_key)
        if cached:
            logger.debug(f"{view_name} cache hit for key: {cache_key}")
            return Response(cached)
        else:
            logger.debug(f"{view_name} cache miss for key: {cache_key}")

        # Get cost entries for the specified date
        queryset = get_cost_entries_for_date(date)
        logger.debug(f"Base queryset created with {queryset.count()} entries")

        view_name = self.__class__.__name__
        filter_data = self.get_filter_data(request)
        logger.debug(f"{view_name} applying filters: {filter_data}")

        filterset = self.filterset_class(filter_data, queryset=queryset)
        queryset = filterset.qs
        logger.debug(f"{view_name} filtered queryset has {queryset.count()} entries")

        # Use temporary annotation names to avoid conflicts with model fields
        annotated_fields = {f"_{k}": v for k, v in self.group_by.items()}
        logger.debug(f"{view_name} annotating with fields: {annotated_fields}")

        start_time = datetime.datetime.now()
        data = list(
            queryset
            .annotate(**annotated_fields)
            .values(*annotated_fields.keys())
            .annotate(
                total_usd=Sum('cost_in_usd'),
                total_billing=Sum('cost_in_billing_currency'),
            )
            .order_by()
        )
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(f"{view_name} aggregation query completed in {duration:.2f} seconds, returning {len(data)} results")

        # Rename temporary annotation keys to the expected response keys
        for row in data:
            for key in list(annotated_fields.keys()):
                alias = key.lstrip('_')
                row[alias] = row.pop(key)

        response_data = {
            'date': date.isoformat() if date else None,
            'sources_queried': BillingBlobSource.objects.filter(is_active=True).count(),
            'sources_included': len(data),
            'sources_missing': [],
            'data': data,
        }

        # Cache the result
        cache.set(cache_key, response_data, timeout=900)
        logger.debug(f"{view_name} caching results with key: {cache_key}, timeout: 900 seconds")

        return Response(response_data)

@extend_schema(
    description="Summarize costs by subscription.",
    parameters=SUMMARY_FILTER_PARAMETERS,
)
class SubscriptionSummaryView(BaseSummaryView):
    """Return aggregated cost data grouped by subscription."""
    group_by = {
        'subscription_id': F('subscription__subscription_id'),
        'subscription_name': F('subscription__name'),
    }

@extend_schema(
    description="Summarize virtual machine costs across subscriptions.",
    parameters=SUMMARY_FILTER_PARAMETERS,
)
class VirtualMachineSummaryView(SubscriptionSummaryView):
    """Summary of virtual machine spend for each subscription."""
    def get_filter_data(self, request):
        data = request.GET.copy()
        data.setdefault('meter_category', 'Virtual Machines')
        return data

@extend_schema(
    description="Summarize costs aggregated by resource group.",
    parameters=SUMMARY_FILTER_PARAMETERS,
)
class ResourceGroupSummaryView(BaseSummaryView):
    """Aggregate costs by resource group across all subscriptions."""
    group_by = {
        'resource_group': F('resource__resource_group'),
    }

@extend_schema(
    description="Summarize costs by meter category, such as compute or storage.",
    parameters=SUMMARY_FILTER_PARAMETERS,
)
class MeterCategorySummaryView(BaseSummaryView):
    """Summaries grouped by top level meter category."""
    group_by = {
        'meter_category': F('meter__category'),
    }

@extend_schema(
    description="Summarize costs by Azure region.",
    parameters=SUMMARY_FILTER_PARAMETERS,
)
class RegionSummaryView(BaseSummaryView):
    """Summaries grouped by Azure geographic region."""
    group_by = {
        'location': F('resource__location'),
    }

@extend_schema(description="List billing dates available for a given month.")
class AvailableReportDatesView(APIView):
    """Return distinct billing dates available within a month."""
    permission_classes = [PublicEndpointPermission]

    def get(self, request):
        logger.info("AvailableReportDatesView request received")

        month_str = request.GET.get('month')
        if month_str:
            try:
                year, month = [int(part) for part in month_str.split('-')]
                month_date = datetime.date(year, month, 1)
                logger.debug(f"Using requested month: {month_str}")
            except ValueError:
                logger.warning(f"Invalid month format: {month_str}")
                return Response({'detail': 'invalid month'}, status=400)
        else:
            today = datetime.date.today()
            month_date = today.replace(day=1)
            logger.debug(f"No month specified, using current month: {month_date.strftime('%Y-%m')}")

        last_day = calendar.monthrange(month_date.year, month_date.month)[1]
        start_date = month_date
        end_date = month_date.replace(day=last_day)
        logger.debug(f"Date range: {start_date} to {end_date}")

        # Find relevant snapshots for each active source
        active_sources = BillingBlobSource.objects.filter(is_active=True)
        logger.debug(f"Checking {active_sources.count()} active sources for snapshots")

        snapshot_ids = []
        for source in active_sources:
            snap = (
                CostReportSnapshot.objects.filter(
                    source=source,
                    status=CostReportSnapshot.Status.COMPLETE,
                    costentry__date__range=(start_date, end_date),
                )
                .order_by('-created_at')
                .first()
            )
            if snap:
                logger.debug(f"Found snapshot {snap.id} for source {source.name}")
                snapshot_ids.append(snap.id)
            else:
                logger.debug(f"No snapshots found for source {source.name} in date range")

        logger.info(f"Found {len(snapshot_ids)} relevant snapshots for month {month_date.strftime('%Y-%m')}")

        # Get distinct dates from cost entries
        dates = (
            CostEntry.objects.filter(snapshot_id__in=snapshot_ids, date__range=(start_date, end_date))
            .values_list('date', flat=True)
            .distinct()
            .order_by('date')
        )

        dates_list = [d.isoformat() for d in dates]
        logger.info(f"Found {len(dates_list)} available dates for month {month_date.strftime('%Y-%m')}")
        logger.debug(f"Available dates: {dates_list}")

        return Response(
            {
                'month': month_date.strftime('%Y-%m'),
                'available_dates': dates_list,
            }
        )

@extend_schema(description="List snapshot report dates available for summaries.")
class SnapshotReportDatesView(APIView):
    """Return distinct ``report_date`` values from completed snapshots."""
    permission_classes = [PublicEndpointPermission]


    def get(self, request):
        logger.info("SnapshotReportDatesView request received")

        print("========= SnapshotReportDatesView initialized =========")
        start_time = datetime.datetime.now()

        # Log any query parameters if present
        if request.GET:
            logger.debug(f"SnapshotReportDatesView received query params: {dict(request.GET)}")

        try:
            dates = (
                CostReportSnapshot.objects.filter(status=CostReportSnapshot.Status.COMPLETE)
                .exclude(report_date__isnull=True)
                .values_list('report_date', flat=True)
                .distinct()
                .order_by('report_date')
            )

            dates_list = [d.isoformat() for d in dates]
            duration = (datetime.datetime.now() - start_time).total_seconds()

            logger.info(f"Found {len(dates_list)} distinct report dates from complete snapshots in {duration:.2f}s")
            logger.debug(f"Available report dates: {dates_list}")

            # Log date range information if dates exist
            if dates_list:
                logger.info(f"Report date range: {dates_list[0]} to {dates_list[-1]}")
            else:
                logger.warning("No report dates available from complete snapshots")

            return Response({'available_report_dates': dates_list})
        except Exception as e:
            logger.error(f"Error retrieving snapshot report dates: {str(e)}", exc_info=True)
            return Response({'detail': 'Error retrieving snapshot report dates'}, status=500)

@extend_schema(description="Total cost per resource for a specified resource group.")
class ResourceGroupTotalsView(APIView):
    """Return total cost per resource within a resource group."""
    permission_classes = [PublicEndpointPermission]
    filterset_class = CostSummaryFilter

    def get_cache_key(self, request, date):
        params = sorted(request.GET.items())
        rg = request.GET.get('resource_group', '')
        return f"resource-group-totals|{rg}|{date}|{params}"

    def get(self, request):
        logger.info("ResourceGroupTotalsView request received")

        resource_group = request.GET.get('resource_group')
        if not resource_group:
            logger.warning("ResourceGroupTotalsView called without required resource_group parameter")
            return Response({'detail': 'resource_group parameter required'}, status=400)
        else:
            logger.debug(f"ResourceGroupTotalsView for resource group: {resource_group}")

        date_str = request.GET.get('date')
        date = None
        if date_str:
            try:
                date = datetime.date.fromisoformat(date_str)
                logger.debug(f"Using date filter: {date}")
            except ValueError:
                logger.warning(f"Invalid date format: {date_str}")
                return Response({'detail': 'invalid date'}, status=400)

        # Check cache first
        cache = get_cache_backend()
        cache_key = self.get_cache_key(request, date_str)
        cached = cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for resource group totals: {cache_key}")
            return Response(cached)
        else:
            logger.debug(f"Cache miss for resource group totals: {cache_key}")

        # Get cost entries for the specified date
        queryset = get_cost_entries_for_date(date)
        queryset = queryset.filter(resource__resource_group=resource_group)

        logger.debug(f"Base queryset built with {queryset.count()} entries")

        # Apply additional filters
        logger.debug(f"Applying additional filters: {request.GET}")
        filterset = self.filterset_class(request.GET, queryset=queryset)
        queryset = filterset.qs
        logger.debug(f"After filtering: {queryset.count()} entries remain")

        # Execute query with aggregation
        start_time = datetime.datetime.now()
        data = list(
            queryset
            .values('resource__resource_id', 'resource__resource_name')
            .annotate(
                total_usd=Sum('cost_in_usd'),
                total_billing=Sum('cost_in_billing_currency'),
            )
            .order_by('-total_usd')
        )
        duration = (datetime.datetime.now() - start_time).total_seconds()

        logger.info(f"Resource group totals query completed in {duration:.2f} seconds, found {len(data)} resources")

        if data:
            top_resource = data[0]
            logger.debug(f"Top resource by cost: {top_resource['resource__resource_name']} - ${top_resource['total_usd']}")

        # Build response and cache
        response_data = {
            'resource_group': resource_group,
            'date': date.isoformat() if date else None,
            'total_resources': len(data),
            'data': data,
        }

        cache.set(cache_key, response_data, timeout=900)
        logger.debug(f"Cached results with key: {cache_key}, timeout: 900 seconds")

        return Response(response_data)
