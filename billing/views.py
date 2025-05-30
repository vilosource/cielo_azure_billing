import datetime
from rest_framework import viewsets
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Sum, F
from django_filters.rest_framework import DjangoFilterBackend
from .models import BillingBlobSource, CostReportSnapshot, Customer, Subscription, Resource, Meter, CostEntry
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
from .utils import get_latest_snapshot_for_date, get_latest_snapshots
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


class CostReportSnapshotViewSet(viewsets.ModelViewSet):
    queryset = CostReportSnapshot.objects.all()
    serializer_class = CostReportSnapshotSerializer
    permission_classes = [PublicEndpointPermission]


class CustomerViewSet(viewsets.ModelViewSet):
    queryset = Customer.objects.all()
    serializer_class = CustomerSerializer
    permission_classes = [PublicEndpointPermission]


class SubscriptionViewSet(viewsets.ModelViewSet):
    queryset = Subscription.objects.all()
    serializer_class = SubscriptionSerializer
    permission_classes = [PublicEndpointPermission]


class ResourceViewSet(viewsets.ModelViewSet):
    queryset = Resource.objects.all()
    serializer_class = ResourceSerializer
    permission_classes = [PublicEndpointPermission]


class MeterViewSet(viewsets.ModelViewSet):
    queryset = Meter.objects.all()
    serializer_class = MeterSerializer
    permission_classes = [PublicEndpointPermission]


@extend_schema(
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
            except ValueError:
                return queryset.none()

            snapshot = get_latest_snapshot_for_date(billing_date)
            if snapshot:
                queryset = queryset.filter(snapshot=snapshot, date=billing_date)
            else:
                queryset = queryset.none()
        return queryset

    @action(detail=False)
    def aggregate(self, request):
        group_by = request.query_params.getlist('group_by')
        if not group_by:
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
            return Response({'detail': 'invalid group_by fields'}, status=400)

        data = (
            queryset.values(*fields)
            .annotate(total_cost=Sum('cost_in_usd'))
            .order_by()
        )
        return Response(list(data))


class BaseSummaryView(APIView):
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
        cache = get_cache_backend()
        date_str = request.GET.get('date')
        date = None
        if date_str:
            try:
                date = datetime.date.fromisoformat(date_str)
            except ValueError:
                return Response({'detail': 'invalid date'}, status=400)

        cache_key = self.get_cache_key(request, date_str)
        cached = cache.get(cache_key)
        if cached:
            return Response(cached)

        snapshots, missing = get_latest_snapshots(date)
        snapshot_ids = [s.id for s in snapshots]

        queryset = CostEntry.objects.filter(snapshot_id__in=snapshot_ids)
        if date:
            queryset = queryset.filter(date=date)

        filter_data = self.get_filter_data(request)
        filterset = self.filterset_class(filter_data, queryset=queryset)
        queryset = filterset.qs

        # Use temporary annotation names to avoid conflicts with model fields
        annotated_fields = {f"_{k}": v for k, v in self.group_by.items()}

        data = list(
            queryset
            .annotate(**annotated_fields)
            .values(*annotated_fields.keys())
            .annotate(total_usd=Sum('cost_in_usd'))
            .order_by()
        )

        # Rename temporary annotation keys to the expected response keys
        for row in data:
            for key in list(annotated_fields.keys()):
                alias = key.lstrip('_')
                row[alias] = row.pop(key)

        response_data = {
            'date': date.isoformat() if date else None,
            'sources_queried': BillingBlobSource.objects.filter(is_active=True).count(),
            'sources_included': len(snapshots),
            'sources_missing': missing,
            'data': data,
        }

        cache.set(cache_key, response_data, timeout=900)
        return Response(response_data)


@extend_schema(parameters=SUMMARY_FILTER_PARAMETERS)
class SubscriptionSummaryView(BaseSummaryView):
    group_by = {
        'subscription_id': F('subscription__subscription_id'),
        'subscription_name': F('subscription__name'),
    }


@extend_schema(parameters=SUMMARY_FILTER_PARAMETERS)
class VirtualMachineSummaryView(SubscriptionSummaryView):
    def get_filter_data(self, request):
        data = request.GET.copy()
        data.setdefault('meter_category', 'Virtual Machines')
        return data


@extend_schema(parameters=SUMMARY_FILTER_PARAMETERS)
class ResourceGroupSummaryView(BaseSummaryView):
    group_by = {
        'resource_group': F('resource__resource_group'),
    }


@extend_schema(parameters=SUMMARY_FILTER_PARAMETERS)
class MeterCategorySummaryView(BaseSummaryView):
    group_by = {
        'meter_category': F('meter__category'),
    }


@extend_schema(parameters=SUMMARY_FILTER_PARAMETERS)
class RegionSummaryView(BaseSummaryView):
    group_by = {
        'location': F('resource__location'),
    }
