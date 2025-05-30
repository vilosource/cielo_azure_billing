import datetime
from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Sum
from django_filters.rest_framework import DjangoFilterBackend
from .models import CostReportSnapshot, Customer, Subscription, Resource, Meter, CostEntry
from .serializers import (
    CostReportSnapshotSerializer,
    CustomerSerializer,
    SubscriptionSerializer,
    ResourceSerializer,
    MeterSerializer,
    CostEntrySerializer,
)
from .permissions import PublicEndpointPermission
from .filters import CostEntryFilter
from .utils import get_latest_snapshot_for_date
from drf_spectacular.utils import extend_schema, OpenApiParameter


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
