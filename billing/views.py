from rest_framework import viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db.models import Sum
from .models import ImportSnapshot, Customer, Subscription, Resource, Meter, CostEntry
from .serializers import (
    ImportSnapshotSerializer,
    CustomerSerializer,
    SubscriptionSerializer,
    ResourceSerializer,
    MeterSerializer,
    CostEntrySerializer,
)
from .permissions import PublicEndpointPermission


class ImportSnapshotViewSet(viewsets.ModelViewSet):
    queryset = ImportSnapshot.objects.all()
    serializer_class = ImportSnapshotSerializer
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


class CostEntryViewSet(viewsets.ModelViewSet):
    queryset = CostEntry.objects.all()
    serializer_class = CostEntrySerializer
    permission_classes = [PublicEndpointPermission]

    def get_queryset(self):
        queryset = super().get_queryset()
        params = self.request.query_params
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

        for param, field in mapping.items():
            value = params.get(param)
            if value:
                queryset = queryset.filter(**{field: value})

        tags = params.get('tags')
        if tags:
            queryset = queryset.filter(tags__contains=tags)
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
