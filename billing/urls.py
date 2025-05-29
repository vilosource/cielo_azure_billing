from rest_framework.routers import DefaultRouter
from django.urls import path, include
from .views import (
    ImportSnapshotViewSet,
    CustomerViewSet,
    SubscriptionViewSet,
    ResourceViewSet,
    MeterViewSet,
    CostEntryViewSet,
)

router = DefaultRouter()
router.register('snapshots', ImportSnapshotViewSet)
router.register('customers', CustomerViewSet)
router.register('subscriptions', SubscriptionViewSet)
router.register('resources', ResourceViewSet)
router.register('meters', MeterViewSet)
router.register('cost-entries', CostEntryViewSet)

urlpatterns = [
    path('', include(router.urls)),
]
