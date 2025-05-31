# Filtering Cost API Endpoints

All cost related API endpoints accept the same set of query parameters. These parameters allow you to limit results to a particular subscription, resource or tag and can be combined freely.

| Parameter | Description |
|-----------|-------------|
| `date` | Billing date. When omitted, the API uses the latest available data. When provided, results are taken from the newest snapshot for each subscription on that day. |
| `subscription_id` | Filter by subscription ID. |
| `resource_group` | Filter by Azure resource group. Names are stored in lowercase. |
| `location` | Azure region of the resource. |
| `meter_category` | High level service category such as `Virtual Machines` or `Storage`. |
| `meter_subcategory` | Optional sub category like `Premium SSD`. |
| `pricing_model` | Pricing model, e.g. `OnDemand` or `Spot`. |
| `publisher_name` | Marketplace publisher name. |
| `resource_name` | Name of the resource. |
| `min_cost`/`max_cost` | Cost range filters (in USD). |
| `source_id` | ID of the `BillingBlobSource` import. |
| `tag_key` and `tag_value` | Filter by resource tags. Use both together to match a specific tag pair. |

Example request filtering by subscription and meter category:

```http
GET /api/costs/subscription-summary/?date=2024-01-01&subscription_id=abcd-1234&meter_category=Virtual%20Machines
```

Cost entry endpoints continue to honour their original camelCase parameters so existing clients will keep working.
