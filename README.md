# Shopify Inventory Sync

FastAPI backend for syncing products, pricing, images, and inventory from an external POS into Shopify.

## What It Does

- installs as a Shopify app
- stores one Shopify connection per shop
- generates POS credentials per shop
- exposes Woo-style compatible product endpoints for POS integrations
- creates new Shopify products as drafts
- updates existing Shopify products by SKU
- syncs price and inventory
- preserves legacy POS matrix barcodes on Shopify size/color variants
- provides CSV exports for catalog and incoming feed activity

## Project Structure

```text
app/
  main.py
  config.py
  auth.py
  db.py
  inventory.py
  models.py
  shopify.py
  state.py
  utils.py
  static/
requirements.txt
.env.example
POS_CONNECTOR_GUIDE.md
```

## Requirements

- Python 3.10+
- Shopify app credentials
- persistent database storage in production

## Environment Variables

Copy `.env.example` to `.env` and set:

```env
SHOPIFY_CLIENT_ID=your_client_id
SHOPIFY_CLIENT_SECRET=your_client_secret
SHOPIFY_API_VERSION=2026-01
# SHOPIFY_LOCATION_ID=123456789
APP_BASE_URL=https://your-domain.example
APP_SCOPES=read_products,write_products,read_inventory,write_inventory,read_locations,read_orders
APP_SESSION_SECRET=replace_with_a_long_random_secret
POS_SECRET_ENCRYPTION_SECRET=replace_with_a_second_long_random_secret
DATABASE_PATH=inventory_sync.sqlite3
FEED_EVENT_RETENTION_ROWS=500
REQUEST_LOG_RETENTION_ROWS=500
ORDER_EVENT_RETENTION_ROWS=250
RECENT_ORDER_RETENTION_ROWS=50
```

Feed and request history is automatically pruned to these limits so recurring connector traffic
cannot grow the Railway SQLite volume without bound.

Full order payloads are a temporary delivery queue capped at 500 rows even if an older environment
setting is higher. Railway separately keeps only 50 small order summaries for the dashboard, and
the Windows Shopify order DBF inbox defaults to 250 orders with a hard maximum of 500.

Legacy POS ZIP uploads retain only the product/inventory DBFs and delete the ZIP after extraction.
The unattended Windows connector does not upload DBF files at all.

The unattended connector treats catalog data as a one-time import: zero-quantity products are
archived, descriptions start empty, and the generated product name is added as a tag. After a base
SKU is successfully imported, recurring connector traffic uses inventory and price-only endpoints.
Shopify edits to titles, descriptions, tags, images, and other merchandising fields are preserved;
a price changes only when `pricechg.dbf` records a newer POS price. Each successful product upload
also saves its SKU/variant quantity baseline before the connector marks that product complete. New
numeric products must produce the same complete catalog payload on two consecutive scans before
they are uploaded, which avoids reading a product while a POS copy operation is still being edited.
If an older connector already uploaded a copied matrix product during its temporary zero-stock,
single-variant stage, the upgraded connector detects that exact legacy shape and requires two stable
complete scans before rebuilding its variants. The repair is limited to a POS-managed product with
one default/base variant, zero inventory in every state at every Shopify location, and no conflicting
child SKUs; ambiguous or partially edited products are blocked for review. Shopify titles,
descriptions, tags, images, vendor, type, and status are omitted
from the structural mutation. Merchant-managed status remains unchanged; a product carrying the
sync's own zero-stock auto-archive marker is restored to Draft after positive child inventory is
confirmed. The connector also probes a known product when its POS type becomes a matrix but its
cached Shopify mapping still has no children, so future copy-then-finish transitions do not wait for
the nightly scan. Incomplete matrix definitions are probed again after a 15-minute cooldown.

The first connector run after an inventory or catalog-structure reconciliation upgrade, and one run
after local midnight each day, compares POS quantities with a lean Shopify inventory snapshot at one location.
This repairs SKUs that predate a local baseline or whose webhook was missed during downtime. Set
`SHOPIFY_LOCATION_ID` when the store has more than one inventory location; otherwise Shopify's
primary location is pinned on the first verified snapshot. Duplicate Shopify SKUs and items that
are unavailable at that location are blocked instead of being adjusted ambiguously.

Products archived automatically because their imported quantity was zero carry the internal
`pos.auto_archived_zero_stock` marker. A later positive stock update returns only those products to
Draft and clears the marker; an unmarked manual archive remains archived. Products archived before
this marker was introduced require a one-time manual change to Draft after their quantity is
repaired.

With `read_orders` authorized, Shopify order webhooks are held in a compact, version-safe Railway
queue until the Windows connector writes them to `C:\ashpsdat_web\shopify-order-header.dbf` and
`C:\ashpsdat_web\shopify-order-detail.dbf`. The connector creates the folder and files when they do
not exist. These genuine dBASE files are isolated from the native POS
FoxPro tables and contain normalized order header/detail rows for printing or a later vendor-tested
POS import; payment/card data and raw webhook payloads are not retained.
After adding `read_orders` to an existing Railway `APP_SCOPES` value, run the Shopify install flow
again so the store grants the new scope. Depending on the app's Shopify distribution and protected
customer data approval, customer contact and shipping fields can be redacted while order and SKU
fields still sync.

## Local Run

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Start the app:

```bash
uvicorn app.main:app --reload
```

## Shopify App Settings

Use these values in Shopify:

```text
App URL: https://your-domain.example/
Allowed redirection URL: https://your-domain.example/auth/callback
```

## Railway Production Notes

For persistent credentials and installs, use a volume.

Mount path:

```text
/data
```

Set:

```env
DATABASE_PATH=/data/inventory_sync.sqlite3
```

Without persistent storage, generated POS keys and secrets can change after redeploys because the SQLite database is lost.

## Main Endpoints

Health:

```text
GET /health
GET /health/shopify
```

UI:

```text
GET /app
GET /app/product-sync
GET /app/catalog
GET /app/settings
```

POS-facing API:

```text
GET/POST /wc-api/v3/products
GET/POST /wp-json/wc/v3/products
GET/PUT /wc-api/v3/products/{id}
GET/PUT /wp-json/wc/v3/products/{id}
GET/POST /wc-api/v3/products/batch
POST /wc-api/v3/products/reconcile
GET /wc-api/v3/inventory
POST /wc-api/v3/inventory/adjustments
GET /wc-api/v3/inventory/changes
POST /wc-api/v3/inventory/changes/ack
GET/POST /sync/product
GET/POST /sync/bulk
GET /sync/inventory
POST /sync/inventory/adjustments
GET /sync/inventory/changes
POST /sync/inventory/changes/ack
POST /sync/catalog/reconcile
POST /sync/catalog/matrix-structure/repair
```

Catalog reconciliation accepts the complete source SKU list and previews Shopify products whose
SKUs are absent. Applying the archive requires both `apply: true` and the explicit
`confirmation: "ARCHIVE_MISSING_PRODUCTS"` value. Products without SKUs and products whose variant
list is truncated are never archived by reconciliation. Reconciliation only manages products marked
with the `pos.sku` metafield by a previous rich POS sync, so unrelated Shopify products remain intact.
Matrix variant SKUs such as `21741. 1 1` reconcile against their managed base SKU `21741`.

## Windows Connector

The unattended POS bridge is documented in [windows_connector/README.md](windows_connector/README.md).
It starts with Windows, uploads product details only during the initial catalog import, processes new
`invdtl.dbf` and `editvoid.dbf` events every three minutes, and runs one full quantity reconciliation
after local midnight. The first upgraded run also forces this reconciliation immediately. Deploy the
Railway/backend update before updating the Windows connector; the connector rejects an older snapshot
schema without acknowledging queued changes. It sends JSON changes rather than DBF ZIP archives and
keeps only bounded state and rotating logs on the host computer.

Exports:

```text
GET /api/catalog.csv
GET /api/feed.csv
GET /api/request-logs.csv
```

## POS Connector

Use the shareable connector guide here:

[POS_CONNECTOR_GUIDE.md](https://github.com/AnthonyBarbaro/shopify-sync/blob/main/POS_CONNECTOR_GUIDE.md)

Recommended POS path:

```text
/wc-api/v3/products
```

The server is tolerant of malformed Woo-style path variants for compatibility, but new integrations should always use the clean path above.

Strict WooCommerce-compatible path:

```text
/wp-json/wc/v3/products
```

Woo-style product updates by ID are also supported:

```text
PUT /wp-json/wc/v3/products/{id}
```

## Notes

- new products should be sent with `status=draft` on first upload
- images must be public `https://` URLs
- POS credentials do not expire automatically
- POS credentials only change if rotated manually or the database is reset
