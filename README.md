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
APP_BASE_URL=https://your-domain.example
APP_SCOPES=read_products,write_products,read_inventory,write_inventory,read_locations
APP_SESSION_SECRET=replace_with_a_long_random_secret
POS_SECRET_ENCRYPTION_SECRET=replace_with_a_second_long_random_secret
DATABASE_PATH=inventory_sync.sqlite3
```

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
GET/POST /sync/product
GET/POST /sync/bulk
```

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
