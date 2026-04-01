# POS Connector Guide

This document is for the person connecting an external POS system to the Shopify Sync service.

## Connection Details

Base URL:

```text
https://shopify-sync-production-905f.up.railway.app
```

Primary products path:

```text
/wc-api/v3/products
```

Batch path:

```text
/wc-api/v3/products/batch
```

Authentication:

```text
consumer_key=<POS_KEY>
consumer_secret=<POS_SECRET>
```

Important:

```text
Type the path exactly as /wc-api/v3/products
Do not add spaces
Do not paste the full URL into the Path field
Do not add hidden characters
```

## Supported Requests

### 1. Read products

```http
GET /wc-api/v3/products?consumer_key=...&consumer_secret=...
```

Example:

```bash
curl -i "https://shopify-sync-production-905f.up.railway.app/wc-api/v3/products?consumer_key=POS_KEY&consumer_secret=POS_SECRET"
```

Expected response:

```json
[]
```

Or a JSON array of products:

```json
[
  {
    "id": "gid://shopify/Product/1234567890",
    "name": "Classic Tee",
    "slug": "classic-tee",
    "status": "draft",
    "sku": "ABC123",
    "barcode": "012345678905",
    "regular_price": "19.99",
    "stock_quantity": 10,
    "vendor": "POS Company",
    "product_type": "Apparel",
    "images": [
      {
        "src": "https://example.com/products/classic-tee.jpg"
      }
    ],
    "updated_at": "2026-04-01T20:00:00+00:00"
  }
]
```

### 2. Create or update one product

```http
POST /wc-api/v3/products?consumer_key=...&consumer_secret=...
Content-Type: application/json
```

Example request:

```bash
curl -i -X POST "https://shopify-sync-production-905f.up.railway.app/wc-api/v3/products?consumer_key=POS_KEY&consumer_secret=POS_SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Classic Tee",
    "sku": "ABC123",
    "barcode": "012345678905",
    "regular_price": "19.99",
    "stock_quantity": 10,
    "status": "draft",
    "vendor": "POS Company",
    "product_type": "Apparel",
    "description": "<p>Imported from POS</p>",
    "images": [
      { "src": "https://example.com/products/classic-tee.jpg" }
    ]
  }'
```

Behavior:

```text
If SKU already exists in Shopify, the product or variant is updated.
If SKU does not exist, a new Shopify product is created.
For first-time uploads, send status=draft.
```

### 3. Create or update multiple products

```http
POST /wc-api/v3/products/batch?consumer_key=...&consumer_secret=...
Content-Type: application/json
```

Example request:

```bash
curl -i -X POST "https://shopify-sync-production-905f.up.railway.app/wc-api/v3/products/batch?consumer_key=POS_KEY&consumer_secret=POS_SECRET" \
  -H "Content-Type: application/json" \
  -d '[
    {
      "name": "Classic Tee",
      "sku": "ABC123",
      "regular_price": "19.99",
      "stock_quantity": 10,
      "status": "draft"
    },
    {
      "name": "Canvas Hat",
      "sku": "DEF456",
      "regular_price": "24.99",
      "stock_quantity": 5,
      "status": "draft"
    }
  ]'
```

## Accepted Product Fields

The service accepts these common Woo-style fields:

```text
name or title
sku
barcode
ean
upc
gtin
regular_price or price
sale_price
stock_quantity or quantity
status
vendor
brand
product_type
description
short_description
images[].src
image_url
```

## Response Codes

```text
200 = success
401 = invalid key or secret
404 = wrong path
502 = Shopify token or store connection problem
```

## Path Rules

Use this exact path:

```text
/wc-api/v3/products
```

Do not use:

```text
/wc-api/v3/products 
/wc-api/v3/products%20
/wc-api/v3/products  /
```

## Notes

```text
New products should be sent with status=draft on first upload.
Images must be public HTTPS URLs.
The POS key and secret do not expire automatically.
They only change if they are manually rotated or if the database is reset.
```

## Troubleshooting

If the POS shows `404 Not Found`, check:

```text
The path was entered exactly as /wc-api/v3/products
There is no trailing space
There is no hidden character
The full URL was not pasted into the Path field
```

If the POS shows `401`, check:

```text
consumer_key is correct
consumer_secret is correct
The credentials were not rotated after the POS was configured
```

If the POS shows `502`, check:

```text
The Shopify app is installed on the correct store
The store is still connected
The Shopify token is valid
```
