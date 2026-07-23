# Shopify Order Bridge Database

The Windows connector creates `C:\ashpsdat\shopify-orders.db` as a SQLite database. It is a separate
inbox for Shopify orders and does not modify or copy `Ordhdr.dbf`, `Orddtl.dbf`, `Customer.dbf`, or
`CustShip.dbf`.

## POS-facing views

`order_header` is the equivalent of an order header. One row represents one Shopify order and includes:

- `order_id`, `invoice_no`, `order_number`, and `confirmation_number`;
- order, processed, cancellation, and closure timestamps;
- financial and fulfillment statuses;
- customer name, email, and phone;
- billing name/company/address/phone;
- shipping name/company/address/phone and shipping method;
- subtotal, order discount, shipping charge, handling, tax, currency, and total;
- note, tags, print status, and POS import status.

Shopify has no separate POS-style handling amount, so `handling` is `0.00`. The complete Shopify
shipping charge is in `shipping`.

`order_detail` is the equivalent of order detail. It contains:

- `order_id`, `invoice_no`, `line_number`, and Shopify line/product/variant identifiers;
- `sku`, `qty`, current quantity, unit price, discount, tax, and extension;
- description, Shopify variant description, vendor, weight, shipping requirement, and fulfillment status.

The views are read-only and duplicate no data. Their source tables are `orders` and `order_items`.

## Import workflow

The POS integration should select unprocessed headers and their details in one SQLite read transaction:

```sql
SELECT *
FROM order_header
WHERE import_status = 'PENDING'
  AND cancelled_at IS NULL
ORDER BY order_date, order_id;

SELECT *
FROM order_detail
WHERE order_id = ?
ORDER BY line_number;
```

After the POS has committed the order successfully, update the source header row:

```sql
UPDATE orders
SET import_status = 'IMPORTED',
    imported_at = datetime('now'),
    pos_order_number = ?,
    import_error = NULL
WHERE shopify_order_id = ?;
```

On a failed import, leave it available for retry and record the reason:

```sql
UPDATE orders
SET import_status = 'ERROR', import_error = ?
WHERE shopify_order_id = ?;
```

The connector preserves these POS-managed import fields when Shopify sends later order updates. It
also preserves `print_status` and `printed_at`. New orders default both import and print status to
`PENDING`.

SQLite WAL mode is enabled so the connector can write while the POS reads. The POS integration should
use a SQLite driver, short transactions, parameterized queries, and the stable `order_id` key. It
must not treat `invoice_no` as globally unique.

## Privacy and retention

The database intentionally excludes card numbers, CVV, payment credentials, authorization data,
billing transactions, and raw Shopify webhook bodies. Customer contact/address fields require the
appropriate Shopify protected-customer-data approval; Shopify can omit them when access is unavailable.

The local inbox retains the newest `ORDER_DB_RETENTION_ROWS` orders (10,000 by default). Related details
are removed automatically through the foreign key when an old header is pruned.
