0  # Shopify Order DBF Developer Handoff

This document can be sent directly to the developer implementing the POS order import.

## Project status and requested work

The Windows connector side is complete. It creates and maintains a genuine dBASE III Shopify order
inbox, but it deliberately does **not** insert records directly into the POS-native `Ordhdr.dbf` or
`Orddtl.dbf` tables.

The developer is being asked to build the final POS import step that:

1. reads the Shopify header/detail DBFs using the lock and snapshot protocol below;
2. selects unimported, non-cancelled orders with no `TRUNCATED` flag;
3. creates the matching native POS order and detail records through a vendor-approved method;
4. prevents duplicate native orders by using the stable Shopify `ORDER_ID` as an idempotency key;
5. updates only the six documented POS-managed status fields after the native transaction finishes;
6. records import errors without deleting the Shopify inbox order.

Before production deployment, test the importer against a copied POS data folder. Do not test by
writing directly to the live `Ordhdr.dbf` or `Orddtl.dbf` files.

## Required acceptance tests

The POS import is ready when all of these pass:

- one pending Shopify order creates exactly one native POS order with every current detail line;
- replaying the same `ORDER_ID` does not create a duplicate native order;
- totals, shipping, tax, discounts, quantities, customer data, and addresses map correctly;
- a cancelled order is skipped or handled through an explicitly approved cancellation workflow;
- a header or detail with `TRUNCATED=true` is routed to error/review and is not auto-imported;
- a failed native transaction leaves the order retryable and writes `IMPORT_ST=ERROR` plus
  `IMP_ERROR`;
- a successful native transaction writes `IMPORT_ST=IMPORTED`, `IMPORTEDAT`, and `POS_ORD_NO`;
- later Shopify updates and connector retries do not reset the six POS-managed fields;
- the importer holds the byte-range lock through reading, duplicate lookup, native commit, and DBF
  status update;
- stopping and restarting the importer between native commit and status update still cannot create a
  duplicate because the native system can find the previous import by `ORDER_ID`.

When `POS_DBF_DIR=C:\ashpsdat`, the Windows connector creates the separate folder and two genuine
dBASE III files by default if they do not already exist:

- `C:\ashpsdat_web\shopify-order-header.dbf`
- `C:\ashpsdat_web\shopify-order-detail.dbf`

They are a separate Shopify order inbox. The connector does not modify or copy the native
`Ordhdr.dbf`, `Orddtl.dbf`, `Customer.dbf`, or `CustShip.dbf` tables. A separate, tested POS import
is still required before these orders appear in the native POS Orders screen.

## File format and pairing

Both files use dBASE III version `0x03`, Windows CP1252 text, and only character (`C`), numeric (`N`),
and logical (`L`) columns. They require no `.FPT`, `.DBT`, or `.CDX` sidecars. Field names are no more
than ten characters.

The connector also creates `shopify-order-header.lock` beside the default DBFs. For a custom header
path, the lock name is that path with its suffix replaced by `.lock`; for example,
`D:\inbox\orders.dbf` uses `D:\inbox\orders.lock`. Before reading the pair or updating a POS-managed
status, an integration must open that file and take an exclusive byte-range lock on byte 0. Hold the
lock through the complete read/check/import/status-write operation, then release it. On Windows, a
.NET integration can use `FileStream.Lock(0, 1)` and `Unlock(0, 1)`. The connector uses the same lock.
If the lock cannot be acquired, retry rather than accessing the DBFs.

Every active row has the same `GEN_ID` for one published snapshot. Each header also has `LINE_COUNT`.
The connector writes the detail file first and the header file last, validates both, and only then
acknowledges Railway. A reader must:

1. read the desired header and remember its `GEN_ID` and `LINE_COUNT`;
2. read details with the same `ORDER_ID` and `GEN_ID`;
3. reread the header and verify that its `GEN_ID` is unchanged and the detail count equals
   `LINE_COUNT`;
4. retry if the generation changed or the counts do not match.

This check is required because two independent filesystem files cannot be replaced in one atomic
operation. The connector keeps temporary recovery copies while publishing so an interrupted write
can restore the previous matched pair on its next run.

Text that cannot be represented in CP1252 is written with replacement characters. Values wider than
their fixed DBF field are shortened. `TRUNCATED` is true when either happens. Shopify notes use
`NOTE1` followed by `NOTE2`, for a combined maximum of 508 bytes. Money fields are normalized to two
decimal places to fit the DBF numeric columns; `TRUNCATED` is also true if that normalization rounds
an amount.

## Header fields

| Field | Type | Meaning |
| --- | --- | --- |
| `GEN_ID` | `C(32)` | Snapshot generation shared by both files |
| `ORDER_ID` | `C(24)` | Stable Shopify order ID; primary relationship key |
| `INVOICE_NO` | `C(32)` | Shopify order name, such as `#1001` |
| `ORDER_NUM` | `C(24)` | Shopify order number |
| `CONFIRM_NO` | `C(64)` | Shopify confirmation number |
| `ORDER_AT` | `C(35)` | Order timestamp in ISO 8601 text |
| `UPDATED_AT` | `C(35)` | Shopify update timestamp |
| `PROC_AT` | `C(35)` | Processed timestamp |
| `CANCEL_AT` | `C(35)` | Cancellation timestamp; blank when active |
| `CLOSED_AT` | `C(35)` | Closure timestamp |
| `FIN_STATUS` | `C(24)` | Shopify financial status |
| `FUL_STATUS` | `C(24)` | Shopify fulfillment status |
| `CURRENCY` | `C(3)` | Currency code |
| `CUST_NAME` | `C(80)` | Customer display name |
| `CUST_FIRST` | `C(40)` | Customer first name |
| `CUST_LAST` | `C(40)` | Customer last name |
| `EMAIL` | `C(254)` | Customer email |
| `PHONE` | `C(30)` | Customer phone |
| `BILL_NAME` | `C(80)` | Billing name |
| `BILL_FIRST` | `C(40)` | Billing first name |
| `BILL_LAST` | `C(40)` | Billing last name |
| `BILL_COMP` | `C(80)` | Billing company |
| `BILL_ADR1` | `C(100)` | Billing address line 1 |
| `BILL_ADR2` | `C(100)` | Billing address line 2 |
| `BILL_CITY` | `C(50)` | Billing city |
| `BILL_PROV` | `C(40)` | Billing province/state name |
| `BILL_PCODE` | `C(10)` | Billing province/state code |
| `BILL_CTRY` | `C(40)` | Billing country name |
| `BILL_CCODE` | `C(2)` | Billing country code |
| `BILL_ZIP` | `C(20)` | Billing postal code |
| `BILL_PHONE` | `C(30)` | Billing phone |
| `SHIP_NAME` | `C(80)` | Shipping name |
| `SHIP_FIRST` | `C(40)` | Shipping first name |
| `SHIP_LAST` | `C(40)` | Shipping last name |
| `SHIP_COMP` | `C(80)` | Shipping company |
| `SHIP_ADR1` | `C(100)` | Shipping address line 1 |
| `SHIP_ADR2` | `C(100)` | Shipping address line 2 |
| `SHIP_CITY` | `C(50)` | Shipping city |
| `SHIP_PROV` | `C(40)` | Shipping province/state name |
| `SHIP_PCODE` | `C(10)` | Shipping province/state code |
| `SHIP_CTRY` | `C(40)` | Shipping country name |
| `SHIP_CCODE` | `C(2)` | Shipping country code |
| `SHIP_ZIP` | `C(20)` | Shipping postal code |
| `SHIP_PHONE` | `C(30)` | Shipping phone |
| `SHIP_METH` | `C(80)` | Shipping method titles joined with commas |
| `SUBTOTAL` | `N(18,2)` | Order subtotal |
| `DISCOUNT` | `N(18,2)` | Order discount |
| `SHIPPING` | `N(18,2)` | Shipping charge |
| `HANDLING` | `N(18,2)` | Always `0.00`; Shopify has no separate value |
| `TAX` | `N(18,2)` | Total tax |
| `TOTAL` | `N(18,2)` | Order total |
| `NOTE1` | `C(254)` | First part of the Shopify note |
| `NOTE2` | `C(254)` | Remaining Shopify note text |
| `TAGS` | `C(200)` | Shopify tags |
| `PRINT_ST` | `C(12)` | POS-managed print status; new value is `PENDING` |
| `PRINTED_AT` | `C(35)` | POS-managed ISO print timestamp |
| `IMPORT_ST` | `C(12)` | POS-managed import status; new value is `PENDING` |
| `IMPORTEDAT` | `C(35)` | POS-managed ISO import timestamp |
| `POS_ORD_NO` | `C(32)` | POS-managed native order number after import |
| `IMP_ERROR` | `C(200)` | POS-managed import error or blank |
| `SRC_EVENT` | `C(24)` | Shopify webhook topic |
| `SRC_VER` | `N(10,0)` | Railway order-change version |
| `SYNCED_AT` | `C(35)` | Connector write timestamp |
| `LINE_COUNT` | `N(6,0)` | Number of matching detail records |
| `TRUNCATED` | `L` | True if header text or numeric precision could not be preserved exactly |

## Detail fields

| Field | Type | Meaning |
| --- | --- | --- |
| `GEN_ID` | `C(32)` | Snapshot generation shared by both files |
| `ORDER_ID` | `C(24)` | Parent Shopify order ID |
| `INVOICE_NO` | `C(32)` | Shopify order name |
| `LINE_NO` | `N(6,0)` | One-based line position |
| `LINE_KEY` | `C(32)` | Stable line ID, or `line-N` fallback |
| `LINE_ID` | `C(24)` | Shopify line-item ID |
| `PRODUCT_ID` | `C(24)` | Shopify product ID |
| `VARIANT_ID` | `C(24)` | Shopify variant ID |
| `SKU` | `C(64)` | Item SKU |
| `QTY` | `N(10,0)` | Original quantity |
| `CURR_QTY` | `N(10,0)` | Current Shopify quantity |
| `UNIT_PRICE` | `N(18,2)` | Unit price |
| `DISCOUNT` | `N(18,2)` | Line discount |
| `TAX` | `N(18,2)` | Sum of line taxes |
| `EXTENSION` | `N(18,2)` | Unit price times quantity minus discount |
| `DESCRIPT` | `C(254)` | Product title |
| `VAR_TITLE` | `C(254)` | Variant title |
| `VENDOR` | `C(100)` | Vendor |
| `GRAMS` | `N(12,0)` | Item weight in grams |
| `REQ_SHIP` | `L` | Whether the item requires shipping |
| `FUL_STATUS` | `C(24)` | Line fulfillment status |
| `SRC_VER` | `N(10,0)` | Matching header source version |
| `TRUNCATED` | `L` | True if line text, tax data, or numeric precision required coercion |

## Import workflow

Select headers where `IMPORT_ST` is `PENDING` and `CANCEL_AT` is blank. Join details by the stable
`ORDER_ID`, never by `INVOICE_NO`, and perform the generation/count checks above. After the native POS
order commits successfully, update only these header fields:

- `IMPORT_ST=IMPORTED`
- `IMPORTEDAT=<ISO timestamp>`
- `POS_ORD_NO=<native POS order number>`
- `IMP_ERROR=<blank>`

Do not auto-import a header or any of its details when `TRUNCATED` is true. Route it for review and
record an `IMPORT_ST=ERROR` reason; that flag means fixed-width text, monetary precision, or tax data
could not be represented exactly.

On failure, use `IMPORT_ST=ERROR` and put the reason in `IMP_ERROR`. A printing integration may set
`PRINT_ST=PRINTED` and `PRINTED_AT`. When the integration follows the lock protocol, the connector
preserves all six POS-managed fields when Shopify sends an update or Railway retries an
acknowledgement.

The native POS import must also be idempotent by `ORDER_ID`. Before creating a native order, look up a
previous import of that Shopify ID and reuse it. A process can stop after the native POS commit but
before `IMPORT_ST=IMPORTED` is written; the DBF lock cannot make those two separate systems one atomic
transaction.

Keep DBF reads and status writes short. Recheck `GEN_ID` before changing a status so a write does not
race a connector snapshot replacement. Do not change `GEN_ID`, `LINE_COUNT`, source fields, or order
data in place.

## Updates, deletion, migration, and retention

Shopify updates replace the connector-managed header values and the complete current detail set.
Cancelled orders remain present with `CANCEL_AT`; importers should skip or separately handle them.
Shopify deletion and customer-redaction events physically remove both header and detail records so
the removed customer data is not left in deleted DBF slots.

On upgrade, an existing SQLite file configured through `SHOPIFY_ORDER_DB_PATH`, or found under the
historical default names, is copied once into the DBF pair before new changes are acknowledged. Its
print/import fields are preserved and the original SQLite file remains available for manual recovery.
Later Shopify deletion and customer-redaction events remove the matching rows from that recovery copy
before Railway is acknowledged, so it does not retain data that was removed from the DBFs.

The local inbox normally holds at most `ORDER_DB_RETENTION_ROWS` headers (250 by default, 100 minimum,
500 maximum) and only their matching details. A previously published, unimported order is never evicted;
available slots admit the oldest queued unimported orders first, then retain the newest imported orders.
When every slot is occupied by an unimported order, newer changes remain unacknowledged on Railway until
the POS imports an order and frees a slot. Lowering the configured limit below the current unimported
count temporarily keeps those existing rows until they drain. Card numbers, CVV values, payment
credentials, authorization data, billing transactions, and raw webhook bodies are never stored.
