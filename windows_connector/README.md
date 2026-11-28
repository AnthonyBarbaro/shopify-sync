# Windows POS Inventory Connector

This connector runs continuously on the Windows POS computer. It checks the live POS event tables
every 30 seconds by default but sends no DBF files or ZIP archives to Railway. A separate heartbeat
also runs every 30 seconds, including during long inventory jobs. The dashboard refreshes every
30 seconds and shows inventory, order, and customer check-ins independently. Order delivery runs in
its own worker, so it can keep polling during a long inventory job. Each check-in records a request
reaching the server. The Customers tab shows recent import outcomes by POS customer number.

## Sync behavior

1. On its first successful run, it uploads product and SKU details. New in-stock Shopify products
   default to draft, while zero-quantity products are archived. All in-stock products are sent
   before zero-stock products, preserving their original POS order within each group.
2. It records a small local baseline for every SKU and matrix variant.
3. Initial descriptions are empty and the generated product name is included as a tag. Later runs
   send inventory deltas and explicit price changes only, so product titles, tags, descriptions,
   images, and other merchandising edits are not repeatedly overwritten.
4. Every 30 seconds by default it reads only records newly appended to `invdtl.dbf` and `editvoid.dbf`.
   Those rows identify affected base SKUs; the connector then rereads their authoritative current
   quantities from the product tables and sends stock adjustments before checking prices.
   It rechecks the affected SKUs on the following cycle in case a receipt appeared before the
   register finished writing stock. It uses the live event tables `invdtl.dbf` and `editvoid.dbf`.
   It checks the numeric SKU sequence between inventory events when `Item.dbf` changes or a
   product check is pending.
   A SKU above the saved five-digit
   high-water mark is held until its entire catalog payload is identical on two consecutive scans,
   then uploaded once as a new product. If the clerk is still editing a copied product, any changed
   field resets that stability check.
   It compares `pricechg.dbf` with a small local snapshot when the file changes or a price needs to
   retry. The first updated
   run establishes a baseline without replaying historical price changes; later changes update only
   the exact Shopify variant price. A base matrix SKU updates all of its matrix variants.
5. A changed `Item.dbf` or `Itemmqty.dbf` triggers a local quantity check for known SKUs. The
   connector prefers a quiet cycle and attempts the check on the next poll after five minutes of
   waiting even if the register stays busy. This catches manual adjustments and receiving activity
   that bypass the event tables. The check uses cached Shopify quantities and any queued changes.
   If either source file changes during the read, the connector reads the event SKUs separately and
   retries the wider check later.
6. The nightly full reconciliation compares POS quantities with Shopify's inventory snapshot at the
   pinned location. It becomes due at local midnight by default and normally waits for the same
   quiet window. It can start after 30 minutes of waiting during sustained store activity.
   A missed pass is picked up after the connector starts. Initial catalog setup, the first scan
   needed to verify legacy matrix mappings, and **Force sync** can start without the quiet window.
   A sale recorded during a running full scan is picked up when the connector returns to event checks.
   This full scan also catches new alphanumeric or out-of-sequence SKUs that the numeric fast path
   cannot identify, and repairs older SKUs that are missing a local baseline.
   It also detects a legacy copied matrix product that was uploaded too early as one zero-stock base
   variant. The connector waits for the complete POS matrix payload to be identical on two full scans,
   then asks the backend to replace only that verified default variant with the POS variants. The
   backend preserves all product-level merchandising and refuses nonzero inventory in any state at any Shopify
   location, partial, duplicate, unmanaged, or otherwise ambiguous structures. During the
   `Item.dbf` catalog check, a known `M`
   product with no cached child mapping schedules the same guarded probe immediately; if its matrix
   is still incomplete, another probe is allowed after a 15-minute cooldown. A verified repair uses
   Shopify's confirmed mutation quantities as the new baseline, so a stale post-repair snapshot
   cannot write zero back to either system.
7. Shopify sends inventory-level webhooks to Railway. Every cycle, the connector checks for those
   changed quantities. During a full reconciliation, connector 1.4.8 reads the location-specific
   Shopify snapshot in separate requests and continues by inventory item ID. A webhook with the same
   timestamp and quantity as an observation already used
   by the connector is skipped. Different quantities with the same timestamp are checked against a
   fresh Shopify snapshot. Changes for tracked SKUs stay queued if the live POS row cannot be read.
8. Independent POS and online-sale deltas are combined so simultaneous sales on both channels are
   preserved.
9. Shopify adjustments use idempotency keys and return the actual resulting quantity when available. If that result
   differs from the planned target because another Shopify change happened concurrently, the paired
   POS write is deferred and recalculated on the next cycle. POS writes use compare-before-update
   checks so a sale at the register cannot be silently overwritten.

The catalog import archives zero-quantity products and marks that transition as connector-owned.
After the nightly quantity reconciliation, a separate daily status pass also archives known
products whose variants all have nonpositive stock. It includes products already at zero. Pending
or blocked inventory changes prevent archiving the affected product, and the backend freshly checks
all variants and Shopify locations before changing status. Products with positive stock, untracked
inventory, missing data, or more than the checked 50 variants or five locations per variant are
skipped. A failed status pass retries on a later cycle; a completed pass does not run again that day.
The connector saves pending status SKUs before sending each request and removes them only after an
explicit successful result. Failed, missing, or timed-out results survive a restart and are checked
again after inventory adjustments complete, including when a later snapshot is positive. The
backend can then restore a connector-owned archive to Draft if the restock webhook was missed or
finished before archiving. Incomplete stock verification for that recovery remains retryable.
The normal inventory cycles do not newly archive products.

A later positive stock observation restores only a marked product to Draft and clears the marker;
manual, unmarked archives are preserved.
An automatically archived product created before this marker existed needs a one-time manual change
to Draft after the upgraded reconciliation repairs its quantity.

All snapshot reads, webhooks, and adjustments use the same Shopify inventory location. Set the
Railway `SHOPIFY_LOCATION_ID` value for a multi-location store. Without it, Shopify's primary
location is pinned; a later location change stops reconciliation for review instead of mixing stock
from two locations. Duplicate Shopify SKUs and inventory items unavailable at that location are also
blocked from unattended writes.

## Storage

The connector's recurring Windows storage is limited to:

- `state.json` and one backup;
- `customers.json` and one backup containing only customer identity/payload hashes and a scan cursor;
- `shopify-order-header.dbf` and `shopify-order-detail.dbf` when order sync is enabled;
- `shopify-order-header.lock`, used to coordinate DBF reads and status writes;
- a 5 MB rotating log with three backups by default;
- the small Python virtual environment created by the installer.

After an upgrade, a legacy `shopify-order.db` or `shopify-orders.db` may also remain as a recovery copy
after its rows are migrated into the DBFs. Later Shopify deletion and customer-redaction events are
also removed from that copy before acknowledgement.

Railway receives JSON inventory/price adjustments and enabled customer uploads, never sales/edit history or DBF archives.
Its inventory-change queue keeps only the latest unprocessed value and deletes it after the Windows
connector acknowledges it. The dashboard keeps only the 50 most recent lightweight price-change
results. The server separately caps its feed and request history using
`FEED_EVENT_RETENTION_ROWS` and `REQUEST_LOG_RETENTION_ROWS`.

## Customer sync

Customer sync is append-only from the POS to Shopify. A separate worker checks the `Customer.dbf`
and `CustShip.dbf` file metadata every 60 seconds. It reads the records after a file change, when
work remains, at the first check of a new day, or when a scan is requested. The first import
processes up to 50 customers per pass, in requests of at most ten. It then submits only records
whose POS payload has changed. The backend checks POS ID, email, and phone before creating a missing
customer. A matched Shopify customer stays unchanged, including names, contacts, addresses, notes,
tags, consent, and metafields. Creation uses `customerCreate` with the stable POS identity in the
same request; retries and creation conflicts recheck for an existing customer and never switch to
an update. Both new customers and preserved existing matches count as successfully processed.
Each confirmed success saves an identity/payload hash; failures,
missing results, and timeouts are retried using the same stable POS ID. Customers without a POS
customer number are skipped. Removing a POS customer does not delete their Shopify record.

Enable the customer permissions on Railway and in Shopify before upgrading Windows:

1. Add `read_customers,write_customers` to the existing Railway `APP_SCOPES` list.
2. Repeat the Shopify install flow to grant those scopes. The app also needs the appropriate
   [protected customer data access](https://shopify.dev/docs/api/admin-graphql/2026-01/mutations/customerCreate).
3. Keep these Windows `connector.env` settings (also the defaults when absent):

   ```env
   CUSTOMER_SYNC_ENABLED=true
   CUSTOMER_SYNC_INTERVAL_SECONDS=60
   ```

`--dry-run` reads customers without sending them or advancing their hashes. Inventory jobs and
customer uploads use separate sessions and state files. Customer upload failures do not stop
inventory. The Customers tab shows check-ins and recent import results. The Windows log has
aggregate results for each batch.
For an address rejection, **View address details** shows the submitted mailing address fields and
Shopify's validation error when available. Details are accessible in the signed-in shop dashboard
for 24 hours after the last rejection or partial import. Older failures can show details after the
connector retries them. If Shopify specifically rejects a name, email, phone, or address field, a
new customer with a stable POS ID can be created without that field. An explicitly rejected address
is omitted if its fields cannot be accepted safely. The Customers tab records **Added with skipped
fields**. Update the omitted fields in Shopify after reviewing them. Routine sync preserves records
that already exist in Shopify.
Recurring uploads omit contact payloads from request/feed logs. Contact details are not stored in
`customers.json`.

## Sales Board

Open **POS Bridge → Settings → Sales Board** in Shopify Admin to enable or pause sales reporting,
set the POS check interval, or enter a monthly goal in USD. The Windows connector reads these
settings from the app. The **Sales Board** tab shows in-store sales and has a tablet view.

The reporting worker reads completed POS invoice headers and detail lines, the employee roster,
and item and department codes. It prefers the register's `depart.dbf` and also supports `MDEPT.DBF`
or `Dept.dbf`. It uses signed POS line extensions in cents, before tax, for net sales.
It excludes alteration lines by default, including on mixed tickets. Use **Include alteration sales**
to include alteration receipts, returns, and line items in the current view. Gift-card issues,
payment-only lines, open orders, and unclassified lines are excluded. Net units reflect returned quantities. Sales tickets count each
receipt with a positive merchandise line once per store or employee. A shared receipt can count for
more than one employee. The board covers the live POS invoice history available on the computer.
Gross profit subtracts the unit cost recorded on each sale line and reverses that cost on returns.
Blank, zero, and invalid unit costs are treated as missing. When costs are missing, the displayed
profit and margin cover the lines with known costs, and the board labels them as partial.

Select a rep on the leaderboard to open their merchandise sales and returns for the selected period.
On shared tickets, each rep sees only their own lines. Reporting periods use Eastern Time, and
receipt times use the Eastern local clock recorded by the POS. Today's hourly chart covers 9 AM
through the 9 PM closing boundary. Daily and monthly totals include completed sales at any time.
The Year view includes a month-by-month profit and margin table.

The worker sends a current snapshot with totals and ticket breakdowns. Ticket data includes invoice
number, time, register, merchandise, quantities and amounts. It omits customer and payment fields
and is available on demand in the authenticated Shopify dashboard. Deploy the backend before
updating the Windows connector so it can save and serve receipt details.

**Reports** supports custom date ranges, year-to-date comparisons, sales trends, and ranked product,
brand/vendor, category and rep totals with CSV export. The connector reads the completed invoice
history present in the live POS files and syncs it by month, newest first. Existing month data is
replaced on a newer import, and changes to the active month are sent as they are detected.
Reports use signed line amounts and recorded historical unit costs. Brand/vendor names come from
POS vendor records. Reporting includes no customer, payment, or source invoice identifiers.
**Year over year** compares three calendar years through the same month and day, with Sunday-starting
weekly totals and a separate department comparison. Recorded tax is optional for the sales columns.
Profit continues to use pre-tax sales. The current POS totals show customers, positive balances due,
and signed cost and retail valuations for stocked merchandise records. The POS snapshot shows current
data alongside the historical sales comparisons.

## Shopify order inbox

When `ORDER_SYNC_ENABLED=true`, the connector pulls queued Shopify order create, update, cancel, and delete
events before each inventory cycle and writes genuine dBASE III files at
`C:\ashpsdat_web\shopify-order-header.dbf` and `C:\ashpsdat_web\shopify-order-detail.dbf` by default
when `POS_DBF_DIR=C:\ashpsdat`. The connector creates the separate folder and both files when they do
not exist. These isolated inbox files mirror the useful shape of the POS `Ordhdr.dbf` and `Orddtl.dbf`
files but never modify, copy, or insert into those native FoxPro tables.

- `shopify-order-header.dbf`: order/invoice identifiers, customer name, email, phone, billing address, shipping
  address, shipping method and charge, subtotal, discount, tax, total, fulfillment/financial state,
  printing state, and POS import state;
- `shopify-order-detail.dbf`: line number, SKU, quantity, unit price, line discount, line tax, extension, product
  description, variant description, vendor, and fulfillment state.

Every row carries a generation ID, and each header records its detail count. A reader should open both
files, verify that their generation IDs match, and retry if the connector was between file replacements.
The POS integration may update the documented print/import status fields in the header file after an
import attempt. It must hold the documented header `.lock` byte-range lock while reading or updating
the pair; with the default name this is `shopify-order-header.lock`. A custom header path uses the same
path with its suffix replaced by `.lock`. Webhook retries and later Shopify updates then preserve those
fields.

Existing installations that set `SHOPIFY_ORDER_DB_PATH` keep using that file only as a one-time legacy
SQLite migration source. On the first non-dry run, the connector copies its retained orders, details,
and print/import status into the two DBFs and leaves the SQLite file available for manual recovery.
Subsequent deletion/redaction events are removed from both formats. New installations should use
`SHOPIFY_ORDER_HEADER_DBF_PATH` and `SHOPIFY_ORDER_DETAIL_DBF_PATH` only when the default DBF locations
need to change. The former standard values inside `POS_DBF_DIR` are automatically redirected to the
new sibling `_web` folder, so an existing `connector.env` does not need to be edited for this move.

Card numbers, CVV values, payment credentials, authorization data, and raw webhook payloads are never
written to either file. Railway acknowledges queued changes only after both DBFs have been written and
validated. If the bounded inbox is full of unimported orders, newer changes stay unacknowledged on
Railway until the POS marks an order imported and frees capacity.

New orders have pending print and import statuses. The DBFs are an order inbox for the bridge and do
not appear in the native POS Orders tab without a separately tested POS import.

The POS developer handoff, field list, and import queries are in
[`SHOPIFY_ORDER_DB_SCHEMA.md`](SHOPIFY_ORDER_DB_SCHEMA.md).

## Install

1. Copy `connector.env.example` to `connector.env`.
2. Enter the local `ashpsdat` path, sync URL, POS key, and POS secret.
3. In PowerShell, test one read-only cycle:

   ```powershell
   py windows_connector\connector.py --config windows_connector\connector.env --once --dry-run
   ```

4. Open PowerShell as Administrator and install the startup task:

   ```powershell
   powershell -ExecutionPolicy Bypass -File windows_connector\install.ps1
   ```

The installer starts the task immediately by default. The task runs as Windows `SYSTEM`, starts one
minute after later boots, restarts after failures, and prevents
overlapping instances. Use a local drive or UNC path for `POS_DBF_DIR`; mapped drive letters are not
normally available to `SYSTEM`.
A recurring recovery trigger also starts a stopped connector within one minute while Windows is
awake. It ignores the trigger when the connector is already running, so only one instance runs.

## Inventory direction

The connector reads POS inventory and updates quantities in Shopify only. Older
`POS_WRITEBACK_MODE` and `POS_WRITEBACK_SCRIPT` settings are ignored. Shopify orders still reach
the POS order inbox, and staff process those sales in Cash Register Express.

## Operations

Deploy the Railway/backend release before updating the Windows connector. The upgraded connector
requires inventory snapshot schema version 2 and will stop safely, without acknowledging queued
inventory changes, if the older endpoint is still deployed.

To update the Windows runtime, run `windows_connector\update_connector.bat` as Administrator. The
updater downloads and validates both `connector.py` and `jbarbaro_db\dbf_pos_sync.py`, plus the small
supporting runtime files. It preserves `connector.env`, connector state, logs, Shopify order DBFs,
native POS DBFs, and any legacy Shopify order SQLite database.

For unattended updates, run the existing scheduled job with highest privileges. Install the updated
batch file and PowerShell script once to enable reporting when the updater download itself fails.
The Shopify app shows the latest 10 attempts under **Overview → Windows connector updates**.
Each failure shows its stage, error code, and affected filename when available. If the dashboard is unreachable,
the report queues under `updater-reports` in the connector data directory and uploads after the connector
reconnects. The scheduled job's output contains the full PowerShell error.

The Shopify app shows **Recent inventory issues** on the Overview. It records the affected SKU,
reason, observed quantities, and planned quantity when available. Full Shopify snapshot failures
appear in the same list. For more details, search `connector.log` for `inventory_quantity_issue`.
Connector 1.4.7 reports interrupted requests and timeouts as single-line warnings and retries on
its normal schedule.

The updater preserves existing configuration. Set `SYNC_INTERVAL_SECONDS=30` in `connector.env`
to replace the old 180-second interval, and restart the task after editing. The dashboard reports
the configured interval after the upgraded connector's first heartbeat. Faster polling does not
guarantee an update within 30 seconds: full reconciliations, POS file locks, API retries, and
connectivity can add delay. Recent quantity history appears after successful POS-to-Shopify
adjustments; it does not list idle checks or Shopify-originated stock changes.
Use the updated installer or updater to register the recovery trigger on an existing computer.
The updater temporarily disables the task while replacing files and preserves an explicitly
disabled task's state after updating.

With Connector 1.4.9 or later, **Force sync** sets Shopify's Available quantity for each matching
SKU at the connected location to its POS count. Process pending POS sales first so the count
reflects them. Matrix products are matched size by size using their exact variant SKUs. The
dashboard reports how many SKUs were updated, already matched, or skipped, and lists recent
inventory issues for skipped SKUs. If Shopify inventory changes between the read and the write,
the connector skips that SKU. Run Force Sync again to compare fresh counts if needed.

The next heartbeat starts the inventory copy and requests a customer scan. A running inventory
cycle finishes first. The Overview displays an animated bar while the copy runs and pauses it
while the Windows connector is offline or waiting to retry after a failed scan.

Start or pause the task (disable recovery before stopping for maintenance):

```powershell
Enable-ScheduledTask -TaskName "Shopify POS Inventory Connector"
Start-ScheduledTask -TaskName "Shopify POS Inventory Connector"
Disable-ScheduledTask -TaskName "Shopify POS Inventory Connector"
Stop-ScheduledTask -TaskName "Shopify POS Inventory Connector"
```

Remove startup registration without deleting state:

```powershell
powershell -ExecutionPolicy Bypass -File windows_connector\uninstall.ps1
```

Do not delete `state.json` after going live. It contains the per-channel quantity baselines that
prevent POS and Shopify changes from being counted twice.
