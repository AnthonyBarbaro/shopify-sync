# Inventory Sync POS Bridge for Cash Register Express

The Cash Register Express connector runs continuously on the store's Windows computer. It checks the
live POS event tables every three minutes but sends no DBF files or ZIP archives to Railway.

## Sync behavior

1. On its first successful run, it uploads product and SKU details. New in-stock Shopify products
   default to draft, while zero-quantity products are archived. All in-stock products are sent
   before zero-stock products, preserving their original POS order within each group.
2. It records a small local baseline for every SKU and matrix variant.
3. Initial descriptions are empty and the generated product name is included as a tag. Later runs
   send inventory deltas and explicit price changes only, so product titles, tags, descriptions,
   images, and other merchandising edits are not repeatedly overwritten.
4. Every three minutes it reads only records newly appended to `invdtl.dbf` and `editvoid.dbf`.
   Those rows identify affected base SKUs; the connector then rereads their authoritative current
   quantities from the product tables. It deliberately does not read `invdtl1.dbf` or `meditvd.dbf`.
   It also checks the current numeric SKU sequence in `Item.dbf`. A SKU above the saved five-digit
   high-water mark is held until its entire catalog payload is identical on two consecutive scans,
   then uploaded once as a new product. If the clerk is still editing a copied product, any changed
   field resets that stability check.
   It also compares the latest rows in `pricechg.dbf` with a small local snapshot. The first updated
   run establishes a baseline without replaying historical price changes; later changes update only
   the exact Shopify variant price. A base matrix SKU updates all of its matrix variants.
5. At the first cycle at or after local midnight, it performs one full POS quantity reconciliation
   against Shopify's actual inventory snapshot at the pinned inventory location. The first run after
   an inventory or catalog-structure reconciliation upgrade also performs the pass immediately, even
   if today's nightly pass was already recorded.
   If the computer was off at midnight, the first later cycle that day performs the missed pass.
   This full scan also catches new alphanumeric or out-of-sequence SKUs that the numeric fast path
   cannot identify, and repairs older SKUs that are missing a local baseline.
   It also detects a legacy copied matrix product that was uploaded too early as one zero-stock base
   variant. The connector waits for the complete POS matrix payload to be identical on two full scans,
   then asks the backend to replace only that verified default variant with the POS variants. The
   backend preserves all product-level merchandising and refuses nonzero inventory in any state at any Shopify
   location, partial, duplicate, unmanaged, or otherwise ambiguous structures. During the existing
   every-cycle `Item.dbf` scan, a known `M`
   product with no cached child mapping schedules the same guarded probe immediately; if its matrix
   is still incomplete, another probe is allowed after a 15-minute cooldown. A verified repair uses
   Shopify's confirmed mutation quantities as the new baseline, so a stale post-repair snapshot
   cannot write zero back to either system.
6. Shopify sends inventory-level webhooks to Railway. Every cycle, the connector consumes only those
   changed quantities. The full location-specific Shopify inventory snapshot is read only during the
   upgrade/nightly reconciliation.
7. Independent POS and online-sale deltas are combined so simultaneous sales on both channels are
   preserved.
8. Shopify adjustments use idempotency keys and return the actual resulting quantity. If that result
   differs from the planned target because another Shopify change happened concurrently, the paired
   POS write is deferred and recalculated on the next cycle. POS writes use compare-before-update
   checks so a sale at the register cannot be silently overwritten.

The catalog import archives zero-quantity products and marks that transition as connector-owned. A
later positive stock observation restores only a marked product to Draft and clears the marker;
manual, unmarked archives are preserved. Recurring inventory cycles do not newly archive products.
An automatically archived product created before this marker existed needs a one-time manual change
to Draft after the upgraded reconciliation repairs its quantity.

All snapshot reads, webhooks, and adjustments use the same inventory location for each shop. A
legacy Railway `SHOPIFY_LOCATION_ID` value is migrated onto the sole existing shop. Additional shops
use their own Shopify primary location. A later location change stops reconciliation for review
instead of mixing stock from two locations. Duplicate Shopify SKUs and inventory items unavailable
at that location are also blocked from unattended writes.

## Storage

The connector's recurring Windows storage is limited to:

- `state.json` and one backup;
- `shopify-order-header.dbf` and `shopify-order-detail.dbf` when order sync is enabled;
- `shopify-order-header.lock`, used to coordinate DBF reads and status writes;
- a 5 MB rotating log with three backups by default;
- the small Python virtual environment created by the installer.

After an upgrade, a legacy `shopify-order.db` or `shopify-orders.db` may also remain as a recovery copy
after its rows are migrated into the DBFs. Later Shopify deletion and customer-redaction events are
also removed from that copy before acknowledgement.

Railway receives only small JSON inventory or price adjustments, never sales/edit history or DBF archives.
Its inventory-change queue keeps only the latest unprocessed value and deletes it after the Windows
connector acknowledges it. The dashboard keeps only the 50 most recent lightweight price-change
results. The server separately caps its feed and request history using
`FEED_EVENT_RETENTION_ROWS` and `REQUEST_LOG_RETENTION_ROWS`.

## Shopify order inbox

When `ORDER_SYNC_ENABLED=true` and the Shopify app has `read_orders` access, the connector pulls
queued Shopify order create, update, cancel, and delete events before each inventory cycle and writes
genuine dBASE III files at `C:\ashpsdat_web\shopify-order-header.dbf` and
`C:\ashpsdat_web\shopify-order-detail.dbf` by default when `POS_DBF_DIR=C:\ashpsdat`. The connector
creates the separate folder and both files when they do not exist. These isolated inbox files mirror
the useful shape of the POS `Ordhdr.dbf` and `Orddtl.dbf` files but never modify, copy, or insert into
those native FoxPro tables.

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

The normal installation is one file:

1. Open the Inventory Sync app's **Settings** page inside Shopify.
2. Select **Download Windows installer**.
3. Move the downloaded BAT file to the Cash Register Express computer and open it within 15 minutes.
4. Approve the Windows Administrator prompt. If `ashpsdat` is not found uniquely at the root of a
   fixed local drive, select the live folder when asked.

The BAT contains a short-lived, one-use pairing code, not a permanent API secret. Downloading another
BAT immediately revokes every earlier unused pairing code, so use only the newest download. The BAT
verifies the setup script and runtime archive, installs into `C:\ProgramData\ShopifyPosConnector`,
creates an isolated Python environment, performs a fast credential and `Item.dbf` check, registers
the task as Windows `SYSTEM`, and starts it. If Python 3.10 or newer is missing, setup downloads the
official, digitally signed Python runtime. Internet access is required during installation.

Only one Cash Register Express connector can be active for a shop. Successfully installing a
replacement revokes the prior device and any legacy shared POS credentials. Existing inventory state
is bound to its Shopify shop and Windows installation so it cannot silently be reused for a different
store. New installs start with `POS_WRITEBACK_MODE=dry-run`; a verified replacement preserves the
existing write-back mode.

For development-only manual setup, copy `connector.env.example` to `connector.env`, fill every
required value, then run `install.ps1` from an Administrator PowerShell window. `--validate` performs
the same fast preflight without scanning the full catalog or recording an operational heartbeat.

The task runs as Windows `SYSTEM`, starts immediately after one-click setup and one minute after each
later boot, restarts after failures, and prevents overlapping instances. The one-click installer
requires a fixed local POS data drive; it rejects UNC and mapped-drive paths because those are not
normally available to `SYSTEM`.

## POS quantity write-back

`POS_WRITEBACK_MODE=dry-run` detects Shopify-to-POS changes but does not edit DBFs. Review the log and
test against a copied POS data directory first.

The live mode is:

```env
POS_WRITEBACK_MODE=vfp-oledb
```

It requires the Microsoft Visual FoxPro OLE DB provider (`VFPOLEDB.1`) on the Cash Register Express
computer. The writer uses the 32-bit Windows PowerShell host, FoxPro transactions, relative quantity
adjustments, and an expected-quantity condition. Matrix adjustments update both the exact `Itemmqty`
cell and the aggregate `Item` quantity.

Before enabling live write-back:

1. Make a POS backup.
2. Point the connector at a copied DBF folder.
3. Verify non-matrix and size/color/length matrix test SKUs.
4. Confirm quantities and indexes in Cash Register Express.
5. Then switch the production config to `vfp-oledb`.

Direct DBF write-back updates inventory quantities only. It does not create a POS sales invoice or
financial transaction for an online Shopify order.

## Operations

Deploy the Railway/backend release before updating Inventory Sync POS Bridge. To update or repair a
one-click installation, download a fresh installer from the Inventory Sync Settings page inside
Shopify and open it on the same computer. It preserves the shop-bound config and inventory state,
verifies the complete runtime, and recreates the scheduled task. The older raw-GitHub updater is
retained only for legacy installs that still live at `C:\ShopifySync`; do not use it for the
ProgramData installation.

Start or stop the task:

The scheduled task retains its legacy internal name for compatibility:

```powershell
Start-ScheduledTask -TaskName "Shopify POS Inventory Connector"
Stop-ScheduledTask -TaskName "Shopify POS Inventory Connector"
```

Remove startup registration without deleting state:

```powershell
powershell -ExecutionPolicy Bypass -File windows_connector\uninstall.ps1
```

Do not delete `state.json` after going live. It contains the per-channel quantity baselines that
prevent POS and Shopify changes from being counted twice.
