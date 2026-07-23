# Windows POS Inventory Connector

This connector runs continuously on the Windows POS computer. It checks the live POS event tables
every three minutes but sends no DBF files or ZIP archives to Railway.

## Sync behavior

1. On its first successful run, it uploads product and SKU details. New in-stock Shopify products
   default to draft, while zero-quantity products are archived. All in-stock products are sent
   before zero-stock products, preserving their original POS order within each group.
2. It records a small local baseline for every SKU and matrix variant.
3. Initial descriptions are empty and the generated product name is included as a tag. Later runs
   send inventory deltas only, so product titles, prices, tags, descriptions, and images are not
   repeatedly overwritten.
4. Every three minutes it reads only records newly appended to `invdtl.dbf` and `editvoid.dbf`.
   Those rows identify affected base SKUs; the connector then rereads their authoritative current
   quantities from the product tables. It deliberately does not read `invdtl1.dbf` or `meditvd.dbf`.
5. At the first cycle at or after local midnight, it performs one full POS quantity reconciliation.
   If the computer was off at midnight, the first later cycle that day performs the missed pass.
6. Shopify sends inventory-level webhooks to Railway. Every cycle, the connector consumes only those
   changed quantities; it does not scan the entire Shopify catalog.
7. Independent POS and online-sale deltas are combined so simultaneous sales on both channels are
   preserved.
8. Shopify adjustments use idempotency keys. POS writes use compare-before-update checks so a sale at
   the register cannot be silently overwritten.

The connector intentionally does not archive Shopify products during unattended runs.

## Storage

Windows keeps only:

- `state.json` and one backup;
- a 5 MB rotating log with three backups by default;
- the small Python virtual environment created by the installer.

Railway receives only small JSON inventory adjustments, never sales/edit history or DBF archives.
Its inventory-change queue keeps only the latest unprocessed value and deletes it after the Windows
connector acknowledges it. The server separately caps its feed and request history using
`FEED_EVENT_RETENTION_ROWS` and `REQUEST_LOG_RETENTION_ROWS`.

## Shopify order inbox

When `ORDER_SYNC_ENABLED=true`, the connector pulls queued Shopify order create, update, cancel, and delete
events before each inventory cycle and writes them transactionally to
`C:\ashpsdat\shopify-order.db` by default. This separate SQLite database contains an `orders` table
and an `order_items` table, and never modifies the POS FoxPro order tables. It stores fulfillment
details needed for a picking ticket but excludes billing addresses, card data, payment credentials,
and raw webhook payloads. Railway deletes a queued change only after the local transaction succeeds.

New orders have `print_status=PENDING`; a printing integration can mark them `PRINTED` and set
`printed_at`. The file is an order inbox for the bridge and does not appear in the native POS Orders
tab without a separately tested POS import.

## Install

1. Copy `connector.env.example` to `connector.env`.
2. Enter the local `ashpsdat` path, sync URL, POS key, and POS secret.
3. Keep `POS_WRITEBACK_MODE=dry-run` initially.
4. In PowerShell, test one read-only cycle:

   ```powershell
   py windows_connector\connector.py --config windows_connector\connector.env --once --dry-run
   ```

5. Open PowerShell as Administrator and install the startup task:

   ```powershell
   powershell -ExecutionPolicy Bypass -File windows_connector\install.ps1
   ```

The task runs as Windows `SYSTEM`, starts one minute after boot, restarts after failures, and prevents
overlapping instances. Use a local drive or UNC path for `POS_DBF_DIR`; mapped drive letters are not
normally available to `SYSTEM`.

## POS quantity write-back

`POS_WRITEBACK_MODE=dry-run` detects Shopify-to-POS changes but does not edit DBFs. Review the log and
test against a copied POS data directory first.

The live mode is:

```env
POS_WRITEBACK_MODE=vfp-oledb
```

It requires the Microsoft Visual FoxPro OLE DB provider (`VFPOLEDB.1`) on the POS computer. The writer
uses the 32-bit Windows PowerShell host, FoxPro transactions, relative quantity adjustments, and an
expected-quantity condition. Matrix adjustments update both the exact `Itemmqty` cell and the
aggregate `Item` quantity.

Before enabling live write-back:

1. Make a POS backup.
2. Point the connector at a copied DBF folder.
3. Verify non-matrix and size/color matrix test SKUs.
4. Confirm quantities and indexes in Cash Register Express.
5. Then switch the production config to `vfp-oledb`.

Direct DBF write-back updates inventory quantities only. It does not create a POS sales invoice or
financial transaction for an online Shopify order.

## Operations

Start or stop the task:

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
