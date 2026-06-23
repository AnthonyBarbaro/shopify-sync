# DBF Path Sync Workflow

This project can now sync from local DBF files on the host computer without needing the POS to push directly.

## What To Read

Primary files:

- `Item.dbf`: main product table. This is the file to upload from.
- `Vendor.dbf`: vendor lookup table so vendor codes become readable vendor names.

Optional files:

- `Itemmqty.dbf`: per-cell quantity table if you want a location-specific quantity instead of `Item.dbf` total quantity.
- `pricechg.dbf`: useful later if you want a changed-SKU-only workflow.
- `vendors.dbf`: contains cost and vendor SKU data, but the current Shopify sync API does not use cost fields.
- `price.dbf`: present in the sample set but empty.

For a large export folder, the script looks for those filenames and ignores unrelated DBF files. Add `--recursive` if those files are inside nested folders.

## Current Field Mapping

The host-side script maps DBF rows like this:

- `SKU` -> `sku`
- `DESC` -> smarter `title` by default
- `DESC2` -> optional `description` when `--include-desc2-description` is enabled
- `PRICE` -> `price`
- `PRICE_R` and other higher price tiers -> optional `compare_at_price` when the value looks sane
- `QTY` and `Itemmqty.dbf` -> best available quantity by default
- `ALT_SKU` -> `barcode`
- `GROUP` -> normalized `product_type`
- `VENDOR` -> vendor code lookup in `Vendor.dbf`
- `SIZE` and `COLOR` -> title suffixes when they look storefront-friendly
- `STYLE`, `SIZE`, `COLOR`, `GROUP`, `VENDOR`, `VEND_ID`, `PFIELD1`-`PFIELD5` -> optional Shopify tags with `--include-tags`
- `IMAGE`, `IMAGE2`, `IMAGE3`, `IMAGE4` -> product images only when the value is already an `http://` or `https://` URL

Notes:

- By default the script does not send `status`, which is the safest repeated-sync behavior.
- New products still create as draft because the API defaults new products to draft when status is omitted.
- Existing products update by SKU.
- Quantity now defaults to `--quantity-source best`, which uses the larger available value from `Item.dbf` and `Itemmqty.dbf`.
- Zero-quantity rows are skipped by default.
- Internal or non-sellable POS rows are skipped by default, including common alterations, rentals, gift cards, shipping, and custom-order records.
- The script sends `quantity`, `qty`, and `stock_quantity` together for better compatibility with different API versions.
- The script defaults to `--name-mode smart`, which uses vendor, category, color, and size to build cleaner product names.
- Tags are off by default because sending tags on updates can replace existing Shopify tags.
- Rich metadata is off by default. Use `--rich` or `--include-metafields` when you want POS fields preserved in Shopify product metafields under the `pos` namespace.
- Metafields are written with Shopify's `metafieldsSet` API after the product is created or found, so unrelated Shopify metafields are not replaced.
- `Vendor.dbf` is used only as a code-to-company lookup. The script does not upload the full vendor address, account, login, or password fields.

## Script

The script lives at `jbarbaro_db/dbf_pos_sync.py`.

Dry run against a DBF folder:

```bash
python3 jbarbaro_db/dbf_pos_sync.py \
  --dbf-dir ./jbarbaro_db \
  --dry-run \
  --output-title-audit ./jbarbaro_db/title_audit_preview.csv \
  --output-json ./tmp/jbarbaro_payload.json \
  --pretty
```

Upload in batches:

```bash
python3 jbarbaro_db/dbf_pos_sync.py \
  --dbf-dir "C:\\POS\\EXPORT" \
  --base-url "https://your-sync-app.example.com" \
  --api-key "your_pos_key" \
  --api-secret "your_pos_secret"
```

You can also paste the full live endpoint with query-string auth, and the script will switch `/products` to `/products/batch` automatically:

```bash
python3 jbarbaro_db/dbf_pos_sync.py \
  --dbf-dir ./jbarbaro_db \
  --endpoint "https://sync.jbarbaro.com/wc-api/v3/products?consumer_key=...&consumer_secret=..."
```

You can also set `SHOPIFY_SYNC_BASE_URL`, `SHOPIFY_SYNC_API_KEY`, and `SHOPIFY_SYNC_API_SECRET` as environment variables for scheduled runs.

Use `Itemmqty.dbf` instead of `Item.dbf` quantity:

```bash
python3 jbarbaro_db/dbf_pos_sync.py \
  --dbf-dir ./jbarbaro_db \
  --base-url "https://your-sync-app.example.com" \
  --api-key "your_pos_key" \
  --api-secret "your_pos_secret" \
  --quantity-source itemmqty
```

Use one `Itemmqty` cell only:

```bash
python3 jbarbaro_db/dbf_pos_sync.py \
  --dbf-dir ./jbarbaro_db \
  --dry-run \
  --quantity-source itemmqty \
  --itemmqty-cell "1 1"
```

Helpful filters:

- `--limit 50`
- `--sku 10532 --sku 10533`
- `--recursive`
- `--skip-zero-price`
- `--include-zero-quantity`
- `--include-non-sellable`
- `--status draft`
- `--name-mode raw`
- `--rich`
- `--include-desc2-description`
- `--include-html-description`
- `--include-tags`
- `--include-metafields`
- `--metafield-namespace pos`

`DESC2` is not uploaded by default because, in the sample DBF set, it often looked like shorthand codes or internal notes instead of storefront description text.

Richer dry run:

```bash
python3 jbarbaro_db/dbf_pos_sync.py \
  --dbf-dir ./jbarbaro_db \
  --dry-run \
  --rich \
  --include-zero-quantity \
  --output-json ./tmp/jbarbaro_rich_payload.json \
  --pretty
```

Use `--include-zero-quantity` only when you want to create draft Shopify products for out-of-stock sellable items too. Leave it off for a smaller live sync that uploads only sellable inventory currently on hand.

## Host Computer Plan

The practical workaround is:

1. Install Python on the host computer.
2. Put this repo or just the script on that computer.
3. Point `--dbf-dir` at the POS export folder.
4. Run `--dry-run` first and inspect the JSON.
5. Schedule the upload command with Task Scheduler, cron, or another local scheduler.

This avoids changing the POS integration itself and uses the DBF files as the source of truth.
