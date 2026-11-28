const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const test = require('node:test')

const context = vm.createContext({
  document: { addEventListener() {} },
  window: { addEventListener() {} },
})
vm.runInContext(fs.readFileSync(path.join(__dirname, '../app/static/app.js'), 'utf8'), context)

test('inventory history has a clear empty state', () => {
  const html = context.renderInventoryChangeList([])
  assert.match(html, /No quantity changes recorded yet/)
  assert.match(html, /Earlier changes are not included/)
})

test('inventory history preserves zero, unknown quantities, signed changes and escapes SKUs', () => {
  const html = context.renderInventoryChangeList([
    { sku: '<script>alert(1)</script>', delta: -2, quantity_after_change: 0 },
    { sku: 'positive', delta: 3, quantity_after_change: null },
  ])
  assert.match(html, /Resulting quantity: 0/)
  assert.match(html, /Resulting quantity unavailable/)
  assert.match(html, /-2 units/)
  assert.match(html, /\+3 units/)
  assert.match(html, /&lt;script&gt;/)
  assert.doesNotMatch(html, /<script>/)
})

test('overview renders recent inventory from the bridge response', () => {
  vm.runInContext(`state.bridge = { inventory: { recent: [
    { sku: 'matrix-123', delta: -1, quantity_after_change: 5 }
  ] } }`, context)
  const html = context.renderOverview()
  assert.match(html, /Recent quantity changes/)
  assert.match(html, /SKU matrix-123/)
  assert.match(html, /Resulting quantity: 5/)
})

test('overview separates connector heartbeat from inventory and shows actual intervals', () => {
  vm.runInContext(`state.bridge = {
    connector: { active: true, sync_interval_seconds: 90, last_seen_at: '2026-11-17T12:00:00Z' },
    inventory: { active: false, last_poll_at: '2026-11-16T12:00:00Z' },
    customers: { enabled: true, authorized: true, active: true, interval_seconds: 60 }
  }`, context)
  const html = context.renderOverview()
  assert.match(html, /Connected/)
  assert.match(html, /inventory polling is delayed/)
  assert.match(html, /POS checks every 90 seconds/)
  assert.match(html, /Customer connector is checking in/)
  assert.match(html, /Checks every 1 minute/)
  assert.match(html, /adds missing POS customers/)
  assert.match(html, /Existing Shopify customers stay unchanged/)
  assert.doesNotMatch(html, /Inventory bridge is active/)
})

test('overview reports missing customer authorization and missing connector check-ins', () => {
  vm.runInContext(`state.bridge = {
    connector: { active: false }, customers: { enabled: true, authorized: false }
  }`, context)
  const html = context.renderOverview()
  assert.match(html, /No check-in/)
  assert.match(html, /Customer access needs approval/)
  assert.match(html, /read_customers and write_customers/)
  assert.doesNotMatch(html, />Offline</)
})

test('inventory history shows product names, variants and SKU with escaped merchant text', () => {
  const html = context.renderInventoryChangeList([
    { sku: '123. 1 1', product_title: '<b>Linen shirt</b>', variant_title: 'Blue / Medium', delta: -1, quantity_after_change: 2 },
    { sku: 'simple', product_title: 'Belt', variant_title: 'Default Title', delta: 1, quantity_after_change: 0 },
  ])
  assert.match(html, /&lt;b&gt;Linen shirt&lt;\/b&gt;/)
  assert.match(html, /SKU 123. 1 1 · Blue \/ Medium/)
  assert.doesNotMatch(html, /<b>Linen shirt/)
  assert.doesNotMatch(html, /Default Title/)
})

test('force sync shows queued work honestly when the connector is unavailable', () => {
  vm.runInContext(`state.bridge = {
    connector: { active: false }, force_sync: { request_id: 'request1', completed_at: null }
  }`, context)
  const html = context.renderOverview()
  assert.match(html, /data-force-sync disabled/)
  assert.match(html, /Full inventory sync/)
  assert.match(html, /Queued until the Windows connector reconnects/)
  assert.doesNotMatch(html, /role="progressbar"/)
  vm.runInContext(`state.bridge.force_sync.completed_at = '2026-11-17T12:00:00Z'`, context)
  assert.match(context.renderOverview(), /Last forced inventory scan finished/)
  assert.match(context.renderOverview(), /Force sync\s*<\/button>/)
  assert.match(context.renderOverview(), /data-force-sync\s*>/)
})

test('force sync bar animates only during a reported stage', () => {
  const request = { request_id: 'request1', completed_at: null }
  assert.match(context.renderForceSyncStatus(request, true, true), /Waiting for the Windows connector to start/)

  const running = { ...request, progress_stage: 'reading_shopify' }
  const html = context.renderForceSyncStatus(running, true, true)
  assert.match(html, /force-sync-track is-running/)
  assert.match(html, /role="progressbar"/)
  assert.match(html, /Reading inventory from Shopify/)
  assert.doesNotMatch(html, /aria-valuenow/)
  assert.doesNotMatch(context.renderForceSyncStatus(running, true, false), /role="progressbar"/)

  const retry = context.renderForceSyncStatus({ ...request, progress_stage: 'retrying' }, true, true)
  assert.match(retry, /will retry the scan/)
  assert.doesNotMatch(retry, /role="progressbar"/)
})

test('overview shows the affected inventory SKU, the reason and the observed quantities', () => {
  vm.runInContext(`state.bridge = { inventory: { issues: [{
    sku: '22270. 1 4', reason: 'pos_writeback_preview', pos_quantity: 2, shopify_quantity: 0, target_quantity: 0,
  }] } }`, context)
  const html = context.renderOverview()
  assert.match(html, /Recent inventory issues/)
  assert.match(html, /SKU 22270\. 1 4/)
  assert.match(html, /POS write-back is in dry-run mode/)
  assert.match(html, /POS 2 · Shopify 0 · Target 0/)
  const failedScan = context.renderInventoryIssueList([{
    reason: 'shopify_snapshot_unavailable', code: 'ReadTimeout',
  }])
  assert.match(failedScan, /Full inventory scan/)
  assert.match(failedScan, /could not read Shopify inventory · Request timed out/)
})

test('overview displays updater successes and failed stages without rendering filename markup', () => {
  vm.runInContext(`state.bridge = { connector_updates: [
    { status: 'failed', stage: 'validate_download', file: '<b>connector.py</b>', occurred_at: '2026-11-28T12:00:00Z' },
    { status: 'succeeded', stage: 'complete', files_updated: 10, occurred_at: '2026-11-27T20:00:00Z' }
  ] }`, context)
  const html = context.renderOverview()
  assert.match(html, /Windows connector updates/)
  assert.match(html, /Validate downloaded files · &lt;b&gt;connector.py&lt;\/b&gt;/)
  assert.match(html, /Failed/)
  assert.match(html, /10 runtime files updated/)
  assert.match(html, /Succeeded/)
  assert.doesNotMatch(html, /<b>connector.py/)
})
