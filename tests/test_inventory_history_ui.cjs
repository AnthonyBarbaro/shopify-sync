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
