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

test('daily bars keep every date, weekday, and bold amount', () => {
  const days = Array.from({ length: 22 }, (_, index) => ({
    label: `2026-11-${String(index + 1).padStart(2, '0')}`,
    net_cents: index === 18 ? 123456 : 0,
  }))
  const html = context.renderSalesTrend(days, 'month')
  assert.equal((html.match(/<li>/g) || []).length, 22)
  assert.match(html, /11-1<small>Sun/)
  assert.match(html, /11-2<small>Mon/)
  assert.match(html, /<strong>\$1,234\.56<\/strong>\s*<\/span>\s*<span class="sales-trend-label">11-19<small>Thu/)
})

test('hourly bars use 12-hour shop hours with Eastern timestamp formatting', () => {
  const hours = Array.from({ length: 24 }, (_, hour) => ({
    label: `${String(hour).padStart(2, '0')}:00`, net_cents: 0,
  }))
  const html = context.renderSalesTrend(hours, 'today')
  assert.equal((html.match(/<li>/g) || []).length, 12)
  assert.match(html, />9 AM</)
  assert.match(html, />12 PM</)
  assert.match(html, />8 PM</)
  assert.doesNotMatch(html, />8 AM<|>9 PM<|>13:00</)
  assert.equal(context.formatDate('2026-11-22T19:36:00Z'), 'Nov 22, 2:36 PM EST')
  assert.equal(context.formatDate('2026-07-02T18:36:00Z'), 'Jul 2, 2:36 PM EDT')
})

test('receipt uses Eastern POS time and escapes merchandise fields', () => {
  const html = context.renderSalesReceipt({
    invoice: '101', date: '2026-11-22', time: '14:36', register_id: 'R1',
    net_cents: 15000, gross_profit_cents: 5000, gross_margin_percent: 50, missing_cost_lines: 1,
    lines: [{ sku: '<SKU>', name: '<Suit>', units: 1, net_cents: 15000, gross_profit_cents: null }],
  })
  assert.match(html, /Ticket #101/)
  assert.match(html, /2:36 PM ET/)
  assert.match(html, /Partial/)
  assert.match(html, /&lt;Suit&gt;/)
  assert.match(html, /SKU &lt;SKU&gt;/)
  assert.doesNotMatch(html, /<Suit>/)
})
