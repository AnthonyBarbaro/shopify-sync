const routes = {
  "/app": { title: "Overview", render: renderOverview },
  "/app/orders": { title: "Orders", render: renderOrders },
  "/app/customers": { title: "Customers", render: renderCustomers },
  "/app/sales": { title: "Sales Board", render: renderSalesBoard },
  "/app/settings": { title: "Settings", render: renderSettings },
}

const state = {
  config: null,
  health: null,
  bridge: null,
  connection: null,
  connectionLoading: false,
  error: null,
  refreshing: false,
  rotating: false,
  testing: false,
  forcing: false,
  sales: null,
  salesLoading: false,
  salesError: null,
  salesPeriod: "month",
  salesEmployee: null,
  salesFocus: false,
  salesSettings: null,
  salesSettingsBusy: false,
  salesSettingsError: null,
  salesSettingsDirty: false,
  customerLog: null,
  customerLogError: null,
  customerLogOutcome: "",
  customerLogBefore: null,
  customerLogRequest: 0,
  customerAddressReview: null,
}

document.addEventListener("DOMContentLoaded", () => boot())
window.addEventListener("popstate", () => {
  state.customerAddressReview = null
  render()
  if (window.location.pathname === "/app/sales") refreshSales()
  if (window.location.pathname === "/app/customers") refreshCustomerLog()
})

document.addEventListener("input", (event) => {
  if (event.target.closest("[data-sales-settings]")) state.salesSettingsDirty = true
})

document.addEventListener("submit", async (event) => {
  if (!event.target.matches("[data-sales-settings]")) return
  event.preventDefault()
  await saveSalesSettings(event.target)
})

document.addEventListener("click", async (event) => {
  const routeLink = event.target.closest("[data-route]")
  if (routeLink) {
    event.preventDefault()
    navigate(routeLink.getAttribute("href"))
    return
  }

  const period = event.target.closest("[data-sales-period]")
  if (period) {
    state.salesPeriod = period.dataset.salesPeriod
    state.salesEmployee = null
    render()
    return
  }

  const employee = event.target.closest("[data-sales-employee]")
  if (employee) {
    state.salesEmployee = employee.dataset.salesEmployee
    render()
    document.getElementById("sales-employee-detail")?.focus()
    return
  }

  if (event.target.closest("[data-sales-focus]")) {
    state.salesFocus = !state.salesFocus
    render()
    return
  }

  if (event.target.closest("[data-sales-refresh]")) {
    await refreshSales()
    return
  }

  const historyFilter = event.target.closest("[data-customer-outcome]")
  if (historyFilter) {
    state.customerLogOutcome = historyFilter.dataset.customerOutcome
    state.customerLogBefore = null
    state.customerLog = null
    state.customerAddressReview = null
    await refreshCustomerLog()
    return
  }

  const historyPage = event.target.closest("[data-customer-before]")
  if (historyPage) {
    state.customerLogBefore = historyPage.dataset.customerBefore || null
    state.customerLog = null
    state.customerAddressReview = null
    await refreshCustomerLog()
    return
  }

  const addressReview = event.target.closest("[data-customer-address]")
  if (addressReview) {
    await toggleCustomerAddress(addressReview.dataset.customerAddress)
    return
  }

  const copyButton = event.target.closest("[data-copy]")
  if (copyButton) {
    await copyValue(copyButton.dataset.copy || "")
    return
  }

  if (event.target.closest("[data-force-sync]")) {
    await forceSync()
    return
  }

  if (event.target.closest("[data-refresh]")) {
    await Promise.all([refreshBridge(true), window.location.pathname === "/app/customers" ? refreshCustomerLog() : null])
    return
  }

  if (event.target.closest("[data-test-shopify]")) {
    await testShopify()
    return
  }

  if (event.target.closest("[data-rotate]")) {
    await rotateCredentials()
  }
})

async function boot() {
  await Promise.all([
    loadOptional("config", () => fetchJson("/api/ui/config").then((value) => { state.config = value })),
    loadOptional("health", () => fetchJson("/health").then((value) => { state.health = value })),
    loadOptional("bridge", () => fetchJson("/api/bridge-status?order_limit=20").then((value) => { state.bridge = value })),
  ])
  render()
  if (window.location.pathname === "/app/sales") refreshSales()
  if (window.location.pathname === "/app/customers") refreshCustomerLog()

  window.setInterval(() => {
    if (window.location.pathname === "/app/sales") refreshSales()
    else {
      refreshBridge(false).catch((error) => console.error(error))
      if (window.location.pathname === "/app/customers" && !state.customerLogBefore) refreshCustomerLog()
    }
  }, 30000)
}

async function refreshSales() {
  if (state.salesLoading) return
  state.salesLoading = true
  try {
    state.sales = await fetchJson("/api/sales-board")
    state.salesError = null
  } catch (error) {
    state.salesError = error
  } finally {
    state.salesLoading = false
    if (window.location.pathname === "/app/sales") render()
  }
}

async function refreshCustomerLog() {
  const request = ++state.customerLogRequest
  const query = new URLSearchParams()
  if (state.customerLogOutcome) query.set("outcome", state.customerLogOutcome)
  if (state.customerLogBefore) query.set("before", state.customerLogBefore)
  try {
    const data = await fetchJson(`/api/customer-sync-history?${query}`)
    if (request !== state.customerLogRequest) return
    state.customerLog = data
    state.customerLogError = null
  } catch (error) {
    if (request !== state.customerLogRequest) return
    state.customerLogError = error
  }
  if (window.location.pathname === "/app/customers") render()
}

async function toggleCustomerAddress(eventId) {
  if (state.customerAddressReview?.eventId === eventId) {
    state.customerAddressReview = null
    render()
    return
  }
  const review = { eventId, data: null, error: null }
  state.customerAddressReview = review
  render()
  try {
    review.data = await fetchJson(`/api/customer-sync-history/${encodeURIComponent(eventId)}/address`)
  } catch (error) {
    review.error = error.message
  }
  if (state.customerAddressReview === review && window.location.pathname === "/app/customers") render()
}

async function ensureSalesSettings() {
  state.salesSettingsBusy = true
  try {
    state.salesSettings = await fetchJson("/api/sales-board/settings")
  } catch (error) {
    state.salesSettingsError = error
  } finally {
    state.salesSettingsBusy = false
    if (window.location.pathname === "/app/settings") render()
  }
}

async function saveSalesSettings(form) {
  if (state.salesSettingsBusy) return
  const values = new FormData(form)
  const goal = values.get("monthly_goal")
  state.salesSettingsBusy = true
  try {
    state.salesSettings = await fetchJson("/api/sales-board/settings", {
      method: "PUT", headers: { "X-Requested-With": "pos-bridge", "Content-Type": "application/json" },
      body: JSON.stringify({ enabled: values.has("enabled"), interval_seconds: Number(values.get("interval")),
        monthly_goal_cents: goal ? Math.round(Number(goal) * 100) : null }),
    })
    state.salesSettingsDirty = false
    state.salesSettingsError = null
    showToast("Sales Board settings saved.", "success")
    if (window.location.pathname === "/app/settings") render()
  } catch (error) {
    showToast(error.message, "error")
  } finally {
    state.salesSettingsBusy = false
  }
}

async function loadOptional(name, loader) {
  try {
    await loader()
  } catch (error) {
    console.error(`${name} failed`, error)
    state.error = error
  }
}

async function refreshBridge(showMessage) {
  state.refreshing = true
  if (showMessage) render()
  try {
    state.bridge = await fetchJson("/api/bridge-status?order_limit=20")
    state.error = null
    if (showMessage) showToast("Status refreshed.", "success")
  } catch (error) {
    state.error = error
    if (showMessage) showToast(error.message, "error")
  } finally {
    state.refreshing = false
    if (window.location.pathname !== "/app/settings" || !state.salesSettingsDirty) render()
  }
}

async function forceSync() {
  state.forcing = true
  render()
  try {
    const request = await fetchJson("/api/connector/force-sync", {
      method: "POST", headers: { "X-Requested-With": "pos-bridge" },
    })
    state.bridge = { ...state.bridge, force_sync: request }
    showToast("Full inventory check queued. The connector will also check customers.", "success")
  } catch (error) {
    showToast(error.message, "error")
  } finally {
    state.forcing = false
    render()
  }
}

async function ensureConnection() {
  if (state.connection || state.connectionLoading) return
  state.connectionLoading = true
  try {
    state.connection = await fetchJson("/api/connection-settings")
  } finally {
    state.connectionLoading = false
  }
}

async function rotateCredentials() {
  if (!window.confirm("Create a new connector key and secret? The current Windows connector will stop until its settings are updated.")) return
  state.rotating = true
  render()
  try {
    state.connection = await fetchJson("/api/connection-settings/rotate", { method: "POST" })
    showToast("New connector credentials created.", "success")
  } catch (error) {
    showToast(error.message, "error")
  } finally {
    state.rotating = false
    render()
  }
}

async function testShopify() {
  state.testing = true
  render()
  try {
    await fetchJson("/health/shopify")
    showToast("Shopify connection is working.", "success")
  } catch (error) {
    showToast(error.message, "error")
  } finally {
    state.testing = false
    render()
  }
}

function navigate(path) {
  window.history.pushState({}, "", `${path}${window.location.search}`)
  state.salesSettingsDirty = false
  state.customerAddressReview = null
  if (path === "/app/settings") {
    state.salesSettingsError = null
    ensureConnection().then(render).catch((error) => {
      state.error = error
      render()
    })
  }
  render()
  if (path === "/app/sales") refreshSales()
  if (path === "/app/customers") refreshCustomerLog()
  window.scrollTo({ top: 0, behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth" })
}

function render() {
  const app = document.getElementById("app")
  const route = normalizeRoute(window.location.pathname)
  document.title = `${routes[route].title} · POS Bridge`
  updateTabs(route)

  if (route === "/app/settings" && !state.connection && !state.connectionLoading && !state.error) {
    ensureConnection().then(render).catch((error) => {
      state.error = error
      render()
    })
  }
  if (route === "/app/settings" && !state.salesSettings && !state.salesSettingsBusy && !state.salesSettingsError) ensureSalesSettings()

  app.innerHTML = routes[route].render()
}

function renderOverview() {
  const connector = state.bridge?.connector || {}
  const inventory = state.bridge?.inventory || {}
  const orders = state.bridge?.orders || {}
  const customers = state.bridge?.customers || {}
  const prices = state.bridge?.prices || {}
  const bridgeActive = connector.active === true
  const forceRequest = state.bridge?.force_sync || {}
  const forcePending = Boolean(forceRequest.request_id && !forceRequest.completed_at)
  const setupNeeded = orders.authorized === false
  const storeName = state.config?.shop_name || state.config?.shop || "Your store"
  const orderQueue = `${number(orders.queued_orders)} order${number(orders.queued_orders) === 1 ? "" : "s"} waiting to be written to the POS computer.`

  return `
    <section class="page">
      <div class="welcome">
        <div>
          <p class="kicker">Store overview <span aria-hidden="true">/</span> ${escapeHtml(storeName)}</p>
          <h1>${bridgeActive ? "Your store, connected." : "Let’s get you connected."}</h1>
          <p>${bridgeActive
            ? "The Windows connector is checking in. Each sync channel reports its own activity below."
            : "No connector check-in received in the last 8 minutes. Check the Windows scheduled task and connector log."}</p>
        </div>
        <div class="welcome-actions">
          ${statusBadge(bridgeActive ? "Connected" : "No check-in", bridgeActive ? "success" : "danger", true)}
          <button class="button" type="button" data-force-sync ${state.forcing || forcePending ? "disabled" : ""}>
            ${icon("refresh", state.forcing ? "is-spinning" : "")} ${state.forcing ? "Requesting…" : forcePending ? "Sync queued" : "Force sync"}
          </button>
          <button class="button secondary" type="button" data-refresh ${state.refreshing ? "disabled" : ""}>
            ${icon("refresh", state.refreshing ? "is-spinning" : "")} ${state.refreshing ? "Checking…" : "Refresh"}
          </button>
          <small>${state.bridge?.timestamp ? `Updated ${escapeHtml(relativeTime(state.bridge.timestamp))}` : "Refreshes every 30 seconds"}</small>
        </div>
      </div>

      ${state.error ? `<div class="notice danger">Status could not be loaded. ${escapeHtml(state.error.message)}</div>` : ""}
      ${forcePending ? `<div class="notice neutral">Full inventory check and customer scan requested. ${bridgeActive ? "Waiting for the connector’s next check-in; long-running jobs finish first." : "Queued until the Windows connector reconnects."}</div>` : forceRequest.completed_at ? `<div class="notice neutral">Last forced inventory scan finished ${escapeHtml(relativeTime(forceRequest.completed_at))}. Failed adjustments continue to retry.</div>` : ""}
      ${setupNeeded ? `<div class="notice warning"><strong>Order access needs approval.</strong> Reopen or reinstall the Shopify app once so Shopify can approve the read_orders permission.</div>` : ""}
      ${customers.enabled && customers.authorized === false ? `<div class="notice warning"><strong>Customer access needs approval.</strong> Add read_customers and write_customers to APP_SCOPES and reopen the Shopify install flow to grant access.</div>` : ""}

      <div class="status-grid">
        ${renderStatusCard({
          icon: "inventory",
          title: "Inventory bridge",
          active: inventory.active,
          activeText: "Inventory bridge is active",
          inactiveText: "No recent inventory check-in",
          detail: inventory.active
            ? `Checks every ${syncIntervalLabel(connector.sync_interval_seconds)}. ${number(inventory.queued_changes)} Shopify change${number(inventory.queued_changes) === 1 ? "" : "s"} waiting for the POS`
            : bridgeActive ? "Connector is connected, but inventory polling is delayed. Check its log for a long-running job or errors." : "Start the Windows scheduled task to reconnect.",
          lastSeen: inventory.last_poll_at,
        })}
        ${renderStatusCard({
          icon: "orders",
          title: "Order sync",
          active: orders.enabled !== false && orders.active && orders.authorized,
          activeText: "Order sync is active",
          inactiveText: orders.enabled === false ? "Order sync is off" : orders.authorized === false ? "Order access needs approval" : "No recent order check-in",
          detail: !orders.active && orders.authorized && bridgeActive && orders.enabled !== false
            ? `${orders.enabled === true ? "Check the Windows connector log for order inbox errors." : "Older connectors check orders in the inventory loop, so delays can affect both checks."} ${orderQueue}`
            : orderQueue,
          lastSeen: orders.last_poll_at,
        })}
        ${renderStatusCard({
          icon: "customers",
          title: "Customer sync · POS → Shopify",
          active: customers.enabled && customers.authorized && customers.active,
          activeText: "Customer connector is checking in",
          inactiveText: !customers.enabled ? "Customer sync is off" : customers.authorized === false ? "Customer access needs approval" : "No recent customer check-in",
          detail: customers.enabled
            ? `Checks every ${syncIntervalLabel(customers.interval_seconds || 60)} and adds missing POS customers. Existing Shopify customers stay unchanged. See the Customers tab for results.`
            : "Update the Windows connector and enable CUSTOMER_SYNC_ENABLED to send POS customers to Shopify.",
          lastSeen: customers.last_poll_at,
        })}
      </div>

      <section class="panel" aria-labelledby="inventory-changes-heading">
        <div class="panel-head">
          <div>
            <p class="kicker">Inventory tracking</p>
            <h2 id="inventory-changes-heading">Recent quantity changes</h2>
          </div>
          <span class="panel-label">POS <span aria-hidden="true">→</span> Shopify</span>
        </div>
        ${renderInventoryChangeList(inventory.recent || [])}
        <div class="panel-footer">Latest 10 successful quantity updates <span>POS checks every ${syncIntervalLabel(connector.sync_interval_seconds)} · Display refreshes every 30 seconds</span></div>
      </section>

      <div class="activity-grid">
      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="kicker">Shopify → POS</p>
            <h2>Recent orders</h2>
          </div>
          <a class="text-link" href="/app/orders" data-route>View orders ${icon("arrow")}</a>
        </div>
        ${renderOrderList((orders.recent || []).slice(0, 5), true)}
      </section>

      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="kicker">Price tracking</p>
            <h2>Recent price changes</h2>
          </div>
        </div>
        ${renderPriceChangeList(prices.recent || [])}
      </section>
      </div>

      <section class="flow-card">
        <div><span>1</span><strong>POS sales</strong><small>Inventory events are detected</small></div>
        <i aria-hidden="true">→</i>
        <div><span>2</span><strong>Inventory sync</strong><small>Shopify quantities are updated</small></div>
        <i aria-hidden="true">→</i>
        <div><span>3</span><strong>Online orders</strong><small>Recent orders reach the POS database</small></div>
      </section>
    </section>
  `
}

function renderStatusCard({ icon: iconName, title, active, activeText, inactiveText, detail, lastSeen }) {
  return `
    <article class="status-card ${active ? "is-active" : "is-offline"}">
      <div class="status-card-top">
        <span class="feature-icon">${icon(iconName)}</span>
        ${statusBadge(active ? "Active" : "Needs attention", active ? "success" : "danger")}
      </div>
      <div>
        <p class="kicker">${escapeHtml(title)}</p>
        <h2>${escapeHtml(active ? activeText : inactiveText)}</h2>
        <p>${escapeHtml(detail)}</p>
      </div>
      <small class="status-last-seen">${icon("clock")} ${lastSeen ? `Last check-in ${escapeHtml(relativeTime(lastSeen))}` : "No check-in recorded yet"}</small>
    </article>
  `
}

function renderOrders() {
  const orders = state.bridge?.orders || {}
  const recent = orders.recent || []
  const delivered = recent.filter((order) => order.delivery_status === "sent_to_pos").length
  return `
    <section class="page">
      <div class="page-head">
        <div>
          <p class="kicker">Shopify <span aria-hidden="true">→</span> POS</p>
          <h1>Recent Shopify orders</h1>
          <p>Follow your online orders from checkout to your store’s POS.</p>
        </div>
        <div class="page-actions">
          <small>Refreshes every 30 seconds</small>
          <button class="button secondary" type="button" data-refresh ${state.refreshing ? "disabled" : ""}>${icon("refresh", state.refreshing ? "is-spinning" : "")} ${state.refreshing ? "Checking…" : "Refresh"}</button>
        </div>
      </div>

      ${state.error ? `<div class="notice danger">Orders could not be refreshed. ${escapeHtml(state.error.message)}</div>` : ""}

      <div class="mini-stats">
        <div><span class="stat-label">Recent orders ${icon("orders")}</span><strong>${recent.length}</strong><small>In your recent history</small></div>
        <div><span class="stat-label">Waiting for POS ${icon("clock")}</span><strong>${number(orders.queued_orders)}</strong><small>Queued for delivery</small></div>
        <div><span class="stat-label">Sent to POS ${icon("check")}</span><strong>${delivered}</strong><small>Of the orders shown</small></div>
      </div>

      <section class="panel orders-panel" aria-labelledby="order-history-heading">
        <div class="panel-head">
          <div><p class="kicker">Order activity</p><h2 id="order-history-heading">Delivery history</h2></div>
          <span class="panel-label">${recent.length} recent order${recent.length === 1 ? "" : "s"}</span>
        </div>
        ${recent.length ? `<div class="list-heading" aria-hidden="true"><span>Order / placed</span><span>Amount / payment</span><span>Delivery status</span></div>` : ""}
        ${renderOrderList(recent, false)}
        <div class="panel-footer">${icon("check")} Order details are delivered automatically to your POS.</div>
      </section>

    </section>
  `
}

function salesMoney(cents) {
  return formatMoney(cents / 100, "USD")
}

function salesDate(value) {
  return new Date(`${value}T12:00:00`).toLocaleDateString([], { month: "short", day: "numeric", year: "numeric" })
}

function renderSalesBoard() {
  const snapshot = state.sales?.snapshot
  const focused = state.salesFocus ? "is-focused" : ""
  const settings = { today: "Today", week: "This week", month: "This month", year: "This year" }
  const actions = `<div class="sales-actions">
    <button class="button secondary" type="button" data-sales-refresh>${icon("refresh")} Refresh</button>
    <button class="button secondary" type="button" data-sales-focus aria-pressed="${state.salesFocus}">${state.salesFocus ? "Exit tablet view" : "Tablet view"}</button>
  </div>`

  if (!snapshot) {
    return `<section class="page sales-board ${focused}">
      <div class="page-head"><div><h1>Sales Board</h1><p>In-store sales · Alterations excluded</p></div>${actions}</div>
      <section class="panel"><div class="empty"><strong>${state.salesError ? "Sales Board could not load" : state.sales?.settings?.enabled === false ? "Sales Board is paused" : state.sales ? "Waiting for POS sales data" : "Loading sales data…"}</strong>
      <p>${state.salesError ? escapeHtml(state.salesError.message) : state.sales?.settings?.enabled === false ? `<a href="/app/settings" data-route>Open settings</a> to resume sales reporting.` : state.sales ? "Update the Windows connector and let it send its first sales summary." : ""}</p></div></section>
    </section>`
  }

  const period = snapshot.periods[state.salesPeriod]
  const totals = period.store
  const ranked = period.employees.filter((person) => person.employee_id !== "unassigned")
  const unassigned = period.employees.find((person) => person.employee_id === "unassigned")
  const selected = period.employees.find((person) => person.employee_id === state.salesEmployee)
  const stale = Date.now() - Date.parse(snapshot.observed_at) > 12 * 60 * 1000
  const goal = state.sales.settings?.monthly_goal_cents
  const monthlyNet = snapshot.periods.month.store.net_cents
  const goalPercent = goal ? Math.round(monthlyNet / goal * 100) : 0
  const status = state.salesError ? "Refresh failed · showing saved totals" : stale ? "POS update delayed" : "POS data current"
  const dates = `${salesDate(period.start)}${period.start === snapshot.business_date ? "" : ` – ${salesDate(snapshot.business_date)}`}`
  return `<section class="page sales-board ${focused}">
    <div class="sales-header">
      <div><h1>Sales Board</h1><p>In-store sales · Alterations excluded</p></div>
      ${actions}
    </div>
    <div class="sales-toolbar">
      <div class="sales-periods" role="group" aria-label="Reporting period">${Object.entries(settings).map(([key, label]) =>
        `<button type="button" data-sales-period="${key}" aria-pressed="${key === state.salesPeriod}">${label}</button>`).join("")}</div>
      <div class="sales-freshness">${statusBadge(status, stale || state.salesError ? "warning" : "success")} <span title="${escapeHtml(formatDate(snapshot.observed_at))}">Updated ${escapeHtml(relativeTime(snapshot.observed_at))}</span></div>
    </div>
    <div class="sales-scorecard">
      <div class="sales-net"><span>Net sales</span><strong>${salesMoney(totals.net_cents)}</strong><small>${escapeHtml(dates)}</small></div>
      <div><span>Sales tickets</span><strong>${formatSalesCount(totals.tickets)}</strong></div>
      <div><span>Net units</span><strong>${formatSalesCount(totals.units)}</strong></div>
      <div><span>Average ticket</span><strong>${totals.tickets ? salesMoney(totals.net_cents / totals.tickets) : "—"}</strong></div>
      <div><span>Returns &amp; credits</span><strong>${salesMoney(totals.credits_cents)}</strong></div>
    </div>
    <div class="sales-layout">
      <section class="panel sales-leaderboard" aria-labelledby="sales-leaderboard-heading">
        <div class="panel-head"><h2 id="sales-leaderboard-heading">Employee leaderboard</h2><span class="panel-label">${settings[state.salesPeriod]}</span></div>
        ${ranked.length || unassigned ? `<div class="sales-leader-labels" aria-hidden="true"><span>Employee</span><span>Tickets</span><span>Net sales</span></div>
          ${ranked.map((person, index) => renderSalesEmployee(person, index + 1)).join("")}
          ${unassigned ? renderSalesEmployee(unassigned, null) : ""}` : `<div class="empty"><strong>No merchandise sales in this period</strong></div>`}
      </section>
      <div class="sales-side">
        <section class="panel">
          <div class="panel-head"><h2>Monthly goal</h2></div>
          ${goal ? `<div class="sales-goal"><strong>${Math.max(0, goalPercent)}%</strong><span>${salesMoney(monthlyNet)} of ${salesMoney(goal)}</span></div>
            <progress aria-label="Monthly sales goal" max="${goal}" value="${Math.max(0, Math.min(monthlyNet, goal))}"></progress>
            <p class="sales-muted">${monthlyNet >= goal ? "Goal reached" : `${salesMoney(goal - monthlyNet)} to goal`}</p>`
          : `<p class="sales-muted">No monthly goal configured.</p>`}
        </section>
        <section class="panel sales-categories"><div class="panel-head"><h2>Category mix</h2></div>${renderSalesCategories(period.categories)}</section>
      </div>
    </div>
    ${selected ? `<section class="panel sales-employee-detail" id="sales-employee-detail" tabindex="-1" aria-labelledby="sales-employee-heading">
      <div class="panel-head"><h2 id="sales-employee-heading">${escapeHtml(selected.name)}</h2><span class="panel-label">${settings[state.salesPeriod]}</span></div>
      <div class="sales-employee-stats"><span><strong>${salesMoney(selected.net_cents)}</strong> Net sales</span><span><strong>${formatSalesCount(selected.tickets)}</strong> Tickets</span><span><strong>${formatSalesCount(selected.units)}</strong> Net units</span><span><strong>${selected.tickets ? salesMoney(selected.net_cents / selected.tickets) : "—"}</strong> Average ticket</span></div>
      ${renderSalesCategories(selected.categories)}
    </section>` : ""}
    <section class="panel"><div class="panel-head"><h2>${state.salesPeriod === "today" ? "Sales by hour" : state.salesPeriod === "year" ? "Sales by month" : "Sales by day"}</h2><span class="panel-label">Net sales</span></div>${renderSalesTrend(period.trend)}</section>
    <p class="sales-muted">${snapshot.last_invoice_date ? `Latest completed POS invoice: ${escapeHtml(salesDate(snapshot.last_invoice_date))}. ` : ""}Sales credit follows the clerk on each line. A shared ticket can count for more than one employee.</p>
  </section>`
}

function formatSalesCount(value) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)
}

function renderSalesEmployee(person, rank) {
  const id = escapeHtml(person.employee_id)
  return `<button type="button" class="sales-leader-row" data-sales-employee="${id}" aria-pressed="${person.employee_id === state.salesEmployee}">
    <span class="sales-person"><span class="sales-rank ${rank && rank <= 3 ? "is-top" : ""}">${rank ?? "—"}</span><strong>${escapeHtml(person.name)}</strong></span>
    <span>${formatSalesCount(person.tickets)}</span><strong>${salesMoney(person.net_cents)}</strong>
  </button>`
}

function renderSalesCategories(categories) {
  if (!categories.length) return `<p class="sales-muted">No categories in this period.</p>`
  const scale = Math.max(1, ...categories.map(({ net_cents }) => Math.abs(net_cents)))
  return `<ul class="sales-category-list">${categories.map(({ name, net_cents }) => `<li>
    <div><span>${escapeHtml(name)}</span><strong>${salesMoney(net_cents)}</strong></div>
    <div class="sales-category-bar" aria-hidden="true"><i class="${net_cents < 0 ? "is-negative" : ""}" style="width:${Math.abs(net_cents) / scale * 100}%"></i></div>
  </li>`).join("")}</ul>`
}

function renderSalesTrend(trend) {
  if (!trend.some(({ net_cents }) => net_cents)) return `<p class="sales-muted">No sales activity to chart in this period.</p>`
  const scale = Math.max(1, ...trend.map(({ net_cents }) => Math.abs(net_cents)))
  return `<ul class="sales-trend">${trend.map(({ label, net_cents }, index) => {
    const text = /^\d{4}-\d{2}-\d{2}$/.test(label) ? label.slice(-2) : /^\d{4}-\d{2}$/.test(label)
      ? new Date(`${label}-01T12:00:00`).toLocaleDateString([], { month: "short" }) : label
    return `<li><span class="sales-trend-bar"><i class="${net_cents < 0 ? "is-negative" : ""}" style="--bar:${Math.abs(net_cents) / scale * 100}%"></i></span>
      <span aria-hidden="true">${index % Math.ceil(trend.length / 12) === 0 ? escapeHtml(text) : ""}</span>
      <span class="sales-chart-value">${escapeHtml(label)} · ${salesMoney(net_cents)}</span></li>`
  }).join("")}</ul>`
}

function renderCustomers() {
  const customers = state.bridge?.customers || {}
  const recent = state.customerLog?.items || []
  const showingLegacy = !recent.length && !state.customerLogOutcome && !state.customerLogBefore && (customers.recent || []).length
  const filters = [["", "All activity"], ["created", "Added"], ["matched", "Matched"], ["failed", "Failed"]]
  const checkIn = customers.last_poll_at
    ? `Last check-in ${relativeTime(customers.last_poll_at)}`
    : "No check-in recorded yet"
  let status = `<div class="notice neutral">${escapeHtml(checkIn)} · Checks every ${syncIntervalLabel(customers.interval_seconds || 60)}</div>`
  if (customers.enabled === false) {
    status = `<div class="notice warning">Customer sync is off. Enable <code>CUSTOMER_SYNC_ENABLED</code> on the Windows connector.</div>`
  } else if (customers.authorized === false) {
    status = `<div class="notice warning">Customer access needs approval. Grant <code>read_customers</code> and <code>write_customers</code> through the Shopify install flow.</div>`
  } else if (customers.active === false) {
    status = `<div class="notice warning">${escapeHtml(checkIn)}. Check the Windows connector log for customer sync errors.</div>`
  }

  return `
    <section class="page">
      <div class="page-head">
        <div>
          <p class="kicker">POS <span aria-hidden="true">→</span> Shopify</p>
          <h1>Customer sync</h1>
          <p>See which POS customers were added to Shopify, matched to an existing customer, or failed to import.</p>
        </div>
        <div class="page-actions">
          <small>Refreshes every 30 seconds</small>
          <button class="button secondary" type="button" data-refresh ${state.refreshing ? "disabled" : ""}>${icon("refresh", state.refreshing ? "is-spinning" : "")} ${state.refreshing ? "Checking…" : "Refresh"}</button>
        </div>
      </div>

      ${state.error ? `<div class="notice danger">Customer activity could not be refreshed. ${escapeHtml(state.error.message)}</div>` : status}

      <section class="panel" aria-labelledby="customer-history-heading">
        <div class="panel-head">
          <h2 id="customer-history-heading">Customer activity log</h2>
        </div>
        <div class="sales-periods" role="group" aria-label="Filter customer activity">${filters.map(([key, label]) =>
          `<button type="button" data-customer-outcome="${key}" aria-pressed="${key === state.customerLogOutcome}">${label}</button>`).join("")}</div>
        ${state.customerLogError ? `<p class="sales-muted">${escapeHtml(state.customerLogError.message)}</p>` : ""}
        ${renderCustomerSyncHistory(showingLegacy ? customers.recent : recent)}
        <div class="customer-log-pages">
          ${state.customerLogBefore ? `<button class="button secondary" type="button" data-customer-before="">Newest</button>` : ""}
          ${state.customerLog?.next_before ? `<button class="button secondary" type="button" data-customer-before="${state.customerLog.next_before}">Older results</button>` : ""}
        </div>
      </section>
    </section>
  `
}

function renderCustomerSyncHistory(items) {
  if (!items.length) {
    return `<div class="empty"><span>${icon("customers")}</span><strong>No customer imports recorded yet</strong><p>Results appear here when the Windows connector submits customer records to Shopify.</p></div>`
  }
  const outcomes = {
    created: ["Added to Shopify", "success"],
    matched: ["Matched existing", "success"],
    failed: ["Import failed", "danger"],
    processed: ["Processed", "success"],
  }
  return `
    <div class="order-list customer-history">
      ${items.map((item) => `
        <article class="order-row">
          <div class="order-main">
            <span class="order-icon" aria-hidden="true">${icon("customers")}</span>
            <div>
              <strong>${item.pos_customer_number ? `POS customer #${escapeHtml(item.pos_customer_number)}` : "POS customer"}</strong>
              <small>${escapeHtml(formatDate(item.received_at))}</small>
              ${item.reason ? `<small>${escapeHtml(item.reason)}</small>` : ""}
              ${item.has_address_review ? `<button class="text-link customer-address-toggle" type="button" data-customer-address="${escapeHtml(item.id)}" aria-expanded="${state.customerAddressReview?.eventId === String(item.id)}" aria-controls="customer-address-${escapeHtml(item.id)}">${state.customerAddressReview?.eventId === String(item.id) ? "Hide address details" : "View address details"}</button>` : ""}
            </div>
          </div>
          ${statusBadge(...(outcomes[item.outcome] || outcomes.processed))}
          ${item.has_address_review ? renderCustomerAddressReview(item.id) : ""}
        </article>
      `).join("")}
    </div>
  `
}

function renderCustomerAddressReview(id) {
  const review = state.customerAddressReview
  const attributes = `id="customer-address-${escapeHtml(id)}" class="customer-address-review"`
  if (review?.eventId !== String(id)) return `<div ${attributes} hidden></div>`
  if (review.error) return `<div ${attributes} role="status">${escapeHtml(review.error)}</div>`
  if (!review.data) return `<div ${attributes} role="status">Loading address details…</div>`
  const { issues, ambiguous, addresses, reviewed_at } = review.data
  return `<div ${attributes}>
    <div class="customer-address-heading"><strong>Shopify validation</strong><small>${escapeHtml(formatDate(reviewed_at))}</small></div>
    <ul>${issues.map((issue) => `<li>${issue.address_number ? `<strong>Address ${escapeHtml(issue.address_number)}</strong> · ` : ""}<code>${escapeHtml(issue.field)}</code><span>${escapeHtml(issue.message)}</span></li>`).join("")}</ul>
    ${ambiguous && addresses.length > 1 ? `<p>Shopify did not identify which submitted address caused at least one of these errors.</p>` : ""}
    <div class="customer-address-cards">${addresses.map((address) => `<div>
      <strong>Submitted address ${escapeHtml(address.number)}</strong>
      ${[["Street", address.address1], ["Unit", address.address2], ["City", address.city], ["State / province", address.provinceCode], ["ZIP / postal code", address.zip], ["Country", address.countryCode]].map(([label, value]) => `<span>${label}</span><span>${escapeHtml(value || "—")}</span>`).join("")}
    </div>`).join("")}</div>
  </div>`
}

function renderOrderList(items, compact) {
  if (!items.length) {
    return `<div class="empty"><span>${icon("orders")}</span><strong>No recent orders yet</strong><p>Your next Shopify order will appear here when it reaches the bridge.</p></div>`
  }

  return `
    <div class="order-list order-history ${compact ? "compact" : ""}">
      ${items.map((order) => {
        const sent = order.delivery_status === "sent_to_pos"
        return `
          <article class="order-row">
            <div class="order-main">
              <span class="order-icon ${sent ? "" : "is-pending"}" aria-hidden="true">${icon(sent ? "orders" : "clock")}</span>
              <div>
                <strong>${escapeHtml(order.order_name || `Order ${order.shopify_order_id}`)}</strong>
                <small>${escapeHtml(formatDate(order.order_created_at || order.received_at))}</small>
              </div>
            </div>
            <div class="order-meta">
              <strong>${escapeHtml(formatMoney(order.total_price, order.currency))}</strong>
              <small>${escapeHtml(labelize(order.financial_status || "status unavailable"))}</small>
            </div>
            ${statusBadge(sent ? "Sent to POS" : "Waiting for POS", sent ? "success" : "warning")}
          </article>
        `
      }).join("")}
    </div>
  `
}

function renderInventoryChangeList(items) {
  if (!items.length) {
    return `<div class="empty"><span>${icon("inventory")}</span><strong>No quantity changes recorded yet</strong><p>Successful POS → Shopify quantity updates will appear here. Earlier changes are not included.</p></div>`
  }

  return `
    <div class="order-list compact">
      ${items.map((change) => `
        <article class="order-row">
          <div class="order-main">
            <span class="order-icon" aria-hidden="true">${change.delta > 0 ? "+" : "−"}</span>
            <div>
              <strong>${escapeHtml(change.product_title || `SKU ${change.sku}`)}</strong>
              ${change.product_title ? `<small>SKU ${escapeHtml(change.sku)}${change.variant_title && change.variant_title !== "Default Title" ? ` · ${escapeHtml(change.variant_title)}` : ""}</small>` : ""}
              <small>${escapeHtml(formatDate(change.changed_at))}</small>
            </div>
          </div>
          <div class="price-change ${change.delta > 0 ? "quantity-increase" : "quantity-decrease"}">
            <small>${change.quantity_after_change == null ? "Resulting quantity unavailable" : `Resulting quantity: ${escapeHtml(change.quantity_after_change)}`}</small>
            <strong>${change.delta > 0 ? "+" : ""}${escapeHtml(change.delta)} ${Math.abs(change.delta) === 1 ? "unit" : "units"}</strong>
          </div>
          ${statusBadge("Updated", "success")}
        </article>
      `).join("")}
    </div>
  `
}

function renderPriceChangeList(items) {
  if (!items.length) {
    return `<div class="empty"><span>${icon("price")}</span><strong>No price changes detected yet</strong><p>Price updates will appear here after the connector’s next check.</p></div>`
  }

  return `
    <div class="order-list compact">
      ${items.map((change) => `
        <article class="order-row">
          <div class="order-main">
            <span class="order-icon" aria-hidden="true">${icon("price")}</span>
            <div>
              <strong>SKU ${escapeHtml(change.source_sku)}</strong>
              <small>${escapeHtml(formatDate(change.changed_at))}${number(change.target_count) > 1 ? ` · ${number(change.target_count)} matrix variants` : ""}</small>
            </div>
          </div>
          <div class="price-change">
            <small>${change.old_price == null ? "New tracked price" : `$${escapeHtml(change.old_price)}`}</small>
            <strong>→ $${escapeHtml(change.new_price)}</strong>
          </div>
          ${statusBadge(change.success ? "Updated" : "Retrying", change.success ? "success" : "warning")}
        </article>
      `).join("")}
    </div>
  `
}

function renderSettings() {
  const connection = state.connection
  const connector = state.bridge?.connector || {}
  const board = state.salesSettings
  return `
    <section class="page narrow">
      <div class="page-head">
        <div>
          <p class="kicker">Settings</p>
          <h1>Connection settings</h1>
          <p>Keep your store’s POS and Shopify working together.</p>
        </div>
        <button class="button secondary" type="button" data-test-shopify ${state.testing ? "disabled" : ""}>${icon("link")} ${state.testing ? "Testing…" : "Test Shopify"}</button>
      </div>

      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="kicker">Windows connector</p>
            <h2>Connection details</h2>
          </div>
        </div>
        ${connection ? `
          <div class="setting-list">
            ${settingRow("Server URL", connection.base_url)}
            ${settingRow("Product path", connection.product_sync_path)}
            ${settingRow("API key", connection.api_key)}
            ${settingRow("API secret", connection.api_secret || connection.api_secret_masked)}
          </div>
          <div class="settings-actions">
            <button class="button secondary" type="button" data-copy="${escapeAttribute(connectionText(connection))}">Copy all settings</button>
            <button class="button danger" type="button" data-rotate ${state.rotating ? "disabled" : ""}>${state.rotating ? "Creating…" : "Create new key"}</button>
          </div>
        ` : `<div class="empty"><span class="spinner"></span><strong>Loading connector settings…</strong></div>`}
      </section>

      <section class="panel" aria-labelledby="sales-settings-heading">
        <div class="panel-head"><h2 id="sales-settings-heading">Sales Board</h2></div>
        ${board ? `<form class="sales-settings-form" data-sales-settings>
          <label class="sales-settings-toggle"><input type="checkbox" name="enabled" ${board.enabled ? "checked" : ""}> Enable Sales Board</label>
          <label>POS check interval (seconds)<input type="number" name="interval" min="30" max="900" step="1" value="${board.interval_seconds}" required></label>
          <label>Monthly sales goal (USD)<input type="number" name="monthly_goal" min="0.01" max="1000000000" step="0.01" value="${board.monthly_goal_cents == null ? "" : (board.monthly_goal_cents / 100).toFixed(2)}" placeholder="Optional"></label>
          <button class="button" type="submit">Save Sales Board settings</button>
        </form>` : `<p class="sales-muted">${state.salesSettingsError ? escapeHtml(state.salesSettingsError.message) : "Loading Sales Board settings…"}</p>`}
      </section>

      <div class="notice neutral">
        Inventory and POS price changes are checked every ${syncIntervalLabel(connector.sync_interval_seconds)}. The updated connector sends a separate heartbeat every 30 seconds. Customer sync checks every ${syncIntervalLabel(connector.customer_sync_interval_seconds || 60)} when enabled and adds missing POS customers to Shopify. Existing Shopify customer details stay unchanged. A full inventory reconciliation runs nightly. Order payloads are removed from Railway after the Windows connector confirms they were written.
      </div>
    </section>
  `
}

function settingRow(label, value) {
  return `
    <div class="setting-row">
      <div><span>${escapeHtml(label)}</span><code>${escapeHtml(value || "Not available")}</code></div>
      <button type="button" data-copy="${escapeAttribute(value || "")}" ${value ? "" : "disabled"}>Copy</button>
    </div>
  `
}

function syncIntervalLabel(value) {
  const seconds = Number(value)
  if (!Number.isFinite(seconds) || seconds <= 0) return "configured interval"
  return seconds % 60 === 0
    ? `${seconds / 60} minute${seconds === 60 ? "" : "s"}`
    : `${seconds} seconds`
}

function statusBadge(text, tone, large = false) {
  return `<span class="badge ${tone} ${large ? "large" : ""}"><i></i>${escapeHtml(text)}</span>`
}

function connectionText(connection) {
  return [
    `CONNECTOR_BASE_URL=${connection.base_url || ""}`,
    `CONNECTOR_API_KEY=${connection.api_key || ""}`,
    `CONNECTOR_API_SECRET=${connection.api_secret || "ROTATE_TO_REVEAL_FULL_SECRET"}`,
  ].join("\n")
}

function normalizeRoute(path) {
  if (["/app/product-sync", "/app/bulk-sync", "/app/pos-archive", "/app/catalog"].includes(path)) return "/app"
  return routes[path] ? path : "/app"
}

function updateTabs(route) {
  document.querySelectorAll(".tabs a").forEach((link) => {
    const active = link.getAttribute("href") === route
    link.classList.toggle("active", active)
    if (active) link.setAttribute("aria-current", "page")
    else link.removeAttribute("aria-current")
  })
}

function icon(name, className = "") {
  const paths = {
    refresh: '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6 7a7 7 0 0 1 12-2l2 2M4 17l2 2a7 7 0 0 0 12-2"/>',
    inventory: '<path d="m12 3 9 5-9 5-9-5 9-5ZM3 8v9l9 5 9-5V8M12 13v9M7 5.8l9 5"/>',
    orders: '<path d="M6 3h12v18l-3-2-3 2-3-2-3 2V3ZM9 8h6M9 12h6"/>',
    customers: '<circle cx="9" cy="8" r="3"/><path d="M3 21v-2a6 6 0 0 1 12 0v2M16 5a3 3 0 0 1 0 6M18 15a5 5 0 0 1 3 4v2"/>',
    clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    check: '<path d="m6 12 4 4 8-8"/>',
    arrow: '<path d="M4 12h16m-5-5 5 5-5 5"/>',
    price: '<path d="M3 3h8l10 10-8 8L3 11V3Z"/><circle cx="7.5" cy="7.5" r="1"/>',
    link: '<path d="m10 13 4-4M8 15l-1 1a4 4 0 0 1-6-6l4-4a4 4 0 0 1 6 0M16 9l1-1a4 4 0 0 1 6 6l-4 4a4 4 0 0 1-6 0" transform="translate(0 -1)"/>',
  }
  return `<svg class="icon ${className}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">${paths[name] || paths.inventory}</svg>`
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options)
  let payload
  try {
    payload = await response.json()
  } catch (_error) {
    payload = null
  }
  if (!response.ok) {
    const detail = payload?.error || {}
    throw new Error(detail.message || "Request failed.")
  }
  return payload
}

async function copyValue(value) {
  try {
    await navigator.clipboard.writeText(value)
    showToast("Copied.", "success")
  } catch (_error) {
    showToast("Could not copy.", "error")
  }
}

function showToast(message, tone) {
  const region = document.getElementById("toast-region")
  const toast = document.createElement("div")
  toast.className = `toast ${tone}`
  toast.textContent = message
  region.appendChild(toast)
  window.setTimeout(() => toast.remove(), 2600)
}

function relativeTime(value) {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000))
  if (seconds < 45) return "just now"
  if (seconds < 90) return "1 minute ago"
  if (seconds < 3600) return `${Math.round(seconds / 60)} minutes ago`
  if (seconds < 7200) return "1 hour ago"
  return `${Math.round(seconds / 3600)} hours ago`
}

function formatDate(value) {
  if (!value) return "Time unavailable"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleString([], { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })
}

function formatMoney(value, currency) {
  const amount = Number(value)
  if (!Number.isFinite(amount)) return "—"
  try {
    return new Intl.NumberFormat(undefined, { style: "currency", currency: currency || "USD" }).format(amount)
  } catch (_error) {
    return `${currency || "$"} ${amount.toFixed(2)}`
  }
}

function labelize(value) {
  return String(value).replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function number(value) {
  return Number.isFinite(Number(value)) ? Number(value) : 0
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
}

function escapeAttribute(value) {
  return escapeHtml(value)
}
