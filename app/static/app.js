const routes = {
  "/app": "Overview",
  "/app/orders": "Orders",
  "/app/settings": "Settings",
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
}

document.addEventListener("DOMContentLoaded", () => boot())
window.addEventListener("popstate", () => render())

document.addEventListener("click", async (event) => {
  const routeLink = event.target.closest("[data-route]")
  if (routeLink) {
    event.preventDefault()
    navigate(routeLink.getAttribute("href"))
    return
  }

  const copyButton = event.target.closest("[data-copy]")
  if (copyButton) {
    await copyValue(copyButton.dataset.copy || "")
    return
  }

  if (event.target.closest("[data-refresh]")) {
    await refreshBridge(true)
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

  window.setInterval(() => {
    refreshBridge(false).catch((error) => console.error(error))
  }, 30000)
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
  if (path === "/app/settings") {
    ensureConnection().then(render).catch((error) => {
      state.error = error
      render()
    })
  }
  render()
  window.scrollTo({ top: 0, behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "instant" : "smooth" })
}

function render() {
  const app = document.getElementById("app")
  const route = normalizeRoute(window.location.pathname)
  document.title = `${routes[route]} · POS Bridge`
  updateTabs(route)

  if (route === "/app/settings" && !state.connection && !state.connectionLoading && !state.error) {
    ensureConnection().then(render).catch((error) => {
      state.error = error
      render()
    })
  }

  app.innerHTML = route === "/app/orders"
    ? renderOrders()
    : route === "/app/settings"
      ? renderSettings()
      : renderOverview()
}

function renderOverview() {
  const connector = state.bridge?.connector || {}
  const inventory = state.bridge?.inventory || {}
  const orders = state.bridge?.orders || {}
  const prices = state.bridge?.prices || {}
  const bridgeActive = connector.active === true
  const setupNeeded = orders.authorized === false
  const storeName = state.config?.shop_name || state.config?.shop || "Your store"

  return `
    <section class="page">
      <div class="welcome">
        <div>
          <p class="kicker">Store overview <span aria-hidden="true">/</span> ${escapeHtml(storeName)}</p>
          <h1>${bridgeActive ? "Your store, connected." : "Let’s get you connected."}</h1>
          <p>${bridgeActive
            ? "Your connector is checking in. Follow inventory and order activity below."
            : "The Windows connector has not checked in during the last 8 minutes."}</p>
        </div>
        <div class="welcome-actions">
          ${statusBadge(bridgeActive ? "Active" : "Offline", bridgeActive ? "success" : "danger", true)}
          <button class="button secondary" type="button" data-refresh ${state.refreshing ? "disabled" : ""}>
            ${icon("refresh", state.refreshing ? "is-spinning" : "")} ${state.refreshing ? "Checking…" : "Refresh"}
          </button>
          <small>${state.bridge?.timestamp ? `Updated ${escapeHtml(relativeTime(state.bridge.timestamp))}` : "Refreshes every 30 seconds"}</small>
        </div>
      </div>

      ${state.error ? `<div class="notice danger">Status could not be loaded. ${escapeHtml(state.error.message)}</div>` : ""}
      ${setupNeeded ? `<div class="notice warning"><strong>Order access needs approval.</strong> Reopen or reinstall the Shopify app once so Shopify can approve the read_orders permission.</div>` : ""}

      <div class="status-grid">
        ${renderStatusCard({
          icon: "inventory",
          title: "Inventory bridge",
          active: inventory.active,
          activeText: "Inventory bridge is active",
          inactiveText: "No recent inventory check-in",
          detail: inventory.active
            ? `${number(inventory.queued_changes)} Shopify change${number(inventory.queued_changes) === 1 ? "" : "s"} waiting for the POS`
            : "Start the Windows scheduled task to reconnect.",
          lastSeen: connector.last_seen_at,
        })}
        ${renderStatusCard({
          icon: "orders",
          title: "Order sync",
          active: orders.active && orders.authorized,
          activeText: "Order sync is active",
          inactiveText: orders.authorized === false ? "Order access needs approval" : "No recent order check-in",
          detail: `${number(orders.queued_orders)} order${number(orders.queued_orders) === 1 ? "" : "s"} waiting to be written to the POS computer`,
          lastSeen: orders.last_poll_at,
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
        <div class="panel-footer">Latest 10 successful quantity updates <span>Refreshes automatically</span></div>
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
              <strong>SKU ${escapeHtml(change.sku)}</strong>
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

      <div class="notice neutral">
        Inventory and POS price changes are checked every 3 minutes. A full inventory reconciliation runs nightly. Order payloads are removed from Railway after the Windows connector confirms they were written.
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
