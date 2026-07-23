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
  window.scrollTo({ top: 0, behavior: "smooth" })
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
  const bridgeActive = connector.active === true
  const setupNeeded = orders.authorized === false
  const storeName = state.config?.shop_name || state.config?.shop || "Your store"

  return `
    <section class="page">
      <div class="welcome">
        <div>
          <p class="kicker">${escapeHtml(storeName)}</p>
          <h1>${bridgeActive ? "Your POS bridge is active" : "Waiting for your POS bridge"}</h1>
          <p>${bridgeActive
            ? "Inventory and Shopify orders are moving through the connector automatically."
            : "The Windows connector has not checked in during the last 8 minutes."}</p>
        </div>
        <div class="welcome-actions">
          ${statusBadge(bridgeActive ? "Active" : "Offline", bridgeActive ? "success" : "danger", true)}
          <button class="button secondary" type="button" data-refresh ${state.refreshing ? "disabled" : ""}>
            ${state.refreshing ? "Checking…" : "Refresh"}
          </button>
        </div>
      </div>

      ${state.error ? `<div class="notice danger">Status could not be loaded. ${escapeHtml(state.error.message)}</div>` : ""}
      ${setupNeeded ? `<div class="notice warning"><strong>Order access needs approval.</strong> Reopen or reinstall the Shopify app once so Shopify can approve the read_orders permission.</div>` : ""}

      <div class="status-grid">
        ${renderStatusCard({
          icon: "I",
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
          icon: "O",
          title: "Order sync",
          active: orders.active && orders.authorized,
          activeText: "Order sync is active",
          inactiveText: orders.authorized === false ? "Order access needs approval" : "No recent order check-in",
          detail: `${number(orders.queued_orders)} order${number(orders.queued_orders) === 1 ? "" : "s"} waiting to be written to the POS computer`,
          lastSeen: orders.last_poll_at,
        })}
      </div>

      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="kicker">Recent orders</p>
            <h2>Shopify → POS</h2>
          </div>
          <a class="text-link" href="/app/orders" data-route>View all recent orders</a>
        </div>
        ${renderOrderList((orders.recent || []).slice(0, 5), true)}
      </section>

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

function renderStatusCard({ icon, title, active, activeText, inactiveText, detail, lastSeen }) {
  return `
    <article class="status-card ${active ? "is-active" : "is-offline"}">
      <div class="status-card-top">
        <span class="feature-icon">${icon}</span>
        ${statusBadge(active ? "Active" : "Needs attention", active ? "success" : "danger")}
      </div>
      <div>
        <p class="kicker">${escapeHtml(title)}</p>
        <h2>${escapeHtml(active ? activeText : inactiveText)}</h2>
        <p>${escapeHtml(detail)}</p>
      </div>
      <small>${lastSeen ? `Last check-in ${escapeHtml(relativeTime(lastSeen))}` : "No check-in recorded yet"}</small>
    </article>
  `
}

function renderOrders() {
  const orders = state.bridge?.orders || {}
  const recent = orders.recent || []
  return `
    <section class="page">
      <div class="page-head">
        <div>
          <p class="kicker">Order sync</p>
          <h1>Recent Shopify orders</h1>
          <p>Only small order summaries are kept here. Full order details are delivered to <code>shopify-orders.db</code> on the POS computer.</p>
        </div>
        <button class="button secondary" type="button" data-refresh ${state.refreshing ? "disabled" : ""}>${state.refreshing ? "Checking…" : "Refresh"}</button>
      </div>

      <div class="mini-stats">
        <div><strong>${recent.length}</strong><span>Recent orders shown</span></div>
        <div><strong>${number(orders.queued_orders)}</strong><span>Waiting for POS</span></div>
        <div><strong>${state.bridge?.storage?.recent_order_limit || 50}</strong><span>Maximum summaries saved</span></div>
      </div>

      <section class="panel">
        ${renderOrderList(recent, false)}
      </section>

      <div class="notice neutral">
        Railway keeps at most ${state.bridge?.storage?.recent_order_limit || 50} lightweight summaries and removes acknowledged full order payloads from its queue. The Windows database also has a configurable recent-order limit.
      </div>
    </section>
  `
}

function renderOrderList(items, compact) {
  if (!items.length) {
    return `<div class="empty"><span>✓</span><strong>No recent orders yet</strong><p>A new Shopify order will appear here as soon as its webhook reaches the app.</p></div>`
  }

  return `
    <div class="order-list ${compact ? "compact" : ""}">
      ${items.map((order) => {
        const sent = order.delivery_status === "sent_to_pos"
        return `
          <article class="order-row">
            <div class="order-main">
              <span class="order-icon">${sent ? "✓" : "↓"}</span>
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

function renderSettings() {
  const connection = state.connection
  return `
    <section class="page narrow">
      <div class="page-head">
        <div>
          <p class="kicker">Settings</p>
          <h1>Windows connector</h1>
          <p>These values connect the computer at the store to this Shopify app.</p>
        </div>
        <button class="button secondary" type="button" data-test-shopify ${state.testing ? "disabled" : ""}>${state.testing ? "Testing…" : "Test Shopify"}</button>
      </div>

      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="kicker">Connection details</p>
            <h2>POS Bridge credentials</h2>
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
        Inventory checks in every 3 minutes. A full catalog reconciliation runs nightly. Order payloads are removed from Railway after the Windows connector confirms they were written.
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
    link.classList.toggle("active", link.getAttribute("href") === route)
  })
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
