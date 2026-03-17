const routeTitles = {
  "/app": "Home",
  "/app/product-sync": "Single Sync",
  "/app/bulk-sync": "Bulk Sync",
  "/app/settings": "Settings",
}

const state = {
  config: null,
  connection: null,
  health: null,
  activity: { total: 0, items: [] },
  shopifyHealth: null,
  singleResult: null,
  bulkResult: null,
  isSubmittingSingle: false,
  isSubmittingBulk: false,
  isRotatingCredentials: false,
  isTestingShopify: false,
}

const sampleBulkPayload = JSON.stringify(
  [
    { sku: "ABC123", price: 19.99, quantity: 10 },
    { sku: "DEF456", price: 24.99, quantity: 5 },
  ],
  null,
  2
)

document.addEventListener("DOMContentLoaded", () => {
  boot().catch((error) => {
    console.error(error)
    showToast(error.message || "Failed to load the UI.", "error")
  })
})

window.addEventListener("popstate", () => render())

document.addEventListener("click", async (event) => {
  const routeLink = event.target.closest("[data-route]")
  if (routeLink) {
    event.preventDefault()
    navigate(routeLink.getAttribute("href"))
    return
  }

  const testButton = event.target.closest("[data-test-shopify]")
  if (testButton) {
    event.preventDefault()
    await testShopify()
    return
  }

  const rotateButton = event.target.closest("[data-rotate-secret]")
  if (rotateButton) {
    event.preventDefault()
    await rotateCredentials()
    return
  }

  const copyButton = event.target.closest("[data-copy]")
  if (copyButton) {
    event.preventDefault()
    await copyValue(copyButton.getAttribute("data-copy"))
  }
})

document.addEventListener("submit", async (event) => {
  const form = event.target

  if (form.matches("[data-single-form]")) {
    event.preventDefault()
    await submitSingle(form)
    return
  }

  if (form.matches("[data-bulk-form]")) {
    event.preventDefault()
    await submitBulk(form)
  }
})

async function boot() {
  await Promise.all([loadUiConfig(), loadConnection(), loadHealth(), loadActivity()])
  render()

  window.setInterval(async () => {
    try {
      await Promise.all([loadHealth(), loadActivity()])
      render()
    } catch (error) {
      console.error(error)
    }
  }, 15000)
}

async function loadUiConfig() {
  state.config = await fetchJson("/api/ui/config")
}

async function loadConnection() {
  state.connection = await fetchJson("/api/connection-settings")
}

async function loadHealth() {
  state.health = await fetchJson("/health")
}

async function loadActivity() {
  state.activity = await fetchJson("/api/activity?limit=10")
}

function navigate(path) {
  window.history.pushState({}, "", `${path}${window.location.search}`)
  render()
  window.scrollTo({ top: 0, behavior: "smooth" })
}

async function testShopify() {
  state.isTestingShopify = true
  render()

  try {
    state.shopifyHealth = await fetchJson("/health/shopify")
    showToast("Shopify connection looks good.", "success")
  } catch (error) {
    state.shopifyHealth = { status: "error", message: error.message, details: error.details || {} }
    showToast(error.message || "Shopify test failed.", "error")
  } finally {
    state.isTestingShopify = false
    render()
  }
}

async function submitSingle(form) {
  state.isSubmittingSingle = true
  render()

  const formData = new FormData(form)
  const payload = {
    sku: String(formData.get("sku") || "").trim(),
    price: Number(formData.get("price")),
    quantity: Number(formData.get("quantity")),
  }

  try {
    state.singleResult = {
      ok: true,
      data: await fetchJson("/api/sync/product", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    }
    await loadActivity()
    showToast(`Synced ${payload.sku}.`, "success")
  } catch (error) {
    state.singleResult = { ok: false, error: normalizeError(error) }
    showToast(error.message || "Sync failed.", "error")
  } finally {
    state.isSubmittingSingle = false
    render()
  }
}

async function submitBulk(form) {
  state.isSubmittingBulk = true
  render()

  const formData = new FormData(form)
  const rawPayload = String(formData.get("payload") || "").trim()

  try {
    const parsed = JSON.parse(rawPayload)
    if (!Array.isArray(parsed)) {
      throw new Error("Bulk payload must be a JSON array.")
    }

    state.bulkResult = {
      ok: true,
      data: await fetchJson("/api/sync/bulk", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      }),
    }
    await loadActivity()
    showToast("Bulk sync finished.", "success")
  } catch (error) {
    state.bulkResult = { ok: false, error: normalizeError(error) }
    showToast(error.message || "Bulk sync failed.", "error")
  } finally {
    state.isSubmittingBulk = false
    render()
  }
}

async function rotateCredentials() {
  state.isRotatingCredentials = true
  render()

  try {
    state.connection = await fetchJson("/api/connection-settings/rotate", {
      method: "POST",
    })
    showToast("Generated a new key and secret.", "success")
  } catch (error) {
    showToast(error.message || "Could not rotate credentials.", "error")
  } finally {
    state.isRotatingCredentials = false
    render()
  }
}

function render() {
  const app = document.getElementById("app")
  if (!app) return

  const route = normalizeRoute(window.location.pathname)
  document.title = `${routeTitles[route]} | Inventory Sync`
  updateTabs(route)

  app.innerHTML = `
    <div class="page">
      ${renderHero(route)}
      ${renderRoute(route)}
    </div>
  `
}

function renderHero(route) {
  const embedded = isEmbeddedContext()
  return `
    <section class="hero">
      <div class="hero-row">
        <div>
          <h2>${routeTitles[route]}</h2>
          <p>${routeDescription(route)}</p>
        </div>
      </div>
      <div class="pill-row">
        <span class="pill ${state.health?.status === "ok" ? "success" : "danger"}">API ${state.health?.status === "ok" ? "ready" : "error"}</span>
        <span class="pill">${escapeHtml(state.config?.shop_name || state.config?.shop || "Loading store")}</span>
        <span class="pill ${embedded ? "success" : "warning"}">${embedded ? "Inside Shopify" : "Browser preview"}</span>
      </div>
    </section>
  `
}

function routeDescription(route) {
  if (route === "/app/product-sync") return "Update one SKU."
  if (route === "/app/bulk-sync") return "Paste a batch and send it."
  if (route === "/app/settings") return "Copy the connection details your other system needs."
  return "A simple place to sync inventory and grab your connector settings."
}

function renderRoute(route) {
  if (route === "/app/product-sync") return renderSingleSync()
  if (route === "/app/bulk-sync") return renderBulkSync()
  if (route === "/app/settings") return renderSettings()
  return renderHome()
}

function renderHome() {
  return `
    <section class="grid three">
      <article class="card">
        <p class="meta-label">Shop</p>
        <div class="metric-value">${escapeHtml(state.config?.shop_name || state.config?.shop || "Loading")}</div>
      </article>
      <article class="card">
        <p class="meta-label">Sync URL</p>
        <div class="metric-value">${escapeHtml(state.connection?.base_url || "Loading")}</div>
      </article>
      <article class="card">
        <p class="meta-label">Recent events</p>
        <div class="metric-value">${state.activity?.total || 0}</div>
      </article>
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <h3>Start here</h3>
            <p>Choose what you want to do.</p>
          </div>
        </div>
        <div class="button-row">
          <a class="button" href="/app/product-sync" data-route>Single sync</a>
          <a class="button-secondary" href="/app/bulk-sync" data-route>Bulk sync</a>
          <a class="button-ghost" href="/app/settings" data-route>Connection settings</a>
        </div>
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <h3>Recent syncs</h3>
            <p>Your newest activity shows up here.</p>
          </div>
        </div>
        ${renderActivity()}
      </article>
    </section>
  `
}

function renderSingleSync() {
  return `
    <section class="grid two">
      <form class="form-card" data-single-form>
        <div class="section-head">
          <div>
            <h3>Single product sync</h3>
            <p>Enter a SKU, price, and quantity.</p>
          </div>
        </div>
        <div class="form-grid">
          <div class="field">
            <label for="sku">SKU</label>
            <input id="sku" name="sku" placeholder="ABC123" required />
          </div>
          <div class="field">
            <label for="price">Price</label>
            <input id="price" name="price" type="number" step="0.01" min="0" placeholder="19.99" required />
          </div>
          <div class="field">
            <label for="quantity">Quantity</label>
            <input id="quantity" name="quantity" type="number" step="1" min="0" placeholder="10" required />
          </div>
          <div class="button-row">
            <button class="button" type="submit" ${state.isSubmittingSingle ? "disabled" : ""}>
              ${state.isSubmittingSingle ? "Syncing..." : "Run sync"}
            </button>
            <a class="button-ghost" href="/app/settings" data-route>View settings</a>
          </div>
        </div>
      </form>

      <div class="stack">
        ${renderSingleResult()}
        ${renderMiniConnectionCard(state.connection?.product_sync_path || "")}
      </div>
    </section>
  `
}

function renderBulkSync() {
  return `
    <section class="grid two">
      <form class="form-card" data-bulk-form>
        <div class="section-head">
          <div>
            <h3>Bulk sync</h3>
            <p>Paste a JSON array from your POS.</p>
          </div>
        </div>
        <div class="form-grid">
          <div class="field">
            <label for="payload">Bulk JSON</label>
            <textarea id="payload" name="payload" spellcheck="false">${escapeHtml(sampleBulkPayload)}</textarea>
          </div>
          <div class="button-row">
            <button class="button-secondary" type="submit" ${state.isSubmittingBulk ? "disabled" : ""}>
              ${state.isSubmittingBulk ? "Processing..." : "Run bulk sync"}
            </button>
            <a class="button-ghost" href="/app/settings" data-route>View settings</a>
          </div>
        </div>
      </form>

      <div class="stack">
        ${renderBulkResult()}
        ${renderMiniConnectionCard(state.connection?.bulk_sync_path || "")}
      </div>
    </section>
  `
}

function renderSettings() {
  const visibleSecret = state.connection?.api_secret || state.connection?.api_secret_masked || ""

  return `
    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <h3>Connection settings</h3>
            <p>Copy these directly into your POS or external connector.</p>
          </div>
          <div class="button-row">
            <button class="button-ghost" type="button" data-copy="${escapeAttribute(buildSimpleSettingsText())}">Copy all</button>
            <button class="button-secondary" type="button" data-rotate-secret ${state.isRotatingCredentials ? "disabled" : ""}>
              ${state.isRotatingCredentials ? "Generating..." : "New key + secret"}
            </button>
          </div>
        </div>
        ${state.connection?.secret_is_temporary ? `<div class="pill success">Copy this secret now. It will be hidden after you reload the page.</div>` : `<div class="pill warning">The secret is masked after first display. Rotate it any time if you need a new one.</div>`}
        ${renderCopyRow("URL", state.connection?.base_url || "")}
        ${renderCopyRow("Path", state.connection?.product_sync_path || "")}
        ${renderCopyRow("Key", state.connection?.api_key || "")}
        ${renderCopyRow("Secret", visibleSecret)}
      </article>

      <div class="stack">
        <article class="card">
          <div class="section-head">
            <div>
              <h3>Simple checks</h3>
              <p>Just enough to confirm everything is pointed to the right place.</p>
            </div>
            <button class="button" type="button" data-test-shopify ${state.isTestingShopify ? "disabled" : ""}>
              ${state.isTestingShopify ? "Testing..." : "Test Shopify"}
            </button>
          </div>
          <div class="setting-row">
            <strong>Shop</strong>
            <span>${escapeHtml(state.config?.shop_name || state.config?.shop || "")}</span>
          </div>
          <div class="setting-row">
            <strong>API Version</strong>
            <span>${escapeHtml(state.config?.api_version || "")}</span>
          </div>
          <div class="setting-row">
            <strong>Last Used</strong>
            <span>${escapeHtml(state.connection?.last_used_at ? formatDate(state.connection.last_used_at) : "Not used yet")}</span>
          </div>
        </article>
        ${renderShopifyResult()}
      </div>
    </section>
  `
}

function renderSingleResult() {
  if (!state.singleResult) {
    return `
      <article class="card">
        <p class="empty-state">Run a sync and the result will appear here.</p>
      </article>
    `
  }

  if (state.singleResult.ok) {
    const result = state.singleResult.data
    return renderResultCard("Last result", true, [
      ["SKU", result.sku],
      ["Price", result.price],
      ["Quantity", result.quantity],
      ["Message", result.message],
    ])
  }

  return renderResultCard("Last result", false, [
    ["Message", state.singleResult.error.message],
    ["Code", state.singleResult.error.code || "request_failed"],
  ])
}

function renderBulkResult() {
  if (!state.bulkResult) {
    return `
      <article class="card">
        <p class="empty-state">Run a bulk sync and the summary will appear here.</p>
      </article>
    `
  }

  if (state.bulkResult.ok) {
    const result = state.bulkResult.data
    return renderResultCard("Last batch", true, [
      ["Total", result.total],
      ["Succeeded", result.succeeded],
      ["Failed", result.failed],
      ["Timestamp", formatDate(result.timestamp)],
    ])
  }

  return renderResultCard("Last batch", false, [
    ["Message", state.bulkResult.error.message],
    ["Code", state.bulkResult.error.code || "request_failed"],
  ])
}

function renderShopifyResult() {
  if (!state.shopifyHealth) {
    return `
      <article class="card">
        <p class="empty-state">Click “Test Shopify” if you want to confirm the store connection.</p>
      </article>
    `
  }

  const ok = state.shopifyHealth.status === "ok"
  return renderResultCard("Shopify test", ok, [
    ["Message", state.shopifyHealth.message],
    ["Shop", state.shopifyHealth.shop_name || state.shopifyHealth.shop || "Unavailable"],
    ["Domain", state.shopifyHealth.myshopify_domain || "Unavailable"],
  ])
}

function renderMiniConnectionCard(path) {
  return `
    <article class="card">
      <div class="section-head">
        <div>
          <h3>Connection</h3>
          <p>This page uses these settings automatically.</p>
        </div>
      </div>
      <div class="setting-row">
        <strong>URL</strong>
        <span>${escapeHtml(state.connection?.base_url || "")}</span>
      </div>
      <div class="setting-row">
        <strong>Path</strong>
        <span>${escapeHtml(path || "")}</span>
      </div>
    </article>
  `
}

function renderResultCard(title, success, rows) {
  return `
    <article class="result-card ${success ? "success" : "error"}">
      <div class="section-head">
        <div>
          <h3>${escapeHtml(title)}</h3>
          <p>${success ? "Success" : "Needs attention"}</p>
        </div>
        <span class="pill ${success ? "success" : "danger"}">${success ? "OK" : "Error"}</span>
      </div>
      <div class="result-grid">
        ${rows.map(([label, value]) => `
          <div class="setting-row">
            <strong>${escapeHtml(String(label))}</strong>
            <span>${escapeHtml(String(value ?? "—"))}</span>
          </div>
        `).join("")}
      </div>
    </article>
  `
}

function renderActivity() {
  const items = state.activity?.items || []
  if (!items.length) {
    return `<p class="empty-state">No syncs yet.</p>`
  }

  return `
    <ul class="activity-list">
      ${items.map((item) => `
        <li class="activity-item">
          <div class="activity-title">
            <span class="status-dot ${item.success ? "success" : "error"}"></span>
            <strong>${escapeHtml(item.sku)}</strong>
            <span class="meta-value muted">${escapeHtml(formatDate(item.timestamp))}</span>
          </div>
          <span class="meta-value muted">${escapeHtml(item.message)}</span>
        </li>
      `).join("")}
    </ul>
  `
}

function renderCopyRow(label, value) {
  return `
    <div class="copy-row">
      <div>
        <div class="meta-label">${escapeHtml(label)}</div>
        <code>${escapeHtml(value)}</code>
      </div>
      <button class="copy-button" type="button" data-copy="${escapeAttribute(value)}">Copy</button>
    </div>
  `
}

function buildSimpleSettingsText() {
  return [
    `Shop: ${state.connection?.shop || ""}`,
    `URL: ${state.connection?.base_url || ""}`,
    `Path: ${state.connection?.product_sync_path || ""}`,
    `Key: ${state.connection?.api_key || ""}`,
    `Secret: ${state.connection?.api_secret || state.connection?.api_secret_masked || ""}`,
  ].join("\n")
}

async function copyValue(value) {
  try {
    await navigator.clipboard.writeText(value)
    showToast("Copied.", "success")
  } catch (_error) {
    showToast("Copy failed.", "error")
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options)
  let payload = null

  try {
    payload = await response.json()
  } catch (_error) {
    payload = null
  }

  if (!response.ok) {
    const errorPayload = payload?.error || {}
    const error = new Error(errorPayload.message || "Request failed.")
    error.code = errorPayload.code
    error.details = errorPayload.details || {}
    throw error
  }

  return payload
}

function updateTabs(route) {
  document.querySelectorAll(".tabs a").forEach((link) => {
    link.classList.toggle("active", link.getAttribute("href") === route)
  })
}

function normalizeRoute(pathname) {
  return routeTitles[pathname] ? pathname : "/app"
}

function normalizeError(error) {
  if (error instanceof Error) {
    return {
      message: error.message,
      code: error.code || null,
      details: error.details || {},
    }
  }

  return { message: "Request failed.", code: null, details: {} }
}

function formatDate(value) {
  try {
    return new Date(value).toLocaleString()
  } catch (_error) {
    return String(value)
  }
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
}

function escapeAttribute(value) {
  return escapeHtml(value)
}

function showToast(message, tone = "info") {
  const region = document.getElementById("toast-region")
  if (!region) return

  const toast = document.createElement("div")
  toast.className = `toast ${tone}`
  toast.textContent = message
  region.appendChild(toast)

  window.setTimeout(() => toast.remove(), 2800)
}

function isEmbeddedContext() {
  const params = new URLSearchParams(window.location.search)
  return params.has("host") || params.has("shop")
}
