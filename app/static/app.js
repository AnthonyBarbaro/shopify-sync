const routeTitles = {
  "/app": "Home",
  "/app/product-sync": "Product Sync",
  "/app/catalog": "Catalog",
  "/app/settings": "Settings",
}

const state = {
  config: null,
  connection: null,
  health: null,
  activity: { total: 0, items: [] },
  catalog: { total: 0, items: [] },
  feed: { total: 0, items: [] },
  requestLogs: { total: 0, items: [] },
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
    {
      name: "Classic Tee",
      sku: "ABC123",
      barcode: "012345678905",
      regular_price: "19.99",
      stock_quantity: 10,
      status: "draft",
      vendor: "POS Company",
      product_type: "Apparel",
      images: [{ src: "https://example.com/products/classic-tee.jpg" }],
    },
    {
      name: "Canvas Hat",
      sku: "DEF456",
      regular_price: "24.99",
      stock_quantity: 5,
      status: "draft",
    },
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
  await Promise.all([
    loadUiConfig(),
    loadConnection(),
    loadHealth(),
    loadActivity(),
    loadCatalog(),
    loadFeed(),
    loadRequestLogs(),
  ])
  render()

  window.setInterval(async () => {
    try {
      await Promise.all([loadHealth(), loadActivity(), loadFeed()])
      await loadRequestLogs()
      render()
    } catch (error) {
      console.error(error)
    }
  }, 20000)
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
  state.activity = await fetchJson("/api/activity?limit=8")
}

async function loadCatalog() {
  state.catalog = await fetchJson("/api/catalog?limit=25")
}

async function loadFeed() {
  state.feed = await fetchJson("/api/feed?limit=12")
}

async function loadRequestLogs() {
  state.requestLogs = await fetchJson("/api/request-logs?limit=15")
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
    title: String(formData.get("title") || "").trim(),
    sku: String(formData.get("sku") || "").trim(),
    barcode: String(formData.get("barcode") || "").trim(),
    price: coerceNumber(formData.get("price")),
    quantity: coerceInteger(formData.get("quantity")),
    image_url: String(formData.get("image_url") || "").trim(),
    vendor: String(formData.get("vendor") || "").trim(),
    product_type: String(formData.get("product_type") || "").trim(),
    status: "draft",
  }

  Object.keys(payload).forEach((key) => {
    if (payload[key] === "" || payload[key] === null || Number.isNaN(payload[key])) {
      delete payload[key]
    }
  })

  try {
    state.singleResult = {
      ok: true,
      data: await fetchJson("/api/sync/product", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    }
    await Promise.all([loadActivity(), loadCatalog(), loadFeed()])
    showToast(`Synced ${payload.sku || payload.title || "product"}.`, "success")
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
    state.bulkResult = {
      ok: true,
      data: await fetchJson("/api/sync/bulk", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      }),
    }
    await Promise.all([loadActivity(), loadCatalog(), loadFeed()])
    showToast("Bulk import finished.", "success")
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
  if (route === "/app/product-sync") return "Create draft products from POS data or update existing Shopify products by SKU."
  if (route === "/app/catalog") return "Preview what the POS-compatible API can read, and download CSV exports."
  if (route === "/app/settings") return "Copy the Woo-compatible URL, path, key, and secret into your POS."
  return "A simple control panel for POS-to-Shopify product sync."
}

function renderRoute(route) {
  if (route === "/app/product-sync") return renderProductSync()
  if (route === "/app/catalog") return renderCatalog()
  if (route === "/app/settings") return renderSettings()
  return renderHome()
}

function renderHome() {
  return `
    <section class="grid three">
      <article class="card">
        <p class="meta-label">Store</p>
        <div class="metric-value">${escapeHtml(state.config?.shop_name || state.config?.shop || "Loading")}</div>
      </article>
      <article class="card">
        <p class="meta-label">Catalog Rows</p>
        <div class="metric-value">${state.catalog?.total || 0}</div>
      </article>
      <article class="card">
        <p class="meta-label">Inbound Feed</p>
        <div class="metric-value">${state.feed?.total || 0}</div>
      </article>
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <h3>Start here</h3>
            <p>The app is set up to look like a Woo-style products API to your POS.</p>
          </div>
        </div>
        <div class="button-row">
          <a class="button" href="/app/product-sync" data-route>Open product sync</a>
          <a class="button-secondary" href="/app/catalog" data-route>Open catalog</a>
          <a class="button-ghost" href="/app/settings" data-route>View settings</a>
        </div>
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <h3>Recent syncs</h3>
            <p>Your latest sync activity shows up here.</p>
          </div>
        </div>
        ${renderActivity()}
      </article>
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <h3>Incoming requests</h3>
            <p>Every hit to the POS-facing API, including bad paths and 404s.</p>
          </div>
          <a class="button-ghost" href="/api/request-logs.csv" target="_blank" rel="noreferrer">Download request CSV</a>
        </div>
        ${renderRequestLogList()}
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <h3>Connection</h3>
            <p>These are the values you’ll copy into the POS connector.</p>
          </div>
        </div>
        ${renderCopyRow("URL", state.connection?.base_url || "")}
        ${renderCopyRow("Path", state.connection?.product_sync_path || "")}
        ${renderCopyRow("Key", state.connection?.api_key || "")}
      </article>
    </section>
  `
}

function renderProductSync() {
  return `
    <section class="grid two">
      <form class="form-card" data-single-form>
        <div class="section-head">
          <div>
            <h3>Single product</h3>
            <p>New products are created as drafts. Existing SKUs update in place.</p>
          </div>
        </div>
        <div class="form-grid">
          <div class="field">
            <label for="title">Product name</label>
            <input id="title" name="title" placeholder="Classic Tee" />
          </div>
          <div class="field">
            <label for="sku">SKU</label>
            <input id="sku" name="sku" placeholder="ABC123" />
          </div>
          <div class="field">
            <label for="barcode">Barcode</label>
            <input id="barcode" name="barcode" placeholder="012345678905" />
          </div>
          <div class="field">
            <label for="price">Price</label>
            <input id="price" name="price" type="number" step="0.01" min="0" placeholder="19.99" />
          </div>
          <div class="field">
            <label for="quantity">Quantity</label>
            <input id="quantity" name="quantity" type="number" step="1" min="0" placeholder="10" />
          </div>
          <div class="field">
            <label for="image_url">Image URL</label>
            <input id="image_url" name="image_url" placeholder="https://example.com/products/classic-tee.jpg" />
          </div>
          <div class="field">
            <label for="vendor">Vendor</label>
            <input id="vendor" name="vendor" placeholder="POS Company" />
          </div>
          <div class="field">
            <label for="product_type">Product type</label>
            <input id="product_type" name="product_type" placeholder="Apparel" />
          </div>
          <div class="button-row">
            <button class="button" type="submit" ${state.isSubmittingSingle ? "disabled" : ""}>
              ${state.isSubmittingSingle ? "Syncing..." : "Sync product"}
            </button>
            <a class="button-ghost" href="/app/settings" data-route>View settings</a>
          </div>
        </div>
      </form>

      <div class="stack">
        ${renderSingleResult()}
        ${renderMiniConnectionCard(state.connection?.product_sync_path || "", state.connection?.bulk_sync_path || "")}
      </div>
    </section>

    <section class="grid two">
      <form class="form-card" data-bulk-form>
        <div class="section-head">
          <div>
            <h3>Bulk import JSON</h3>
            <p>Paste an array from the POS. Woo-style fields work here too.</p>
          </div>
        </div>
        <div class="form-grid">
          <div class="field">
            <label for="payload">Bulk JSON</label>
            <textarea id="payload" name="payload" spellcheck="false">${escapeHtml(sampleBulkPayload)}</textarea>
          </div>
          <div class="button-row">
            <button class="button-secondary" type="submit" ${state.isSubmittingBulk ? "disabled" : ""}>
              ${state.isSubmittingBulk ? "Processing..." : "Run bulk import"}
            </button>
          </div>
        </div>
      </form>

      <div class="stack">
        ${renderBulkResult()}
        <article class="card">
          <div class="section-head">
            <div>
              <h3>What happens</h3>
              <p>The backend looks up Shopify by SKU, updates matches, and creates missing products as drafts.</p>
            </div>
          </div>
          <div class="setting-row">
            <strong>Images</strong>
            <span>Public image URLs are attached during sync.</span>
          </div>
          <div class="setting-row">
            <strong>Inventory</strong>
            <span>Quantity is set after the product or variant is ready.</span>
          </div>
          <div class="setting-row">
            <strong>Feed logging</strong>
            <span>Each external sync is saved for CSV export.</span>
          </div>
        </article>
      </div>
    </section>
  `
}

function renderCatalog() {
  return `
    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <h3>Catalog export</h3>
            <p>This is the current Shopify product data your POS-compatible API can expose.</p>
          </div>
          <a class="button" href="/api/catalog.csv" target="_blank" rel="noreferrer">Download catalog CSV</a>
        </div>
        ${renderCatalogTable()}
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <h3>Inbound feed log</h3>
            <p>These rows show what came into the POS-facing API.</p>
          </div>
          <a class="button-secondary" href="/api/feed.csv" target="_blank" rel="noreferrer">Download feed CSV</a>
        </div>
        ${renderFeedTable()}
      </article>
    </section>

    <section class="grid one">
      <article class="card">
        <div class="section-head">
          <div>
            <h3>Request log</h3>
            <p>Use this when the POS says 404 or sends a malformed path.</p>
          </div>
          <a class="button-ghost" href="/api/request-logs.csv" target="_blank" rel="noreferrer">Download request CSV</a>
        </div>
        ${renderRequestLogTable()}
      </article>
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
            <h3>POS connection</h3>
            <p>Use the Woo-compatible path first. Your POS can call this as a products API.</p>
          </div>
          <div class="button-row">
            <button class="button-ghost" type="button" data-copy="${escapeAttribute(buildSimpleSettingsText())}">Copy all</button>
            <button class="button-secondary" type="button" data-rotate-secret ${state.isRotatingCredentials ? "disabled" : ""}>
              ${state.isRotatingCredentials ? "Generating..." : "New key + secret"}
            </button>
          </div>
        </div>
        ${state.connection?.secret_is_temporary ? `<div class="pill success">Copy this secret now. It will be hidden after you reload the page.</div>` : `<div class="pill warning">For Woo-style signed requests, rotate old credentials once so the new secret is stored in the updated format.</div>`}
        ${renderCopyRow("URL", state.connection?.base_url || "")}
        ${renderCopyRow("Path", state.connection?.product_sync_path || "")}
        ${renderCopyRow("Batch Path", state.connection?.bulk_sync_path || "")}
        ${renderCopyRow("Key", state.connection?.api_key || "")}
        ${renderCopyRow("Secret", visibleSecret)}
      </article>

      <div class="stack">
        <article class="card">
          <div class="section-head">
            <div>
              <h3>Simple checks</h3>
              <p>Make sure the store and connector are pointing at the same place.</p>
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
            <strong>Auth</strong>
            <span>consumer_key / consumer_secret or signed oauth query</span>
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
        <p class="empty-state">Run a product sync and the result will appear here.</p>
      </article>
    `
  }

  if (state.singleResult.ok) {
    const result = state.singleResult.data
    return renderResultCard("Last product sync", true, [
      ["Product", result.details?.product_title || result.sku],
      ["SKU", result.sku],
      ["Status", result.details?.product_status || "Unknown"],
      ["Price", result.price ?? "—"],
      ["Quantity", result.quantity ?? "—"],
      ["Message", result.message],
    ])
  }

  return renderResultCard("Last product sync", false, [
    ["Message", state.singleResult.error.message],
    ["Code", state.singleResult.error.code || "request_failed"],
  ])
}

function renderBulkResult() {
  if (!state.bulkResult) {
    return `
      <article class="card">
        <p class="empty-state">Run a bulk import and the summary will appear here.</p>
      </article>
    `
  }

  if (state.bulkResult.ok) {
    const result = state.bulkResult.data
    return renderResultCard("Last bulk import", true, [
      ["Total", result.total],
      ["Succeeded", result.succeeded],
      ["Failed", result.failed],
      ["Timestamp", formatDate(result.timestamp)],
    ])
  }

  return renderResultCard("Last bulk import", false, [
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

function renderMiniConnectionCard(path, batchPath) {
  return `
    <article class="card">
      <div class="section-head">
        <div>
          <h3>Connection</h3>
          <p>The app uses the same paths your POS will call.</p>
        </div>
      </div>
      <div class="setting-row">
        <strong>URL</strong>
        <span>${escapeHtml(state.connection?.base_url || "")}</span>
      </div>
      <div class="setting-row">
        <strong>Products</strong>
        <span>${escapeHtml(path || "")}</span>
      </div>
      <div class="setting-row">
        <strong>Batch</strong>
        <span>${escapeHtml(batchPath || "")}</span>
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
            <strong>${escapeHtml(item.details?.product_title || item.sku)}</strong>
            <span class="meta-value muted">${escapeHtml(formatDate(item.timestamp))}</span>
          </div>
          <span class="meta-value muted">${escapeHtml(item.message)}</span>
        </li>
      `).join("")}
    </ul>
  `
}

function renderFeedList() {
  const items = state.feed?.items || []
  if (!items.length) {
    return `<p class="empty-state">No external requests yet.</p>`
  }

  return `
    <ul class="activity-list">
      ${items.map((item) => `
        <li class="activity-item">
          <div class="activity-title">
            <span class="status-dot ${item.success ? "success" : "error"}"></span>
            <strong>${escapeHtml(item.title || item.sku || "Product request")}</strong>
            <span class="meta-value muted">${escapeHtml(formatDate(item.received_at))}</span>
          </div>
          <span class="meta-value muted">${escapeHtml(`${item.method} ${item.endpoint}`)}</span>
        </li>
      `).join("")}
    </ul>
  `
}

function renderRequestLogList() {
  const items = state.requestLogs?.items || []
  if (!items.length) {
    return `<p class="empty-state">No incoming requests logged yet.</p>`
  }

  return `
    <ul class="activity-list">
      ${items.map((item) => `
        <li class="activity-item">
          <div class="activity-title">
            <span class="status-dot ${item.status_code >= 400 ? "error" : "success"}"></span>
            <strong>${escapeHtml(`${item.method} ${item.path}`)}</strong>
            <span class="meta-value muted">${escapeHtml(formatDate(item.created_at))}</span>
          </div>
          <span class="meta-value muted">${escapeHtml(`status ${item.status_code}${item.query_string ? ` • ${item.query_string}` : ""}`)}</span>
        </li>
      `).join("")}
    </ul>
  `
}

function renderCatalogTable() {
  const items = state.catalog?.items || []
  if (!items.length) {
    return `<p class="empty-state">No catalog rows available yet.</p>`
  }

  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>Product</th>
            <th>SKU</th>
            <th>Status</th>
            <th>Price</th>
            <th>Qty</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>${escapeHtml(item.title)}</td>
              <td>${escapeHtml(item.sku || "—")}</td>
              <td>${escapeHtml(item.status || "—")}</td>
              <td>${escapeHtml(item.price ?? "—")}</td>
              <td>${escapeHtml(item.quantity ?? "—")}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `
}

function renderFeedTable() {
  const items = state.feed?.items || []
  if (!items.length) {
    return `<p class="empty-state">No feed rows available yet.</p>`
  }

  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>When</th>
            <th>Source</th>
            <th>SKU</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>${escapeHtml(formatDate(item.received_at))}</td>
              <td>${escapeHtml(item.source)}</td>
              <td>${escapeHtml(item.sku || "—")}</td>
              <td>${escapeHtml(item.message)}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `
}

function renderRequestLogTable() {
  const items = state.requestLogs?.items || []
  if (!items.length) {
    return `<p class="empty-state">No request rows available yet.</p>`
  }

  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>When</th>
            <th>Status</th>
            <th>Method</th>
            <th>Path</th>
            <th>Query</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>${escapeHtml(formatDate(item.created_at))}</td>
              <td>${escapeHtml(String(item.status_code))}</td>
              <td>${escapeHtml(item.method)}</td>
              <td>${escapeHtml(item.path)}</td>
              <td>${escapeHtml(item.query_string || "—")}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
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
    `Batch Path: ${state.connection?.bulk_sync_path || ""}`,
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
  if (pathname === "/app/bulk-sync") return "/app/product-sync"
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

function coerceNumber(value) {
  if (value === null || value === "") return null
  return Number(value)
}

function coerceInteger(value) {
  if (value === null || value === "") return null
  return Number.parseInt(String(value), 10)
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
