const routeTitles = {
  "/app": "Overview",
  "/app/product-sync": "Product Sync",
  "/app/pos-archive": "POS Archive",
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
  posArchive: null,
  posPreview: null,
  posSyncResult: null,
  shopifyHealth: null,
  singleResult: null,
  bulkResult: null,
  loadErrors: {},
  isSubmittingSingle: false,
  isSubmittingBulk: false,
  isRotatingCredentials: false,
  isTestingShopify: false,
  isAnalyzingArchive: false,
  isUploadingArchive: false,
  isSyncingArchive: false,
}

const sampleBulkPayload = JSON.stringify(
  [
    {
      name: "Classic Tee",
      sku: "ABC123",
      barcode: "012345678905",
      regular_price: "19.99",
      cost: "8.50",
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
      cost: "11.25",
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
    return
  }

  const analyzeArchiveButton = event.target.closest("[data-analyze-pos-archive]")
  if (analyzeArchiveButton) {
    event.preventDefault()
    await loadPosArchive()
    return
  }

  const previewArchiveButton = event.target.closest("[data-preview-pos-products]")
  if (previewArchiveButton) {
    event.preventDefault()
    await loadPosProductPreview()
    return
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
    return
  }

  if (form.matches("[data-pos-upload-form]")) {
    event.preventDefault()
    await uploadPosArchive(form)
    return
  }

  if (form.matches("[data-pos-sync-form]")) {
    event.preventDefault()
    await syncPosProducts(form)
  }
})

async function boot() {
  await loadInitialData()
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

async function loadInitialData() {
  const loaders = [
    ["config", loadUiConfig],
    ["connection", loadConnection],
    ["health", loadHealth],
    ["activity", loadActivity],
    ["catalog", loadCatalog],
    ["feed", loadFeed],
    ["requestLogs", loadRequestLogs],
  ]

  await Promise.all(loaders.map(([key, loader]) => loadOptional(key, loader)))
}

async function loadOptional(key, loader) {
  try {
    await loader()
    delete state.loadErrors[key]
  } catch (error) {
    state.loadErrors[key] = normalizeError(error)
    console.error(`${key} load failed`, error)
  }
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
    cost: coerceNumber(formData.get("cost")),
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

async function loadPosArchive() {
  state.isAnalyzingArchive = true
  render()

  try {
    state.posArchive = await fetchJson("/api/pos-archive/analyze")
    state.posPreview = {
      total: state.posArchive.product_preview?.length || 0,
      items: state.posArchive.product_preview || [],
    }
    showToast("POS archive analyzed.", "success")
  } catch (error) {
    showToast(error.message || "POS archive analysis failed.", "error")
  } finally {
    state.isAnalyzingArchive = false
    render()
  }
}

async function uploadPosArchive(form) {
  state.isUploadingArchive = true
  render()

  try {
    const formData = new FormData(form)
    const result = await fetchJson("/api/pos-archive/upload", {
      method: "POST",
      body: formData,
    })
    state.posArchive = result.analysis
    state.posPreview = {
      total: result.analysis.product_preview?.length || 0,
      items: result.analysis.product_preview || [],
    }
    showToast("POS archive uploaded and analyzed.", "success")
  } catch (error) {
    showToast(error.message || "Archive upload failed.", "error")
  } finally {
    state.isUploadingArchive = false
    render()
  }
}

async function loadPosProductPreview() {
  state.isAnalyzingArchive = true
  render()

  try {
    state.posPreview = await fetchJson("/api/pos-archive/products/preview?limit=25")
    showToast("Loaded product preview.", "success")
  } catch (error) {
    showToast(error.message || "Product preview failed.", "error")
  } finally {
    state.isAnalyzingArchive = false
    render()
  }
}

async function syncPosProducts(form) {
  const formData = new FormData(form)
  const limit = Math.min(Math.max(coerceInteger(formData.get("limit")) || 25, 1), 250)
  const offset = Math.max(coerceInteger(formData.get("offset")) || 0, 0)
  const includeZeroQuantity = formData.get("include_zero_quantity") === "on"
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    include_zero_quantity: includeZeroQuantity ? "true" : "false",
  })
  const confirmed = window.confirm(`Sync ${limit} POS products from offset ${offset} to Shopify by SKU? New products will be drafts.`)
  if (!confirmed) return

  state.isSyncingArchive = true
  render()

  try {
    state.posSyncResult = await fetchJson(`/api/pos-archive/products/sync?${params.toString()}`, {
      method: "POST",
    })
    await Promise.all([loadActivity(), loadCatalog(), loadFeed()])
    showToast("POS product sync finished.", "success")
  } catch (error) {
    state.posSyncResult = { error: normalizeError(error) }
    showToast(error.message || "POS product sync failed.", "error")
  } finally {
    state.isSyncingArchive = false
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
  const latestSync = getLatestActivity()
  const latestRequest = getLatestRequest()
  const requestIssues = getFailedRequestCount()
  const storeName = state.config?.shop_name || state.config?.shop || "Loading store"
  const apiReady = state.health?.status === "ok"

  return `
    <section class="hero">
      <div class="hero-copy">
        <div>
          <p class="eyebrow">Shopify Inventory Sync</p>
          <h2>${routeTitles[route]}</h2>
          <p>${routeDescription(route)}</p>
        </div>
        <div class="pill-row">
          <span class="pill ${apiReady ? "success" : state.loadErrors.health ? "danger" : "warning"}">API ${apiReady ? "ready" : state.loadErrors.health ? "error" : "loading"}</span>
          <span class="pill">${escapeHtml(storeName)}</span>
          <span class="pill ${embedded ? "success" : "warning"}">${embedded ? "Inside Shopify" : "Browser preview"}</span>
        </div>
      </div>
      <div class="hero-aside">
        ${renderMetricTile("Store", storeName, state.config?.shop || "Shop context")}
        ${renderMetricTile("Catalog rows", state.catalog?.total || 0, "Current Shopify snapshot")}
        ${renderMetricTile("Requests logged", state.requestLogs?.total || 0, requestIssues ? `${requestIssues} with issues` : "No recent 4xx or 5xx", requestIssues ? "warning" : "success")}
        ${renderMetricTile("Last sync", latestSync ? formatShortDate(latestSync.timestamp) : "No sync yet", latestSync ? (latestSync.details?.product_title || latestSync.sku || latestSync.message) : "Waiting for activity", latestSync && !latestSync.success ? "warning" : latestRequest && latestRequest.status_code >= 400 ? "danger" : "neutral")}
      </div>
    </section>
  `
}

function routeDescription(route) {
  if (route === "/app/product-sync") return "Create draft products from POS payloads or update matching Shopify SKUs with price, quantity, barcode, and image data."
  if (route === "/app/pos-archive") return "Upload or inspect the old POS DBF archive, preview product data, and sync Shopify-ready rows when you are ready."
  if (route === "/app/catalog") return "Inspect the Shopify snapshot your POS can read, compare inbound feed rows, and export clean CSVs."
  if (route === "/app/settings") return "Keep your connector values clean, stable, and easy for the POS team to copy without guesswork."
  return "Monitor connector health, incoming traffic, and the Shopify catalog from one compact operations view."
}

function renderRoute(route) {
  if (route === "/app/product-sync") return renderProductSync()
  if (route === "/app/pos-archive") return renderPosArchive()
  if (route === "/app/catalog") return renderCatalog()
  if (route === "/app/settings") return renderSettings()
  return renderHome()
}

function renderHome() {
  const latestRequest = getLatestRequest()
  const latestSync = getLatestActivity()
  const failedRequests = getFailedRequestCount()

  return `
    <section class="dashboard-grid">
      <div class="stack">
        <article class="card card-accent">
          <div class="section-head">
            <div>
              <div class="section-kicker">Workflow</div>
              <h3>Run the connector like a monitored service</h3>
              <p>Keep the POS pointed at one clean Woo-style endpoint, watch request traffic, and verify what is landing in Shopify.</p>
            </div>
          </div>
          <div class="action-grid">
            <a class="button" href="/app/product-sync" data-route>Open product sync</a>
            <a class="button-secondary" href="/app/catalog" data-route>Review catalog</a>
            <a class="button-ghost" href="/app/settings" data-route>Open settings</a>
          </div>
        </article>

        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Activity</div>
              <h3>Recent syncs</h3>
              <p>Use this to confirm the last product updates that made it through to Shopify.</p>
            </div>
          </div>
          ${renderActivity()}
        </article>

        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Traffic</div>
              <h3>Incoming requests</h3>
              <p>Every hit to the POS-facing API, including malformed paths and 404s, shows up here.</p>
            </div>
            <a class="button-ghost" href="/api/request-logs.csv" target="_blank" rel="noreferrer">Download request CSV</a>
          </div>
          ${renderRequestLogList()}
        </article>
      </div>

      <aside class="stack">
        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Connector</div>
              <h3>POS connection summary</h3>
              <p>The values your POS team needs most often are surfaced here without the full settings page.</p>
            </div>
            <a class="button-ghost" href="/app/settings" data-route>Full settings</a>
          </div>
          <div class="copy-stack">
            ${renderCopyRow("URL", state.connection?.base_url || "")}
            ${renderCopyRow("Path", state.connection?.product_sync_path || "")}
            ${renderCopyRow("Key", state.connection?.api_key || "")}
          </div>
        </article>

        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Watchlist</div>
              <h3>What needs attention</h3>
              <p>A compact view of sync traffic, feed volume, and credential usage.</p>
            </div>
          </div>
          <div class="setting-row">
            <strong>Last sync</strong>
            <span>${escapeHtml(latestSync ? `${formatShortDate(latestSync.timestamp)} • ${latestSync.details?.product_title || latestSync.sku || latestSync.message}` : "No syncs yet")}</span>
          </div>
          <div class="setting-row">
            <strong>Last request</strong>
            <span>${escapeHtml(latestRequest ? `${formatShortDate(latestRequest.created_at)} • ${latestRequest.method} ${latestRequest.path}` : "No request traffic yet")}</span>
          </div>
          <div class="setting-row">
            <strong>Feed rows</strong>
            <span>${escapeHtml(`${state.feed?.total || 0} captured payloads`)}${failedRequests ? ` • ${failedRequests} issues flagged` : ""}</span>
          </div>
          <div class="setting-row">
            <strong>Credential use</strong>
            <span>${escapeHtml(state.connection?.last_used_at ? formatDate(state.connection.last_used_at) : "Connector has not been used yet")}</span>
          </div>
        </article>

        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Exports</div>
              <h3>Download the working data</h3>
              <p>Share the exact catalog, inbound feed, or request trace with your POS team.</p>
            </div>
          </div>
          <div class="action-grid">
            <a class="button-ghost" href="/api/catalog.csv" target="_blank" rel="noreferrer">Catalog CSV</a>
            <a class="button-ghost" href="/api/feed.csv" target="_blank" rel="noreferrer">Feed CSV</a>
            <a class="button-ghost" href="/api/request-logs.csv" target="_blank" rel="noreferrer">Request CSV</a>
          </div>
        </article>
      </aside>
    </section>
  `
}

function renderProductSync() {
  return `
    <section class="sync-grid">
      <form class="form-card" data-single-form>
        <div class="section-head">
          <div>
            <div class="section-kicker">Single item</div>
            <h3>Create or update one product</h3>
            <p>Missing SKUs create draft products. Existing Shopify matches update in place by SKU.</p>
          </div>
          <span class="pill warning">Draft on first upload</span>
        </div>
        <div class="form-grid split">
          <div class="field field-full">
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
            <label for="cost">Cost</label>
            <input id="cost" name="cost" type="number" step="0.01" min="0" placeholder="8.50" />
          </div>
          <div class="field">
            <label for="quantity">Quantity</label>
            <input id="quantity" name="quantity" type="number" step="1" min="0" placeholder="10" />
          </div>
          <div class="field field-full">
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
        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Recent</div>
              <h3>Latest sync activity</h3>
              <p>Confirm recent product pushes without leaving the sync screen.</p>
            </div>
          </div>
          ${renderActivity()}
        </article>
      </div>
    </section>

    <section class="grid two">
      <form class="form-card" data-bulk-form>
        <div class="section-head">
          <div>
            <div class="section-kicker">Bulk import</div>
            <h3>Paste Woo-style JSON</h3>
            <p>Send an array from the POS and the backend will upsert catalog data in sequence.</p>
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
              <div class="section-kicker">Field mapping</div>
              <h3>What the sync expects</h3>
              <p>These are the values that matter most when the POS posts product data into Shopify.</p>
            </div>
          </div>
          <div class="setting-row">
            <strong>Name</strong>
            <span><code>name</code> or <code>title</code> becomes the Shopify product title.</span>
          </div>
          <div class="setting-row">
            <strong>Pricing</strong>
            <span><code>regular_price</code> or <code>price</code> updates the matching variant.</span>
          </div>
          <div class="setting-row">
            <strong>Cost</strong>
            <span><code>cost</code>, <code>cost_price</code>, or <code>unit_cost</code> updates Shopify cost per item.</span>
          </div>
          <div class="setting-row">
            <strong>Inventory</strong>
            <span><code>stock_quantity</code> or <code>quantity</code> sets on-hand inventory after sync.</span>
          </div>
          <div class="setting-row">
            <strong>Images</strong>
            <span><code>images[].src</code> or <code>image_url</code> needs a public URL.</span>
          </div>
        </article>
      </div>
    </section>
  `
}

function renderPosArchive() {
  const analysis = state.posArchive
  const preview = state.posPreview?.items || analysis?.product_preview || []
  const categories = analysis?.categories || []

  return `
    <section class="metric-strip">
      ${renderMetricTile("DBF tables", analysis?.table_count ?? "—", analysis ? `${formatInteger(analysis.total_records)} total DBF records` : "Analyze an archive first")}
      ${renderMetricTile("Sensitive tables", analysis?.sensitive_table_count ?? "—", "Customer, payment, contact, and credential fields stay out of product sync", analysis?.sensitive_table_count ? "warning" : "neutral")}
      ${renderMetricTile("Product preview", preview.length, "Shopify-ready rows from Item.dbf and inventory lookups", preview.length ? "success" : "neutral")}
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Archive</div>
            <h3>Analyze uploaded POS data</h3>
            <p>Use this before the final switch to inspect the DBF package, confirm tables, and build a Shopify product preview.</p>
          </div>
          <div class="button-row">
            <button class="button" type="button" data-analyze-pos-archive ${state.isAnalyzingArchive ? "disabled" : ""}>
              ${state.isAnalyzingArchive ? "Analyzing..." : "Analyze current archive"}
            </button>
            <button class="button-secondary" type="button" data-preview-pos-products ${state.isAnalyzingArchive ? "disabled" : ""}>Preview products</button>
          </div>
        </div>
        <form class="form-grid" data-pos-upload-form>
          <div class="field">
            <label for="pos_archive_file">Upload ZIP</label>
            <input id="pos_archive_file" name="file" type="file" accept=".zip,application/zip" required />
            <div class="field-note">On Railway, mount persistent storage at <code>/data</code> before using this for the final production archive.</div>
          </div>
          <div class="button-row">
            <button class="button-secondary" type="submit" ${state.isUploadingArchive ? "disabled" : ""}>
              ${state.isUploadingArchive ? "Uploading..." : "Upload and analyze"}
            </button>
          </div>
        </form>
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Safety</div>
            <h3>What gets synced</h3>
            <p>Product sync uses SKU, title, barcode, price, cost, quantity, vendor, type, tags, and POS product metafields. Customer and payment data remain archive-only.</p>
          </div>
        </div>
        ${(analysis?.notes || [
          "Products come from Item.dbf, Itemmqty.dbf, Vendor.dbf, vendors.dbf, and price-change lookups.",
          "Customer, order, employee, payment, and vendor credential tables are readable for analysis but not pushed to Shopify products.",
        ]).map((note) => `<div class="notice warning">${escapeHtml(note)}</div>`).join("")}
      </article>
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Map</div>
            <h3>DBF categories</h3>
            <p>This shows how the archive is grouped before any Shopify sync action.</p>
          </div>
        </div>
        ${renderCategoryTable(categories)}
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Core</div>
            <h3>Important tables</h3>
            <p>These are the tables the app found that are most relevant for products, customers, vendors, orders, and history.</p>
          </div>
        </div>
        ${renderCoreTableList(analysis?.core_tables || [])}
      </article>
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Products</div>
            <h3>Shopify payload preview</h3>
            <p>Preview rows are built from POS data but are not synced until you click the sync button.</p>
          </div>
          <form class="button-row" data-pos-sync-form>
            <input class="compact-input" name="offset" type="number" min="0" step="1" value="0" aria-label="Offset" />
            <input class="compact-input" name="limit" type="number" min="1" max="250" step="1" value="25" aria-label="Limit" />
            <label class="checkbox-row">
              <input name="include_zero_quantity" type="checkbox" />
              <span>Include zero qty</span>
            </label>
            <button class="button" type="submit" ${state.isSyncingArchive || !preview.length ? "disabled" : ""}>
              ${state.isSyncingArchive ? "Syncing..." : "Sync batch"}
            </button>
          </form>
        </div>
        ${renderPosProductPreview(preview)}
      </article>

      <div class="stack">
        ${renderPosSyncResult()}
        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Samples</div>
              <h3>Archive sample rows</h3>
              <p>Small samples from the most useful tables, with blank fields removed for scanability.</p>
            </div>
          </div>
          ${renderArchiveSamples(analysis?.samples || {})}
        </article>
      </div>
    </section>
  `
}

function renderCategoryTable(categories) {
  if (!categories.length) return `<p class="empty-state">No archive analysis loaded yet.</p>`
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>Category</th>
            <th>Tables</th>
            <th>Records</th>
          </tr>
        </thead>
        <tbody>
          ${categories.map((item) => `
            <tr>
              <td>${escapeHtml(item.category)}</td>
              <td>${escapeHtml(item.tables ?? 0)}</td>
              <td>${escapeHtml(formatInteger(item.records ?? 0))}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `
}

function renderCoreTableList(tables) {
  if (!tables.length) return `<p class="empty-state">No core DBF tables found yet.</p>`
  return `
    <ul class="activity-list">
      ${tables.slice(0, 18).map((table) => `
        <li class="activity-item">
          <div class="activity-title">
            <strong>${escapeHtml(table.name)}</strong>
            ${renderStatusBadge(table.category, table.sensitive_fields?.length ? "warning" : "success")}
            <span class="meta-value muted">${escapeHtml(`${formatInteger(table.records)} records`)}</span>
            <a class="button-ghost" href="/api/pos-archive/tables/${encodeURIComponent(table.name)}/csv" target="_blank" rel="noreferrer">CSV</a>
          </div>
          <span class="meta-value muted">${escapeHtml((table.fields || []).slice(0, 10).join(", "))}</span>
        </li>
      `).join("")}
    </ul>
  `
}

function renderPosProductPreview(items) {
  if (!items.length) return `<p class="empty-state">Analyze the archive to preview Shopify product payloads.</p>`
  return `
    <div class="table-wrap">
      <table class="data-table">
        <thead>
          <tr>
            <th>Product</th>
            <th>SKU</th>
            <th>Vendor</th>
            <th>Price</th>
            <th>Cost</th>
            <th>Qty</th>
          </tr>
        </thead>
        <tbody>
          ${items.slice(0, 25).map((item) => `
            <tr>
              <td>
                <div class="cell-stack">
                  <span class="cell-main">${escapeHtml(item.title || item.name || "Untitled")}</span>
                  <span class="cell-meta">${escapeHtml(item.product_type || "No type")}</span>
                </div>
              </td>
              <td>${escapeHtml(item.sku || "—")}</td>
              <td>${escapeHtml(item.vendor || "—")}</td>
              <td>${escapeHtml(item.price ?? "—")}</td>
              <td>${escapeHtml(item.cost ?? "—")}</td>
              <td>${escapeHtml(item.quantity ?? "—")}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `
}

function renderArchiveSamples(samples) {
  const entries = Object.entries(samples)
  if (!entries.length) return `<p class="empty-state">No table samples loaded yet.</p>`
  return entries.slice(0, 6).map(([name, rows]) => `
    <div class="setting-row">
      <strong>${escapeHtml(name)}</strong>
      <span><code>${escapeHtml(JSON.stringify((rows || [])[0] || {}, null, 0))}</code></span>
    </div>
  `).join("")
}

function renderPosSyncResult() {
  if (!state.posSyncResult) {
    return `
      <article class="card">
        <p class="empty-state">Run a POS product sync and the result will appear here.</p>
      </article>
    `
  }
  if (state.posSyncResult.error) {
    return renderResultCard("Last POS archive sync", false, [
      ["Message", state.posSyncResult.error.message],
      ["Code", state.posSyncResult.error.code || "request_failed"],
    ])
  }
  return renderResultCard("Last POS archive sync", true, [
    ["Total", state.posSyncResult.total],
    ["Succeeded", state.posSyncResult.succeeded],
    ["Failed", state.posSyncResult.failed],
    ["Timestamp", formatDate(state.posSyncResult.timestamp)],
  ])
}

function renderCatalog() {
  return `
    <section class="metric-strip">
      ${renderMetricTile("Catalog rows", state.catalog?.total || 0, "Products available to the POS")}
      ${renderMetricTile("Feed rows", state.feed?.total || 0, "Captured product payloads")}
      ${renderMetricTile("Request issues", getFailedRequestCount(), getFailedRequestCount() ? "Malformed or failed requests need review" : "Request traffic looks healthy", getFailedRequestCount() ? "warning" : "success")}
    </section>

    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Snapshot</div>
            <h3>Catalog export</h3>
            <p>This is the current Shopify product data your Woo-compatible API can expose back to the POS.</p>
          </div>
          <a class="button" href="/api/catalog.csv" target="_blank" rel="noreferrer">Download catalog CSV</a>
        </div>
        ${renderCatalogTable()}
      </article>

      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Inbound</div>
            <h3>Feed log</h3>
            <p>These rows show which product payloads came into the POS-facing API.</p>
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
            <div class="section-kicker">Diagnostics</div>
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
  const fullSecret = state.connection?.api_secret || ""
  const visibleSecret = fullSecret || state.connection?.api_secret_masked || ""
  const connectionError = state.loadErrors.connection
  const secretNotice = fullSecret
    ? `<div class="notice success">The full POS secret is available here. Keep it private and use Copy or Copy all for the exact value.</div>`
    : connectionError
      ? `<div class="notice danger">Settings could not load: ${escapeHtml(connectionError.message)}${connectionError.code ? ` (${escapeHtml(connectionError.code)})` : ""}</div>`
    : `<div class="notice warning">Only a masked secret preview is available. Generate a new key + secret to copy a full secret.</div>`

  return `
    <section class="grid two">
      <article class="card">
        <div class="section-head">
          <div>
            <div class="section-kicker">Connector</div>
            <h3>POS connection</h3>
            <p>Use the clean Woo-compatible path first. This server is tolerant of malformed path variants, but the documented path should stay clean.</p>
          </div>
          <div class="button-row">
            <button class="button-ghost" type="button" data-copy="${escapeAttribute(buildSimpleSettingsText())}">Copy all</button>
            <button class="button-secondary" type="button" data-rotate-secret ${state.isRotatingCredentials ? "disabled" : ""}>
              ${state.isRotatingCredentials ? "Generating..." : "New key + secret"}
            </button>
          </div>
        </div>
        ${secretNotice}
        <div class="copy-stack">
          ${renderCopyRow("URL", state.connection?.base_url || "", state.connection?.base_url || "")}
          ${renderCopyRow("Path", state.connection?.product_sync_path || "", state.connection?.product_sync_path || "")}
          ${renderCopyRow("Batch Path", state.connection?.bulk_sync_path || "", state.connection?.bulk_sync_path || "")}
          ${renderCopyRow("Customer Path", state.connection?.customer_sync_path || "", state.connection?.customer_sync_path || "")}
          ${renderCopyRow("Customer Batch Path", state.connection?.customer_bulk_sync_path || "", state.connection?.customer_bulk_sync_path || "")}
          ${renderCopyRow("Key", state.connection?.api_key || "", state.connection?.api_key || "")}
          ${renderCopyRow("Secret", visibleSecret, fullSecret)}
        </div>
      </article>

      <div class="stack">
        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Checks</div>
              <h3>Simple connection checks</h3>
              <p>Make sure the installed Shopify store and the POS connector are pointed at the same environment.</p>
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
        <article class="card">
          <div class="section-head">
            <div>
              <div class="section-kicker">Operational note</div>
              <h3>Credential stability</h3>
              <p>The POS key and secret should stay the same across deploys as long as the runtime database persists.</p>
            </div>
          </div>
          <div class="setting-row">
            <strong>Rotate only when needed</strong>
            <span>Updating code should not change credentials. Manual rotation is the normal way to replace them.</span>
          </div>
          <div class="setting-row">
            <strong>Production storage</strong>
            <span>Use a persistent volume or database so keys survive redeploys and restarts.</span>
          </div>
          <div class="setting-row">
            <strong>POS path</strong>
            <span>Document <code>/wc-api/v3/products</code> as the clean path even if the server tolerates malformed variants.</span>
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
      ["Cost", result.cost ?? "—"],
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
          <div class="section-kicker">Connection</div>
          <h3>Connector paths</h3>
          <p>The app uses the same clean endpoints your POS team should be configured to call.</p>
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
            ${renderStatusBadge(item.success ? "Synced" : "Failed", item.success ? "success" : "danger")}
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
            ${renderStatusBadge(item.status_code, toneForRequestStatus(item.status_code))}
            <span class="meta-value muted">${escapeHtml(formatDate(item.created_at))}</span>
          </div>
          <span class="meta-value muted">${escapeHtml(item.query_string ? truncateText(item.query_string, 120) : "No query string")}</span>
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
            <th>Cost</th>
            <th>Qty</th>
          </tr>
        </thead>
        <tbody>
          ${items.map((item) => `
            <tr>
              <td>
                <div class="cell-stack">
                  <span class="cell-main">${escapeHtml(item.title)}</span>
                  <span class="cell-meta">${escapeHtml(item.sku || "No SKU")}</span>
                </div>
              </td>
              <td>${escapeHtml(item.sku || "—")}</td>
              <td>${renderStatusBadge(item.status || "—", toneForProductStatus(item.status))}</td>
              <td>${escapeHtml(item.price ?? "—")}</td>
              <td>${escapeHtml(item.cost ?? "—")}</td>
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
              <td>
                <div class="cell-stack">
                  <span class="cell-main">${escapeHtml(item.source)}</span>
                  <span class="cell-meta">${escapeHtml(item.endpoint)}</span>
                </div>
              </td>
              <td>${escapeHtml(item.sku || "—")}</td>
              <td>${escapeHtml(truncateText(item.message, 110))}</td>
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
              <td>${renderStatusBadge(item.status_code, toneForRequestStatus(item.status_code))}</td>
              <td>${escapeHtml(item.method)}</td>
              <td>${escapeHtml(item.path)}</td>
              <td>${escapeHtml(truncateText(item.query_string || "—", 140))}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `
}

function renderCopyRow(label, value, copyValue = value) {
  const canCopy = Boolean(copyValue)
  return `
    <div class="copy-row">
      <div class="copy-meta">
        <div class="meta-label">${escapeHtml(label)}</div>
        <code>${escapeHtml(value)}</code>
      </div>
      <button class="copy-button" type="button" data-copy="${escapeAttribute(copyValue || "")}" ${canCopy ? "" : "disabled"}>Copy</button>
    </div>
  `
}

function renderMetricTile(label, value, note, tone = "neutral") {
  return `
    <article class="metric-tile ${tone}">
      <div class="metric-label">${escapeHtml(String(label))}</div>
      <div class="metric-main">${escapeHtml(String(value ?? "—"))}</div>
      <div class="metric-note">${escapeHtml(String(note ?? ""))}</div>
    </article>
  `
}

function renderStatusBadge(value, tone = "neutral") {
  return `<span class="status-badge ${tone}">${escapeHtml(String(value))}</span>`
}

function buildSimpleSettingsText() {
  const fullSecret = state.connection?.api_secret || ""
  return [
    `Shop: ${state.connection?.shop || ""}`,
    `URL: ${state.connection?.base_url || ""}`,
    `Path: ${state.connection?.product_sync_path || ""}`,
    `Batch Path: ${state.connection?.bulk_sync_path || ""}`,
    `Customer Path: ${state.connection?.customer_sync_path || ""}`,
    `Customer Batch Path: ${state.connection?.customer_bulk_sync_path || ""}`,
    `Key: ${state.connection?.api_key || ""}`,
    `Secret: ${fullSecret || "ROTATE_CREDENTIALS_TO_REVEAL_FULL_SECRET"}`,
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

function formatShortDate(value) {
  try {
    return new Date(value).toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    })
  } catch (_error) {
    return String(value)
  }
}

function formatInteger(value) {
  const number = Number(value)
  if (!Number.isFinite(number)) return String(value ?? "—")
  return number.toLocaleString()
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

function truncateText(value, limit = 96) {
  const text = String(value ?? "").trim()
  if (text.length <= limit) return text
  return `${text.slice(0, limit - 1)}...`
}

function getLatestActivity() {
  return state.activity?.items?.[0] || null
}

function getLatestRequest() {
  return state.requestLogs?.items?.[0] || null
}

function getFailedRequestCount() {
  return (state.requestLogs?.items || []).filter((item) => Number(item.status_code) >= 400).length
}

function toneForRequestStatus(statusCode) {
  if (Number(statusCode) >= 500) return "danger"
  if (Number(statusCode) >= 400) return "warning"
  return "success"
}

function toneForProductStatus(status) {
  const normalized = String(status || "").toLowerCase()
  if (normalized === "active") return "success"
  if (normalized === "draft") return "warning"
  if (normalized === "archived") return "neutral"
  return "neutral"
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
