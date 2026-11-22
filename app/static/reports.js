function readReportFilters(form) {
  return {
    year: form.elements.year.value,
    start: form.elements.start.value,
    end: form.elements.end.value,
    compare: form.elements.compare.checked,
    compareStart: form.elements.compare_start.value,
    compareEnd: form.elements.compare_end.value,
    includeAlterations: form.elements.include_alterations.checked,
  }
}

function reportYearDates(year, businessDate, fullYear = false) {
  const [, month, day] = businessDate.split("-").map(Number)
  const endMonth = fullYear ? 12 : month
  const lastDay = new Date(Date.UTC(year, endMonth, 0)).getUTCDate()
  const endDay = fullYear ? lastDay : Math.min(day, lastDay)
  const end = `${year}-${String(endMonth).padStart(2, "0")}-${String(endDay).padStart(2, "0")}`
  return { start: `${year}-01-01`, end: end > businessDate ? businessDate : end }
}

function setReportYear(form, year, fullYear) {
  const businessDate = state.reports.business_date
  const { start, end } = reportYearDates(year, businessDate, fullYear)
  const prior = reportYearDates(year - 1, end, fullYear && end.endsWith("-12-31"))
  form.elements.start.value = start
  form.elements.end.value = end
  form.elements.compare_start.value = prior.start
  form.elements.compare_end.value = prior.end
  state.reportFilters = readReportFilters(form)
}

async function refreshSalesReports(filters = state.reportFilters) {
  const request = ++state.reportRequest
  const query = new URLSearchParams()
  state.reportLoading = true
  state.reportError = null
  if (filters) {
    query.set("start", filters.start)
    query.set("end", filters.end)
    query.set("include_alterations", filters.includeAlterations)
    if (filters.compare) {
      query.set("compare_start", filters.compareStart)
      query.set("compare_end", filters.compareEnd)
    }
  } else {
    query.set("prior_year", "true")
  }
  render()
  try {
    const response = await fetchJson(`/api/sales-reports?${query}`)
    if (request !== state.reportRequest) return
    state.reports = response
    state.reportPage = 0
    if (!filters && response.report) {
      state.reportFilters = {
        year: response.business_date.slice(0, 4), start: response.report.start, end: response.report.end,
        compare: Boolean(response.comparison), compareStart: response.comparison?.start || "",
        compareEnd: response.comparison?.end || "", includeAlterations: false,
      }
    }
  } catch (error) {
    if (request === state.reportRequest) state.reportError = error
  } finally {
    if (request === state.reportRequest) {
      state.reportLoading = false
      if (window.location.pathname === "/app/reports") render()
    }
  }
}

function readYearReportFilters(form) {
  return {
    through: form.elements.through.value,
    includeAlterations: form.elements.include_alterations.checked,
    departmentPeriod: form.elements.department_period.value,
  }
}

async function refreshSalesYearOverYear() {
  const request = ++state.yearReportRequest
  const filters = state.yearReportFilters
  const query = new URLSearchParams()
  if (filters) {
    query.set("through", filters.through)
    query.set("include_alterations", filters.includeAlterations)
    query.set("department_period", filters.departmentPeriod)
  }
  state.yearReportLoading = true
  state.yearReportError = null
  render()
  try {
    const data = await fetchJson(`/api/sales-reports/year-over-year?${query}`)
    if (request !== state.yearReportRequest) return
    state.yearReport = data
    if (!filters) {
      state.yearReportFilters = { through: data.through, includeAlterations: false, departmentPeriod: "year" }
    }
  } catch (error) {
    if (request === state.yearReportRequest) state.yearReportError = error
  } finally {
    if (request === state.yearReportRequest) {
      state.yearReportLoading = false
      if (window.location.pathname === "/app/reports/year-over-year") render()
    }
  }
}

const reportDimensions = { products: "Products", brands: "Brands / vendors", categories: "Categories", reps: "Sales reps" }
const reportMetrics = [
  ["net_cents", "Net sales", salesMoney],
  ["gross_profit_cents", "Gross profit", salesMoney],
  ["gross_margin_percent", "Gross margin", salesMargin],
  ["tickets", "Sales tickets", formatSalesCount],
  ["units", "Net units", formatSalesCount],
  ["average_ticket_cents", "Average ticket", salesMoney],
  ["credits_cents", "Returns & credits", salesMoney],
]

const reportWindows = {
  today: "Selected day", yesterday: "Previous day",
  week_yesterday: "Week through previous day", week: "Week to date",
  month_yesterday: "MTD through previous day", month: "Month to date",
  year_yesterday: "YTD through previous day", year: "Year to date",
}

function reportsNavigation(yearOverYear) {
  return `<nav class="reports-nav" aria-label="Sales reports">
    <a href="/app/reports" data-route ${yearOverYear ? "" : 'aria-current="page"'}>Custom reports</a>
    <a href="/app/reports/year-over-year" data-route ${yearOverYear ? 'aria-current="page"' : ""}>Year over year</a>
  </nav>`
}

function renderSalesYearOverYear() {
  const data = state.yearReport
  const filters = state.yearReportFilters
  const error = state.yearReportError ? `<div class="notice danger" role="alert">${escapeHtml(state.yearReportError.message)}</div>` : ""
  const header = `<div class="sales-header"><div><h1>Year over Year</h1><p>Three years through the same calendar date · Eastern Time</p></div>
    <div class="sales-actions"><button class="button secondary" type="button" data-year-report-refresh ${state.yearReportLoading ? "disabled" : ""}>${icon("refresh")} Refresh</button>
      ${data?.years.length ? '<button class="button secondary" type="button" data-year-report-print>Print report</button>' : ""}</div></div>
    ${reportsNavigation(true)}`
  if (!data?.years.length || !filters) {
    const message = state.yearReportLoading ? "Loading comparison…" : state.yearReportError ? "Comparison could not load" :
      data?.enabled === false ? "Sales reporting is paused in Settings." : "Waiting for the Windows connector to import POS sales history."
    return `<section class="page sales-board">${header}${error}<section class="panel"><div class="empty"><strong>${message}</strong></div></section></section>`
  }
  const salesKey = state.yearReportTax ? "sales_with_tax_cents" : "net_cents"
  const years = data.years
  const snapshot = data.store_snapshot
  const yearCostLines = years.reduce((count, year) => count + year.periods.year.missing_cost_lines, 0)
  return `<section class="page sales-board reports-board year-report-board">
    ${header}
    <section class="panel year-report-controls">
      <form class="reports-form" data-year-report-form>
        <label>Sales through<input type="date" name="through" value="${escapeHtml(filters.through)}" max="${data.business_date}" required></label>
        <label>Department period<select name="department_period">${Object.entries(reportWindows).filter(([key]) => !key.endsWith("_yesterday")).map(([key, label]) =>
          `<option value="${key}" ${filters.departmentPeriod === key ? "selected" : ""}>${label}</option>`).join("")}</select></label>
        <label class="sales-alterations"><input type="checkbox" name="include_alterations" ${filters.includeAlterations ? "checked" : ""}> Include alteration sales</label>
        <label class="sales-alterations"><input type="checkbox" data-year-report-tax ${state.yearReportTax ? "checked" : ""}> Include recorded sales tax</label>
        <button class="button" type="submit" ${state.yearReportLoading ? "disabled" : ""}>${state.yearReportLoading ? "Running…" : "Run comparison"}</button>
      </form>
    </section>
    ${error}
    ${snapshot ? renderReportStoreSnapshot(snapshot, data.snapshot_at) : ""}
    ${renderThreeYearTable(years, salesKey, state.yearReportTax ? "Sales including tax" : "Net sales")}
    ${renderThreeYearTable(years, "gross_profit_cents", "Gross profit")}
    ${yearCostLines ? '<p class="sales-muted">Profit and margin cover lines with recorded costs. Rows missing costs are marked Partial.</p>' : ""}
    <section class="panel">
      <div class="panel-head"><div><h2>Department comparison</h2><p class="sales-muted">${reportWindows[data.department_period]} · Alterations ${data.include_alterations ? "included" : "excluded"}</p></div>
        <label class="reports-sort">Order by <select data-year-report-sort>
          <option value="name" ${state.yearReportSort === "name" ? "selected" : ""}>Department</option>
          ${years.map(({ year }, index) => `<option value="${index}" ${state.yearReportSort === String(index) ? "selected" : ""}>${year} sales</option>`).join("")}
        </select></label></div>
      <div id="year-report-departments">${renderYearDepartments(data, salesKey)}</div>
    </section>
  </section>`
}

function renderReportStoreSnapshot(snapshot, observedAt) {
  const cells = [
    ["Customers", snapshot.customer_count, formatSalesCount], ["Positive customer balances", snapshot.positive_balance_cents, salesMoney],
    ["Inventory items", snapshot.inventory_items, formatSalesCount], ["Items with stock", snapshot.in_stock, formatSalesCount],
    ["Items with negative stock", snapshot.negative_stock, formatSalesCount],
    ["Inventory at cost", snapshot.inventory_cost_cents, salesMoney], ["Inventory at retail", snapshot.inventory_retail_cents, salesMoney],
  ]
  return `<section class="panel"><div class="panel-head"><h2>Current POS totals</h2><span class="panel-label">${escapeHtml(formatDate(observedAt))}</span></div>
    <dl class="report-store-totals">${cells.map(([name, value, format]) => `<div><dt>${name}</dt><dd>${value == null ? "—" : format(value)}</dd></div>`).join("")}</dl>
    ${snapshot.missing_stock_costs || snapshot.missing_stock_prices ? `<p class="sales-muted">Inventory valuation excludes items with missing cost or retail price from the corresponding total.</p>` : ""}
  </section>`
}

function yearSalesChange(current, previous) {
  if (current == null || previous == null || previous <= 0) return "—"
  const change = (current - previous) / previous * 100
  return `${change > 0 ? "+" : ""}${salesMargin(change)}`
}

function renderThreeYearTable(years, key, heading) {
  return `<section class="panel"><div class="panel-head"><h2>${heading} year over year</h2><span class="panel-label">Weeks start Sunday</span></div>
    <div class="sales-table-scroll"><table class="sales-table report-comparison-table">
      <thead><tr><th scope="col">Period</th>${years.map(year => `<th scope="col">${year.year}<small>Through ${escapeHtml(salesDate(year.through))}</small></th>`).join("")}
      <th scope="col">${years[0].year} vs ${years[1].year}</th><th scope="col">${years[1].year} vs ${years[2].year}</th></tr></thead>
      <tbody>${Object.entries(reportWindows).map(([period, label]) => {
        const values = years.map(year => year.available[period] ? year.periods[period][key] : null)
        return `<tr><th scope="row">${label}</th>${years.map((year, index) =>
          `<td>${salesMoney(values[index])}${key === "gross_profit_cents" && year.available[period] ? salesPartial(year.periods[period]) : ""}</td>`).join("")}
          <td>${yearSalesChange(values[0], values[1])}</td><td>${yearSalesChange(values[1], values[2])}</td></tr>`
      }).join("")}</tbody>
    </table></div></section>`
}

function renderYearDepartments(data, salesKey) {
  const groups = new Map()
  data.years.forEach((year, index) => {
    year.departments.forEach(row => {
      const group = groups.get(row.code) || { name: row.name, code: row.code, totals: [] }
      group.totals[index] = row
      groups.set(row.code, group)
    })
  })
  const index = Number(state.yearReportSort)
  const rows = Array.from(groups.values()).sort((first, second) =>
    (state.yearReportSort === "name" ? 0 : (second.totals[index]?.[salesKey] || 0) - (first.totals[index]?.[salesKey] || 0))
    || first.name.localeCompare(second.name))
  return `<div class="sales-table-scroll"><table class="sales-table report-department-table">
    <thead><tr><th scope="col" rowspan="2">Department</th>${data.years.map(year => `<th scope="colgroup" colspan="3">${year.year}</th>`).join("")}</tr>
    <tr>${data.years.map(() => "<th scope=\"col\">Sales</th><th scope=\"col\">Profit</th><th scope=\"col\">Margin</th>").join("")}</tr></thead>
    <tbody>${rows.map(row => `<tr><th scope="row">${escapeHtml(row.name)}${row.code !== row.name ? `<small>${escapeHtml(row.code)}</small>` : ""}</th>
      ${data.years.map((year, yearIndex) => {
        const total = row.totals[yearIndex]
        const available = year.available[data.department_period]
        return `<td>${salesMoney(total?.[salesKey] ?? (available ? 0 : null))}</td><td>${salesMoney(total ? total.gross_profit_cents : available ? 0 : null)}${total ? salesPartial(total) : ""}</td><td>${salesMargin(total?.gross_margin_percent)}</td>`
      }).join("")}</tr>`).join("")}</tbody>
  </table></div>${rows.length ? "" : '<p class="sales-muted">No department sales for this period.</p>'}`
}

function renderYearReportDepartments() {
  const key = state.yearReportTax ? "sales_with_tax_cents" : "net_cents"
  document.getElementById("year-report-departments").innerHTML = renderYearDepartments(state.yearReport, key)
}

function renderSalesReports() {
  const data = state.reports
  const report = data?.report
  const title = `<div class="sales-header"><div><h1>Sales Reports</h1><p>In-store sales · Eastern Time</p></div>
    <button class="button secondary" type="button" data-report-refresh ${state.reportLoading ? "disabled" : ""}>${icon("refresh")} Refresh</button></div>
    ${reportsNavigation(false)}`
  const error = state.reportError ? `<div class="notice danger" role="alert">${escapeHtml(state.reportError.message)}</div>` : ""
  if (!report || !state.reportFilters) {
    const message = state.reportLoading ? "Loading sales history…" : state.reportError ? "Sales Reports could not load" :
      data?.enabled === false ? "Sales reporting is paused in Settings." : "Waiting for the Windows connector to import POS sales history."
    return `<section class="page sales-board">${title}${error}<section class="panel"><div class="empty"><strong>${message}</strong></div></section></section>`
  }
  const filters = state.reportFilters
  const years = [...new Set([data.business_date.slice(0, 4), ...data.months.map((month) => month.slice(0, 4))])].sort().reverse()
  const periodLabel = period => `${salesDate(period.start)} – ${salesDate(period.end)}`
  const monthLabel = month => `${salesMonth(month)} ${month.slice(0, 4)}`
  const currentPeriod = periodLabel(report)
  return `<section class="page sales-board reports-board">
    ${title}
    <section class="panel" aria-labelledby="report-filters-heading">
      <div class="panel-head"><h2 id="report-filters-heading">Report period</h2><span class="panel-label">Imported ${monthLabel(data.months[0])} – ${monthLabel(data.months.at(-1))}</span></div>
      <form class="reports-form" data-report-form>
        <label>YTD year<select name="year" data-report-year><option value="">Custom dates</option>${years.map(year =>
          `<option value="${year}" ${year === filters.year ? "selected" : ""}>${year} YTD</option>`).join("")}</select></label>
        <label>Start date<input type="date" name="start" value="${escapeHtml(filters.start)}" max="${data.business_date}" required></label>
        <label>End date<input type="date" name="end" value="${escapeHtml(filters.end)}" max="${data.business_date}" required></label>
        <button class="button secondary" type="button" data-report-full-year>Use full year</button>
        <label class="sales-alterations"><input type="checkbox" name="include_alterations" ${filters.includeAlterations ? "checked" : ""}> Include alteration sales</label>
        <label class="sales-alterations"><input type="checkbox" name="compare" data-report-compare ${filters.compare ? "checked" : ""}> Compare another period</label>
        <fieldset class="report-comparison-dates" data-comparison-dates ${filters.compare ? "" : "hidden disabled"}>
          <legend class="reports-sr-only">Comparison dates</legend>
          <label>Comparison start<input type="date" name="compare_start" value="${escapeHtml(filters.compareStart)}" max="${data.business_date}" required></label>
          <label>Comparison end<input type="date" name="compare_end" value="${escapeHtml(filters.compareEnd)}" max="${data.business_date}" required></label>
        </fieldset>
        <button class="button" type="submit" ${state.reportLoading ? "disabled" : ""}>${state.reportLoading ? "Running…" : "Run report"}</button>
      </form>
    </section>
    ${error}
    <section aria-label="Sales report totals">
      <div class="panel-head"><h2>${escapeHtml(currentPeriod)}</h2><span class="panel-label">Alterations ${report.include_alterations ? "included" : "excluded"}</span></div>
      <div class="sales-scorecard">
        ${reportMetrics.map(([key, label, format], index) => `<div ${index === 0 ? 'class="sales-net"' : ""}><span>${label}</span><strong>${format(report.totals[key])}</strong></div>`).join("")}
      </div>
      ${report.line_count === 0 ? '<p class="sales-muted">No sales for this range in imported POS history.</p>' : ""}
      ${renderReportCoverage(report)}
      ${salesCostNote(report.totals) ? `<p class="sales-muted">${salesCostNote(report.totals)}</p>` : ""}
    </section>
    ${data.comparison ? renderReportComparison(report, data.comparison, periodLabel(data.comparison)) : ""}
    <section class="panel">
      <div class="panel-head"><h2 id="report-trend-heading">Sales by ${report.trend_kind}</h2><span class="panel-label">Net sales</span></div>
      ${renderSalesTrend(report.trend, report.trend_kind === "day" ? "month" : "report-months", "report-trend-heading")}
      ${report.trend_kind === "month" ? renderSalesMonths(report.trend, true) : ""}
    </section>
    <section class="panel">
      <div class="panel-head"><h2 id="sales-ranking-heading" tabindex="-1">Sales rankings</h2><button type="button" class="button secondary" data-report-download>Download CSV</button></div>
      <div class="reports-ranking-tools">
        <div class="sales-periods" role="group" aria-label="Rankings by">${Object.entries(reportDimensions).map(([key, label]) =>
          `<button type="button" data-report-group="${key}" aria-pressed="${state.reportGroup === key}">${label}</button>`).join("")}</div>
        <label>Search<input type="search" data-report-search value="${escapeHtml(state.reportSearch)}" placeholder="Name or SKU"></label>
        <label>Sort by<select data-report-sort>${["net_cents", "units", "gross_profit_cents", "gross_margin_percent", "tickets"].map(key =>
          `<option value="${key}" ${state.reportSort === key ? "selected" : ""}>${reportMetrics.find(metric => metric[0] === key)[1]}</option>`).join("")}</select></label>
      </div>
      <div id="sales-ranking-results">${renderReportRankings()}</div>
    </section>
    <div class="reports-time-charts">
      <section class="panel"><div class="panel-head"><h2 id="report-weekdays-heading">Sales by weekday</h2></div>
        ${renderSalesTrend(report.weekdays, "weekdays", "report-weekdays-heading")}
      </section>
      <section class="panel"><div class="panel-head"><h2 id="report-hours-heading">Sales by hour</h2><span class="panel-label">9 AM–9 PM ET</span></div>
        ${renderSalesTrend(report.hours, "today", "report-hours-heading")}
      </section>
    </div>
  </section>`
}

function renderReportCoverage(report) {
  const count = report.unavailable_months
  return count ? `<p class="sales-muted">${count} ${count === 1 ? "month has" : "months have"} no imported POS data for this period.</p>` : ""
}

function renderReportComparison(report, baseline, label) {
  return `<section class="panel"><div class="panel-head"><h2>Period comparison</h2><span class="panel-label">${escapeHtml(label)}</span></div>
    ${baseline.line_count === 0 ? '<p class="sales-muted">No sales for the comparison range in imported POS history.</p>' : ""}
    ${renderReportCoverage(baseline)}
    <div class="sales-table-scroll"><table class="sales-table">
      <thead><tr><th scope="col">Metric</th><th scope="col">Report period</th><th scope="col">Comparison period</th><th scope="col">Change</th></tr></thead>
      <tbody>${reportMetrics.map(([key, name, format]) => {
        const value = report.totals[key]
        const previous = baseline.totals[key]
        const difference = value == null || previous == null || !baseline.line_count ? null : value - previous
        const change = difference == null ? "—" :
          `${difference > 0 ? "+" : ""}${key === "gross_margin_percent" ? `${difference.toFixed(1)} pp` : format(difference)}`
        return `<tr><th scope="row">${name}</th><td>${format(value)}</td><td>${baseline.line_count ? format(previous) : "—"}</td><td>${change}</td></tr>`
      }).join("")}</tbody>
    </table></div></section>`
}

function reportRankingRows() {
  const rows = state.reports?.report?.rankings[state.reportGroup] || []
  const search = state.reportSearch.trim().toLocaleLowerCase()
  return rows.filter(row => !search || `${row.name} ${row.key}`.toLocaleLowerCase().includes(search))
    .sort((first, second) => (second[state.reportSort] ?? -Infinity) - (first[state.reportSort] ?? -Infinity)
      || second.net_cents - first.net_cents || first.name.localeCompare(second.name))
}

function renderReportRankings() {
  const rows = reportRankingRows()
  const pageSize = 25
  const start = state.reportPage * pageSize
  const page = rows.slice(start, start + pageSize)
  if (!rows.length) return `<p class="sales-muted">No matching sales.</p>`
  return `<div class="sales-table-scroll"><table class="sales-table">
    <thead><tr><th scope="col">${reportDimensions[state.reportGroup]}</th>${reportMetrics.map(([, label]) => `<th scope="col">${label}</th>`).join("")}</tr></thead>
    <tbody>${page.map(row => `<tr><th scope="row">${escapeHtml(row.name)}${state.reportGroup === "products" && row.key && row.key !== row.name ? `<small>${escapeHtml(row.key)}</small>` : ""}</th>
      ${reportMetrics.map(([key, , format]) => `<td>${format(row[key])}${key === "gross_profit_cents" ? salesPartial(row) : ""}</td>`).join("")}</tr>`).join("")}</tbody>
    </table></div>
    ${rows.length > pageSize ? `<div class="sales-receipt-pages"><button type="button" class="button secondary" data-report-page="${state.reportPage - 1}" ${start === 0 ? "disabled" : ""}>Previous</button>
      <span>${start + 1}–${start + page.length} of ${rows.length}</span>
      <button type="button" class="button secondary" data-report-page="${state.reportPage + 1}" ${start + pageSize >= rows.length ? "disabled" : ""}>Next</button></div>` : ""}`
}

function renderReportRankingResults() {
  document.getElementById("sales-ranking-results").innerHTML = renderReportRankings()
}

function downloadReportRanking() {
  const cell = value => typeof value === "number" ? String(value) :
    `"${String(value ?? "").replace(/^[\s\u0000-\u001f]*[=+\-@]/, match => "'" + match).replaceAll('"', '""')}"`
  const { start, end, include_alterations } = state.reports.report
  const header = ["Start date", "End date", "Alterations", "Name", "ID", ...reportMetrics.map(([, label]) => label), "Missing cost lines"]
  const rows = reportRankingRows().map(row => [
    start, end, include_alterations ? "Included" : "Excluded", row.name, row.key,
    ...reportMetrics.map(([key]) => key.endsWith("_cents") && row[key] != null ? Number((row[key] / 100).toFixed(2)) : row[key]),
    row.missing_cost_lines,
  ])
  const url = URL.createObjectURL(new Blob([[header, ...rows].map(row => row.map(cell).join(",")).join("\r\n")], { type: "text/csv;charset=utf-8" }))
  const link = document.createElement("a")
  link.href = url
  link.download = `sales-${state.reportGroup}-${start}-to-${end}.csv`
  link.click()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
