const state = { offset: 0, limit: 8, borough: "", roomType: "" };
const $ = (selector) => document.querySelector(selector);

function formatNumber(value) { return new Intl.NumberFormat("en-US").format(value ?? 0); }
function formatCompactNumber(value) { return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value ?? 0); }
function formatCurrency(value) { return `$${Number(value ?? 0).toFixed(2)}`; }
function showError(message) { const banner = $("#error-banner"); banner.textContent = message; banner.hidden = false; }
function clearError() { $("#error-banner").hidden = true; }

async function request(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return response.json();
}

async function pipelineRequest(path, options = {}) {
  const response = await fetch(path, options);
  if (!response.ok) throw new Error(`Pipeline request failed (${response.status})`);
  return response.json();
}

function renderSummary(summary) {
  $("#listing-count").textContent = formatNumber(summary.listing_count);
  $("#hero-listing-total").textContent = formatNumber(summary.listing_count);
  $("#average-price").textContent = summary.average_price == null ? "—" : formatCurrency(summary.average_price);
  $("#average-availability").textContent = summary.average_availability == null ? "—" : `${Number(summary.average_availability).toFixed(1)}d`;
  $("#neighbourhood-count").textContent = formatNumber(summary.neighbourhood_count);
  $("#last-sync").textContent = summary.last_loaded_at ? `Synced ${summary.last_loaded_at.split(" ")[0]}` : "No data loaded";
}

function renderBoroughCounts(items) {
  const counts = new Map();
  items.forEach((item) => counts.set(item.neighbourhood_group, (counts.get(item.neighbourhood_group) || 0) + item.listing_count));
  [["Manhattan", "#borough-count-manhattan"], ["Brooklyn", "#borough-count-brooklyn"], ["Queens", "#borough-count-queens"]].forEach(([borough, selector]) => {
    $(selector).textContent = formatCompactNumber(counts.get(borough) || 0);
  });
}

function renderDataQuality(quality) {
  const status = (quality.validation_status || "unknown").toLowerCase();
  $("#quality-total").textContent = formatNumber(quality.total_records);
  $("#quality-missing").textContent = formatNumber(quality.missing_values);
  $("#quality-duplicates").textContent = formatNumber(quality.duplicate_ids);
  $("#quality-invalid-prices").textContent = formatNumber(quality.invalid_prices);
  $("#quality-validation").textContent = status;
  $("#quality-status").textContent = status;
  $("#quality-status").className = `quality-status ${status}`;
}

function renderUnavailable(index) {
  if (index === 0) {
    ["#listing-count", "#hero-listing-total", "#average-price", "#average-availability", "#neighbourhood-count", "#last-sync"].forEach((selector) => { $(selector).textContent = "Unavailable"; });
  } else if (index === 1) {
    $("#room-total").textContent = "Unavailable";
    $("#room-chart").innerHTML = '<div class="history-empty">Room-type data unavailable.</div>';
  } else if (index === 2) {
    $("#neighbourhood-list").innerHTML = '<div class="history-empty">Neighbourhood data unavailable.</div>';
    ["#borough-count-manhattan", "#borough-count-brooklyn", "#borough-count-queens"].forEach((selector) => { $(selector).textContent = "—"; });
  } else if (index === 3) {
    $("#listing-table").innerHTML = '<tr><td colspan="6">Listing data unavailable.</td></tr>';
    $("#listing-range").textContent = "Listings unavailable";
  } else if (index === 4) {
    ["#quality-total", "#quality-missing", "#quality-duplicates", "#quality-invalid-prices", "#quality-validation"].forEach((selector) => { $(selector).textContent = "Unavailable"; });
    $("#quality-status").textContent = "Unavailable";
    $("#quality-status").className = "quality-status empty";
  } else if (index === 5) {
    $("#pipeline-state").textContent = "Pipeline: unavailable";
    $("#pipeline-state").className = "pipeline-badge";
    $("#freshness-state").textContent = "Data freshness: unavailable";
    $("#last-successful-run").textContent = "Latest successful DAG run: unavailable";
  } else {
    $("#recovery-summary").textContent = "Unavailable";
    $("#recovery-alert").textContent = "Airflow run history is unavailable.";
    $("#recovery-alert").classList.remove("is-warning");
    $("#pipeline-history-list").innerHTML = '<div class="history-empty">Pipeline history unavailable.</div>';
  }
}

function renderPipelineStatus(status) {
  $("#pipeline-state").textContent = `Pipeline: ${status.state}`;
  $("#pipeline-state").className = `pipeline-badge ${status.state}`;
  $("#freshness-state").textContent = `Data freshness: ${status.freshness}`;
  const successfulRun = status.latest_successful_run_at;
  $("#last-successful-run").textContent = successfulRun
    ? `Latest successful DAG run: ${new Date(successfulRun).toLocaleString()}`
    : "Latest successful DAG run: none recorded";
}

function renderRecoveryPanel(history) {
  const failedRuns = (history.recent_runs || []).filter((run) => Number(run.failed_task_count) > 0 || (run.state || "").toLowerCase() === "failed");
  const latestFailed = failedRuns[0] || (history.recent_runs || [])[0];
  const alert = $("#recovery-alert");
  const statusNode = $("#recovery-summary");

  if (!history.recent_runs || history.recent_runs.length === 0) {
    statusNode.textContent = "No recent runs";
    alert.textContent = "No DAG runs were returned from Airflow yet.";
    $("#pipeline-history-list").innerHTML = '<div class="history-empty">No recent pipeline activity.</div>';
    return;
  }

  if (latestFailed && (Number(latestFailed.failed_task_count) > 0 || (latestFailed.state || "").toLowerCase() === "failed")) {
    statusNode.textContent = `Latest failed: ${latestFailed.dag_run_id}`;
    alert.textContent = `Detected ${latestFailed.failed_task_count || 0} failed task(s) in ${latestFailed.dag_run_id}. Review the Airflow run immediately.`;
    alert.classList.add("is-warning");
  } else {
    statusNode.textContent = "No active failures";
    alert.textContent = "Recent DAG runs are healthy. No failed task recovery is required.";
    alert.classList.remove("is-warning");
  }

  $("#pipeline-history-list").innerHTML = (history.recent_runs || []).slice(0, 5).map((run) => {
    const failed = Number(run.failed_task_count) || 0;
    const state = (run.state || "unknown").toLowerCase();
    const statusLabel = state === "failed" ? "Failed" : state === "running" ? "Running" : state === "queued" ? "Queued" : "Success";
    const retryButton = state === "failed" ? `<button class="retry-button" data-run-id="${encodeURIComponent(run.dag_run_id)}" title="Retry failed run"><i data-lucide="rotate-ccw"></i>Retry</button>` : "";
    return `
      <div class="history-row">
        <div>
          <div class="history-run">${run.dag_run_id}</div>
          <small>${run.execution_date ? new Date(run.execution_date).toLocaleString() : "No execution timestamp"}</small>
        </div>
        <span class="history-state ${state}">${statusLabel}</span>
        <span class="history-failures">${failed} failed</span>
        ${retryButton}
      </div>
    `;
  }).join("");
  document.querySelectorAll(".retry-button").forEach((button) => {
    button.addEventListener("click", () => retryPipeline(button.dataset.runId));
  });
  if (window.lucide) lucide.createIcons();
}

function renderRoomTypes(items) {
  $("#room-total").textContent = `${items.length} types`;
  if (!items.length) {
    $("#room-chart").innerHTML = '<div class="history-empty">No room-type metrics available.</div>';
    return;
  }
  const max = Math.max(...items.map((item) => item.listing_count), 1);
  $("#room-chart").innerHTML = items.map((item) => `
    <div class="bar-row"><span>${item.room_type.replace("Entire home/apt", "Entire home")}</span><div class="bar-track"><div class="bar-fill" style="width:${(item.listing_count / max) * 100}%"></div></div><span class="bar-value">${formatNumber(item.listing_count)}</span></div>`).join("");
}

function renderNeighbourhoods(items) {
  const rows = items.slice(0, 5).map((item, index) => `
    <div class="rank-row"><span class="rank-number">0${index + 1}</span><div><div class="rank-name">${item.neighbourhood}</div><div class="rank-sub">${formatNumber(item.listing_count)} listings · ${Number(item.average_availability).toFixed(0)}d available</div></div><span class="rank-price">${formatCurrency(item.average_price)}</span></div>`).join("");
  $("#neighbourhood-list").innerHTML = rows || '<div class="history-empty">No neighbourhood metrics available.</div>';
}

function renderListings(result) {
  $("#listing-table").innerHTML = result.items.length ? result.items.map((item) => `
    <tr><td>${item.name}<small>${item.host_name} · #${item.listing_id}</small></td><td>${item.neighbourhood}<small>${item.neighbourhood_group}</small></td><td>${item.room_type.replace("Entire home/apt", "Entire home")}</td><td class="price-cell">${formatCurrency(item.price)}<small>/ night</small></td><td>${formatNumber(item.number_of_reviews)}</td><td><div class="availability"><div class="availability-bar"><span style="width:${(item.availability_365 / 365) * 100}%"></span></div>${item.availability_365}d</div></td></tr>`).join("") : '<tr><td colspan="6">No listings match these filters.</td></tr>';
  $("#listing-range").textContent = `Showing ${result.items.length} listings · page ${Math.floor(state.offset / state.limit) + 1}`;
  $("#prev-page").disabled = state.offset === 0;
  $("#next-page").disabled = result.items.length < state.limit;
}

async function loadDashboard() {
  clearError();
  $("#refresh-button").classList.add("spinning");
  try {
    const results = await Promise.allSettled([
      request("/api/v1/summary"),
      request("/api/v1/metrics/room-types"),
      request("/api/v1/metrics/neighbourhoods?limit=500"),
      request(`/api/v1/listings?limit=${state.limit}&offset=${state.offset}${state.borough ? `&neighbourhood_group=${encodeURIComponent(state.borough)}` : ""}${state.roomType ? `&room_type=${encodeURIComponent(state.roomType)}` : ""}`),
      request("/api/v1/data-quality"),
      pipelineRequest("/operations/status"),
      pipelineRequest("/operations/history"),
    ]);
    const failures = [];
    const renderResult = (index, render) => {
      if (results[index].status === "fulfilled") render(results[index].value);
      else {
        failures.push(results[index].reason);
        renderUnavailable(index);
      }
    };
    renderResult(0, renderSummary);
    renderResult(1, renderRoomTypes);
    renderResult(2, (items) => { renderNeighbourhoods(items); renderBoroughCounts(items); });
    renderResult(3, renderListings);
    renderResult(4, renderDataQuality);
    renderResult(5, renderPipelineStatus);
    renderResult(6, renderRecoveryPanel);
    if (failures.length) {
      showError("Some live dashboard data could not be refreshed. Check API, PostgreSQL, and Airflow status.");
      failures.forEach((error) => console.error(error));
    }
  } catch (error) { showError("Unable to load dashboard data. Check that the API and PostgreSQL services are running."); console.error(error); }
  finally { $("#refresh-button").classList.remove("spinning"); }
}

async function retryPipeline(runId) {
  const button = document.querySelector(`[data-run-id="${runId}"]`);
  if (button) {
    button.disabled = true;
    button.innerHTML = '<i data-lucide="loader-circle"></i>Retrying...';
    if (window.lucide) lucide.createIcons();
  }
  try {
    await pipelineRequest(`/operations/retry?dag_run_id=${runId}`, { method: "POST" });
    await loadDashboard();
  } catch (error) {
    showError("The failed pipeline run could not be retried. Check the Airflow service and operator access.");
  }
}

async function runPipeline() {
  const button = $("#run-pipeline");
  button.disabled = true;
  button.innerHTML = '<i data-lucide="loader-circle"></i>Starting...';
  if (window.lucide) lucide.createIcons();
  try {
    await pipelineRequest("/operations/run", { method: "POST" });
    await loadDashboard();
  } catch (error) {
    showError("Pipeline could not be triggered. Check the Airflow service and operations configuration.");
  } finally {
    button.disabled = false;
    button.innerHTML = '<i data-lucide="play"></i>Run pipeline';
    if (window.lucide) lucide.createIcons();
  }
}

$("#refresh-button").addEventListener("click", loadDashboard);
$("#run-pipeline").addEventListener("click", runPipeline);
$("#apply-filters").addEventListener("click", () => { state.borough = $("#borough-filter").value; state.roomType = $("#room-filter").value; state.offset = 0; loadDashboard(); });
$("#prev-page").addEventListener("click", () => { state.offset = Math.max(0, state.offset - state.limit); loadDashboard(); });
$("#next-page").addEventListener("click", () => { state.offset += state.limit; loadDashboard(); });
if (window.lucide) lucide.createIcons();
loadDashboard();
setInterval(() => loadDashboard(), 30000);
