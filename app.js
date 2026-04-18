const state = {
  instruments: [],
  instrumentLabels: new Map(),
};

const form = document.getElementById("analysis-form");
const datalist = document.getElementById("instrument-options");
const hint = document.getElementById("form-hint");
const statusLine = document.getElementById("status-line");
const analyzeButton = document.getElementById("analyze-button");

const metricStart = document.getElementById("metric-start");
const metricEnd = document.getElementById("metric-end");
const metricMedian = document.getElementById("metric-median");
const metricPoints = document.getElementById("metric-points");
const monthlyTableBody = document.getElementById("monthly-table-body");

const priceChart = document.getElementById("price-chart");
const spreadChart = document.getElementById("spread-chart");
const percentChart = document.getElementById("percent-chart");
const seasonalityChart = document.getElementById("seasonality-chart");

function setDefaultStartDate() {
  const dateInput = document.getElementById("start-date");
  const initial = new Date(Date.UTC(2024, 0, 1));
  dateInput.value = initial.toISOString().slice(0, 10);
}

function formatNumber(value, fractionDigits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }

  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  }).format(Number(value));
}

function formatInteger(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }

  return new Intl.NumberFormat("ru-RU", {
    maximumFractionDigits: 0,
  }).format(Number(value));
}

function sanitizeTickerInput(rawValue) {
  if (!rawValue) {
    return "";
  }
  return rawValue.split("|")[0].trim().toUpperCase();
}

function labelForTicker(ticker) {
  return state.instrumentLabels.get(ticker) || ticker;
}

function updateMetrics(result) {
  metricStart.textContent = result.effective_start || "-";
  metricEnd.textContent = result.effective_end || "-";
  metricMedian.textContent = formatNumber(result.summary.median_spread, 3);
  metricPoints.textContent = formatInteger(result.summary.observations);
}

function renderMonthlyTable(rows) {
  if (!rows.length) {
    monthlyTableBody.innerHTML = '<tr><td colspan="6" class="empty-cell">Нет данных</td></tr>';
    return;
  }

  monthlyTableBody.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${row.month}</td>
          <td>${formatInteger(row.observations)}</td>
          <td>${formatNumber(row.min_spread, 3)}</td>
          <td>${formatNumber(row.median_spread, 3)}</td>
          <td>${formatNumber(row.max_spread, 3)}</td>
          <td>${formatNumber(row.std_spread, 3)}</td>
        </tr>
      `,
    )
    .join("");
}

function plotPrices(result) {
  const series1 = result.series.instrument1;
  const series2 = result.series.instrument2;

  Plotly.newPlot(
    priceChart,
    [
      {
        x: series1.map((point) => point.ts),
        y: series1.map((point) => point.close),
        mode: "lines",
        name: `${result.instrument1.secid} close`,
        line: { color: "#8fb0ff", width: 2 },
      },
      {
        x: series2.map((point) => point.ts),
        y: series2.map((point) => point.close),
        mode: "lines",
        name: `${result.instrument2.secid} close`,
        line: { color: "#5dd8b4", width: 2 },
      },
    ],
    {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(5,11,22,0.55)",
      margin: { t: 16, r: 12, l: 52, b: 42 },
      legend: { orientation: "h", y: 1.12 },
      xaxis: { gridcolor: "rgba(138,175,255,0.08)", zeroline: false },
      yaxis: { gridcolor: "rgba(138,175,255,0.08)", zeroline: false },
      font: { color: "#ecf4ff" },
    },
    { responsive: true },
  );
}

function plotSpread(result) {
  const spreadSeries = result.series.spread;
  const monthlyStats = result.monthly_stats;

  Plotly.newPlot(
    spreadChart,
    [
      {
        x: spreadSeries.map((point) => point.ts),
        y: spreadSeries.map((point) => point.spread),
        mode: "lines",
        name: "Spread",
        line: { color: "#f7a6c2", width: 2 },
      },
      {
        x: spreadSeries.map((point) => point.ts),
        y: spreadSeries.map(() => result.summary.median_spread),
        mode: "lines",
        name: "Median spread",
        line: { color: "#ffd166", width: 2, dash: "dashdot" },
      },
      {
        x: monthlyStats.map((row) => row.month_date),
        y: monthlyStats.map((row) => row.max_spread),
        mode: "markers",
        name: "Monthly max",
        marker: { color: "#63e2bc", size: 11, symbol: "circle" },
      },
      {
        x: monthlyStats.map((row) => row.month_date),
        y: monthlyStats.map((row) => row.min_spread),
        mode: "markers",
        name: "Monthly min",
        marker: { color: "#ff7f9f", size: 11, symbol: "circle" },
      },
    ],
    {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(5,11,22,0.55)",
      margin: { t: 16, r: 12, l: 52, b: 42 },
      legend: { orientation: "h", y: 1.14 },
      xaxis: { gridcolor: "rgba(138,175,255,0.08)", zeroline: false },
      yaxis: { gridcolor: "rgba(138,175,255,0.08)", zeroline: false },
      font: { color: "#ecf4ff" },
    },
    { responsive: true },
  );
}

function plotPercentSpread(result) {
  const spreadSeries = result.series.spread;

  Plotly.newPlot(
    percentChart,
    [
      {
        x: spreadSeries.map((point) => point.ts),
        y: spreadSeries.map((point) => point.spread_pct),
        mode: "lines",
        name: "% spread",
        line: { color: "#7c9cff", width: 2 },
        fill: "tozeroy",
        fillcolor: "rgba(124,156,255,0.12)",
      },
    ],
    {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(5,11,22,0.55)",
      margin: { t: 16, r: 12, l: 52, b: 42 },
      xaxis: { gridcolor: "rgba(138,175,255,0.08)", zeroline: false },
      yaxis: { gridcolor: "rgba(138,175,255,0.08)", zeroline: false, ticksuffix: "%" },
      font: { color: "#ecf4ff" },
    },
    { responsive: true },
  );
}

function plotSeasonality(result) {
  const heatmap = result.seasonality;

  Plotly.newPlot(
    seasonalityChart,
    [
      {
        type: "heatmap",
        x: heatmap.months,
        y: heatmap.years,
        z: heatmap.values,
        colorscale: [
          [0, "#ff7f9f"],
          [0.5, "#0f1b32"],
          [1, "#63e2bc"],
        ],
        colorbar: { title: "Median" },
      },
    ],
    {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(5,11,22,0.55)",
      margin: { t: 16, r: 12, l: 52, b: 42 },
      font: { color: "#ecf4ff" },
    },
    { responsive: true },
  );
}

async function loadInstruments() {
  try {
    const response = await fetch("/api/instruments");
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || "Не удалось загрузить справочник");
    }

    state.instruments = payload.items;
    state.instrumentLabels = new Map(payload.items.map((item) => [item.secid, item.label]));

    datalist.innerHTML = payload.items.map((item) => `<option value="${item.secid}">${item.label}</option>`).join("");

    hint.textContent = `Доступно ${payload.items.length} инструментов в быстром списке. Можно вводить и любой другой тикер MOEX вручную.`;
  } catch (error) {
    hint.textContent = error.message;
  }
}

async function handleSubmit(event) {
  event.preventDefault();

  const formData = new FormData(form);
  const startDate = formData.get("startDate");
  const ticker1 = sanitizeTickerInput(formData.get("ticker1"));
  const ticker2 = sanitizeTickerInput(formData.get("ticker2"));

  if (!ticker1 || !ticker2) {
    statusLine.textContent = "Укажи оба тикера.";
    return;
  }

  analyzeButton.disabled = true;
  statusLine.textContent = `Обновляю данные ${ticker1} и ${ticker2} в базе...`;

  try {
    const response = await fetch("/api/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ start_date: startDate, ticker1, ticker2 }),
    });

    const result = await response.json();

    if (!response.ok) {
      throw new Error(result.detail || "Не удалось построить анализ");
    }

    updateMetrics(result);
    renderMonthlyTable(result.monthly_stats);
    plotPrices(result);
    plotSpread(result);
    plotPercentSpread(result);
    plotSeasonality(result);

    statusLine.textContent = `${result.instrument1.label} против ${result.instrument2.label}: анализ готов.`;
  } catch (error) {
    statusLine.textContent = error.message;
  } finally {
    analyzeButton.disabled = false;
  }
}

setDefaultStartDate();
loadInstruments();
form.addEventListener("submit", handleSubmit);
