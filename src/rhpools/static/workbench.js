(() => {
  "use strict";

  const API_ROOT = "/api/workbench";
  const ALLOCATION_API = "/api/lp/allocation";
  const CHAIN_ID = 4663;
  const CHAIN_HEX = "0x1237";
  const DEFAULT_POOL = "0xd4eb21209c4d6093f80b5b84f5c45cc093ea14a3";
  const USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168";
  const MAX_TICK = 887272;
  const MAX_ALLOCATION_BANDS = 12;
  const ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;
  const AMOUNT_RE = /^(?:0|[1-9]\d*)(?:\.\d+)?$/;
  const INITIAL_QUERY = new URLSearchParams(window.location.search);
  const INITIAL_POOL = (INITIAL_QUERY.get("id") || "").toLowerCase();
  const INITIAL_TX = INITIAL_QUERY.get("tx");
  const EMBEDDED = INITIAL_QUERY.get("embedded") === "1" && window.parent !== window;

  const $ = (id) => document.getElementById(id);
  const elements = {
    ownerBackLink: $("owner-back-link"),
    globalClock: $("global-clock"),
    catalogHealth: $("catalog-health"),
    catalogAsof: $("catalog-asof"),
    catalogPanel: $("catalog-panel"),
    catalogCoverage: $("catalog-coverage"),
    catalogScrim: $("catalog-scrim"),
    catalogClose: $("catalog-close"),
    poolSwitcher: $("pool-switcher"),
    switcherPair: $("switcher-pair"),
    poolCount: $("pool-count"),
    search: $("pool-search"),
    kindFilter: $("kind-filter"),
    sort: $("pool-sort"),
    catalogSummary: $("catalog-summary"),
    catalogError: $("catalog-error"),
    poolList: $("pool-list"),
    pagePrev: $("page-prev"),
    pageNext: $("page-next"),
    pageLabel: $("page-label"),
    poolWorkbench: $("pool-workbench"),
    poolEmpty: $("pool-empty"),
    poolContent: $("pool-content"),
    poolError: $("pool-error"),
    poolCoverage: $("pool-coverage"),
    selectedKind: $("selected-kind"),
    selectedProtocol: $("selected-protocol"),
    selectedPair: $("selected-pair"),
    selectedAddress: $("selected-address"),
    liveBadge: $("pool-live-badge"),
    poolBlock: $("pool-block"),
    spotLabel: $("spot-label"),
    spotPrice: $("spot-price"),
    spotTick: $("spot-tick"),
    metricTvl: $("metric-tvl"),
    metricTvlNote: $("metric-tvl-note"),
    metricVolume: $("metric-volume"),
    metricSwaps: $("metric-swaps"),
    metricFees: $("metric-fees"),
    metricFeeTier: $("metric-fee-tier"),
    metricLiquidity: $("metric-liquidity"),
    metricBlock: $("metric-block"),
    metricRanges: $("metric-ranges"),
    metricRangesNote: $("metric-ranges-note"),
    invertPrice: $("invert-price"),
    returnLive: $("return-live"),
    fitChart: $("fit-chart"),
    liquidityTitle: $("liquidity-title"),
    chartMode: $("chart-mode"),
    chartFrame: $("chart-frame"),
    chartCanvas: $("liquidity-chart"),
    chartLoading: $("chart-loading"),
    chartReadout: $("chart-readout"),
    readoutPrice: $("readout-price"),
    readoutLiquidity: $("readout-liquidity"),
    readoutDetail: $("readout-detail"),
    axisOrientation: $("axis-orientation"),
    priceCanvas: $("price-chart"),
    priceChartMessage: $("price-chart-message"),
    poolTools: $("pool-tools"),
    trackingStatus: $("tracking-status"),
    allocationState: $("allocation-state"),
    allocationAmount0: $("allocation-amount0"),
    allocationAmount1: $("allocation-amount1"),
    allocationToken0: $("allocation-token0"),
    allocationToken1: $("allocation-token1"),
    rangeEdges: $("range-edges"),
    allocPositionCount: $("alloc-position-count"),
    allocPresets: $("alloc-presets"),
    allocCapital: $("alloc-capital"),
    allocShape: $("alloc-shape"),
    allocBands: $("alloc-bands"),
    allocHistoryWindow: $("alloc-history-window"),
    allocPriceLower: $("alloc-price-lower"),
    allocTickLower: $("alloc-tick-lower"),
    allocPctLower: $("alloc-pct-lower"),
    allocPriceUpper: $("alloc-price-upper"),
    allocTickUpper: $("alloc-tick-upper"),
    allocPctUpper: $("alloc-pct-upper"),
    allocEntryGas: $("alloc-entry-gas"),
    allocExitGas: $("alloc-exit-gas"),
    allocSplit: $("alloc-split"),
    allocStatus: $("alloc-status"),
    allocBandsList: $("alloc-bands-list"),
    allocHistory: $("alloc-history"),
    minuteStatus: $("minute-status"),
    lpSwaps1m: $("lp-swaps-1m"),
    lpVolume1m: $("lp-volume-1m"),
    lpPoolFees1m: $("lp-pool-fees-1m"),
    lpOurFees1m: $("lp-our-fees-1m"),
    ownerToggle: $("owner-toggle"),
    ownerForm: $("owner-form"),
    ownerInput: $("owner-input"),
    ownerDetailLink: $("owner-detail-link"),
    positionsList: $("positions-list"),
    historicalPositionsDetails: $("historical-positions-details"),
    historicalPositionsList: $("historical-positions-list"),
    historicalPositionsCount: $("historical-positions-count"),
    positionsSummary: $("positions-summary"),
    participantSort: $("participant-sort"),
    swapsList: $("swaps-list"),
    swapCount: $("swap-count"),
    flowFilter: $("flow-filter"),
    actionDrawer: $("action-drawer"),
    drawerScrim: $("drawer-scrim"),
    mobileActionOpen: $("mobile-action-open"),
    mobileActionClose: $("mobile-action-close"),
    walletState: $("wallet-state"),
    walletAccount: $("wallet-account"),
    connectWallet: $("connect-wallet"),
    walletNotice: $("wallet-notice"),
    actionTabs: $("action-tabs"),
    capabilityNotice: $("capability-notice"),
    actionForm: $("action-form"),
    actionFields: $("action-fields"),
    tickLower: $("tick-lower"),
    tickUpper: $("tick-upper"),
    rangeLower: $("range-lower"),
    rangeUpper: $("range-upper"),
    rangePrices: $("range-prices"),
    amountFields: $("amount-fields"),
    amount0Label: $("amount0-label"),
    amount1Label: $("amount1-label"),
    amount0: $("amount0"),
    amount1: $("amount1"),
    liquidityControl: $("liquidity-control"),
    liquidityBps: $("liquidity-bps"),
    liquidityOutput: $("liquidity-output"),
    slippageBps: $("slippage-bps"),
    slippageOutput: $("slippage-output"),
    simulateAction: $("simulate-action"),
    simulationPanel: $("simulation-panel"),
    simulationExpiry: $("simulation-expiry"),
    simulationWarnings: $("simulation-warnings"),
    simulationSummary: $("simulation-summary"),
    transactionSteps: $("transaction-steps"),
    transactionDialog: $("transaction-dialog"),
    reviewTitle: $("review-title"),
    reviewDescription: $("review-description"),
    reviewFrom: $("review-from"),
    reviewTo: $("review-to"),
    reviewValue: $("review-value"),
    reviewGas: $("review-gas"),
    reviewData: $("review-data"),
    reviewError: $("review-error"),
    confirmTransaction: $("confirm-transaction"),
    embeddedClose: $("embedded-close"),
    embeddedOpenOwner: $("embedded-open-owner"),
    toastRegion: $("toast-region")
  };

  const state = {
    catalog: { q: "", kind: "", sort: "activity", limit: 50, offset: 0, total: 0, counts: {}, rows: [] },
    catalogRequest: 0,
    catalogController: null,
    catalogLoaded: false,
    selectedId: null,
    selectedRow: null,
    detail: null,
    detailRequest: 0,
    detailController: null,
    stream: null,
    streamGeneration: 0,
    streamRetryTimer: null,
    owner: null,
    readOnlyOwner: null,
    account: null,
    walletChain: null,
    walletBound: false,
    action: "add",
    actionRevision: 0,
    simulation: null,
    simulationController: null,
    stepStatus: new Map(),
    review: null,
    actionTicksInitialized: false,
    allocationTicksInitialized: false,
    allocationPreview: null,
    allocationResult: null,
    allocationSpotKey: null,
    allocationRevision: 0,
    allocationController: null,
    allocationTimer: null,
    marketStatusReady: false,
    participantSort: "size",
    flowFilter: "lp",
    selectedParticipantId: null,
    selectedEventId: null,
    detailSource: null,
    streamState: "idle",
    liveBlock: 0,
    participantRenderKey: null,
    eventRenderKey: null,
    allocationRenderKey: null,
    runtimeCapabilities: null,
    embeddedOpen: !EMBEDDED,
    detailRefreshQueued: false,
    runtimeCapabilitiesLoading: false
  };
  if (EMBEDDED) document.body.classList.add("is-embedded");

  function postEmbedded(message) {
    if (!EMBEDDED) return;
    window.parent.postMessage(message, window.location.origin);
  }

  function setEmbeddedVisibility(open) {
    if (!EMBEDDED) return;
    const visible = open === true;
    if (state.embeddedOpen === visible) return;
    state.embeddedOpen = visible;
    if (!visible) {
      closeStream();
      state.detailRefreshQueued = false;
      if (state.detailController) state.detailController.abort();
      if (state.allocationController) state.allocationController.abort();
      pausePoolTools();
      if (state.simulationController) state.simulationController.abort();
      closeCatalog();
      closeActionDrawer();
      return;
    }
    if (state.selectedId) {
      loadDetail({ quiet: true });
      openStream();
    }
    loadMarketStatus();
  }

  function text(node, value) {
    const next = value == null ? "—" : String(value);
    if (node.textContent !== next) node.textContent = next;
  }

  function setNotice(node, message) {
    if (!message) {
      node.classList.add("is-hidden");
      node.textContent = "";
      return;
    }
    node.textContent = String(message);
    node.classList.remove("is-hidden");
  }

  function setCoverage(details, node, message) {
    setNotice(node, message);
    details.classList.toggle("is-hidden", !message);
    if (!message) details.open = false;
  }

  function el(tag, className, content) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (content != null) node.textContent = String(content);
    return node;
  }

  function shortAddress(value, head = 6, tail = 4) {
    if (!value || typeof value !== "string") return "—";
    return value.length > head + tail + 1 ? `${value.slice(0, head)}…${value.slice(-tail)}` : value;
  }

  function formatUsd(value) {
    if (value == null || value === "" || !Number.isFinite(Number(value))) return "Unavailable";
    const number = Number(value);
    if (Math.abs(number) >= 1_000_000_000) return `$${(number / 1_000_000_000).toFixed(number >= 10_000_000_000 ? 1 : 2)}B`;
    if (Math.abs(number) >= 1_000_000) return `$${(number / 1_000_000).toFixed(number >= 10_000_000 ? 1 : 2)}M`;
    if (Math.abs(number) >= 1_000) return `$${(number / 1_000).toFixed(number >= 10_000 ? 1 : 2)}K`;
    return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: number < 1 ? 4 : 2 }).format(number);
  }

  function formatCompact(value) {
    if (value == null || value === "") return "Unavailable";
    const number = Number(value);
    if (!Number.isFinite(number)) return String(value);
    const absolute = Math.abs(number);
    if (absolute >= 1e18) return number.toExponential(3).replace("e+", "e");
    if (absolute >= 1e15) return `${(number / 1e15).toFixed(2)}Q`;
    if (absolute >= 1e12) return `${(number / 1e12).toFixed(2)}T`;
    if (absolute >= 1e9) return `${(number / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${(number / 1e6).toFixed(2)}M`;
    if (absolute >= 1e3) return `${(number / 1e3).toFixed(2)}K`;
    return new Intl.NumberFormat(undefined, { maximumSignificantDigits: 7 }).format(number);
  }

  function formatPrice(value) {
    const number = Number(value);
    if (!(number > 0) || !Number.isFinite(number)) return "Unavailable";
    if (number >= 100_000) return number.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (number >= 100) return number.toLocaleString(undefined, { maximumFractionDigits: 4 });
    if (number >= 1) return number.toLocaleString(undefined, { maximumSignificantDigits: 8 });
    if (number >= .0001) return number.toLocaleString(undefined, { maximumSignificantDigits: 7 });
    return number.toExponential(4);
  }

  function timestampMs(value) {
    if (value == null || value === "") return null;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      const magnitude = Math.abs(numeric);
      if (numeric <= 0) return null;
      if (magnitude < 100_000_000_000) return numeric * 1000;
      if (magnitude < 100_000_000_000_000) return numeric;
      if (magnitude < 100_000_000_000_000_000) return numeric / 1000;
      return numeric / 1_000_000;
    }
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function formatTime(value) {
    const millis = timestampMs(value);
    if (millis == null) return "TIME —";
    const date = new Date(millis);
    if (Number.isNaN(date.getTime())) return "TIME —";
    return `${date.toISOString().slice(0, 19).replace("T", " ")}Z`;
  }

  function errorMessage(error) {
    if (!error) return "Unknown error";
    if (error.name === "AbortError") return "Request cancelled";
    return error.message || String(error);
  }

  async function api(path, options = {}) {
    const response = await fetch(path.startsWith("/api/") ? path : `${API_ROOT}${path}`, {
      credentials: "same-origin",
      headers: options.body ? { "Content-Type": "application/json", ...(options.headers || {}) } : options.headers,
      ...options
    });
    let data = null;
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      data = await response.json().catch(() => null);
    } else {
      const body = await response.text();
      data = body ? { error: body } : null;
    }
    if (!response.ok) {
      const message = data && (data.error || data.message);
      throw new Error(message || `Request failed (${response.status})`);
    }
    if (!data || typeof data !== "object") throw new Error("The server returned an unreadable response.");
    return data;
  }

  function toast(message, duration = 4200) {
    const node = el("div", "toast", message);
    elements.toastRegion.append(node);
    window.setTimeout(() => node.remove(), duration);
  }

  function updateClock() {
    text(elements.globalClock, `${new Date().toISOString().slice(11, 19)}Z`);
    refreshPoolLive();
  }

  function setCatalogLoading(loading) {
    elements.poolList.setAttribute("aria-busy", String(loading));
    if (loading && !state.catalog.rows.length) {
      const placeholders = Array.from({ length: 7 }, () => el("div", "catalog-placeholder"));
      elements.poolList.replaceChildren(...placeholders);
    }
  }

  function catalogQuery() {
    const params = new URLSearchParams({
      sort: state.catalog.sort,
      limit: String(state.catalog.limit),
      offset: String(state.catalog.offset)
    });
    if (state.catalog.q) params.set("q", state.catalog.q);
    if (state.catalog.kind) params.set("kind", state.catalog.kind);
    return params.toString();
  }

  async function loadCatalog({ quiet = false } = {}) {
    const request = ++state.catalogRequest;
    if (state.catalogController) state.catalogController.abort();
    const controller = new AbortController();
    state.catalogController = controller;
    if (!quiet) setCatalogLoading(true);
    try {
      const data = await api(`/pools?${catalogQuery()}`, { signal: controller.signal });
      if (request !== state.catalogRequest) return;
      const rows = Array.isArray(data.rows) ? data.rows : [];
      state.catalog.rows = rows;
      state.catalog.total = Number.isFinite(Number(data.total)) ? Number(data.total) : rows.length;
      state.catalog.counts = data.counts && typeof data.counts === "object" ? data.counts : {};
      state.catalogLoaded = true;
      renderCatalog(data);
      setCoverage(elements.catalogCoverage, elements.catalogError, "");
    } catch (error) {
      if (error.name === "AbortError" || request !== state.catalogRequest) return;
      setCoverage(elements.catalogCoverage, elements.catalogError, `Pool census unavailable: ${errorMessage(error)}`);
      if (!state.catalog.rows.length) {
        const empty = el("p", "empty-copy", "No pool rows can be shown until the census responds.");
        elements.poolList.replaceChildren(empty);
      }
    } finally {
      if (state.catalogController === controller) state.catalogController = null;
      if (request === state.catalogRequest) setCatalogLoading(false);
    }
  }

  async function loadMarketStatus() {
    try {
      const data = await api("/api/lp/status");
      state.marketStatusReady = true;
      const status = String(data.state || "unknown").toUpperCase();
      const live = status === "LIVE" || status === "OK";
      const indexed = data.indexed_head == null ? "INDEX" : `INDEX #${Number(data.indexed_head).toLocaleString()}`;
      const visibleState = live ? "LIVE" : "STALE";
      text(elements.catalogHealth, `${indexed} · ${visibleState}`);
      const lag = data.lag_blocks == null ? "" : `LAG ${Number(data.lag_blocks).toLocaleString()} · `;
      text(elements.catalogAsof, `${lag}${formatTime(data.as_of)}`);
    } catch (_) {
      if (!state.marketStatusReady) {
        text(elements.catalogHealth, "INDEX UNAVAILABLE");
        text(elements.catalogAsof, "STATUS UNAVAILABLE");
      }
    }
  }
  async function loadRuntimeCapabilities() {
    if (state.runtimeCapabilitiesLoading) return;
    state.runtimeCapabilitiesLoading = true;
    try {
      const data = await api("/capabilities");
      state.runtimeCapabilities = data && typeof data === "object" ? data : {};
    } catch (_) {
      state.runtimeCapabilities = { allocation_preview: true, simulate: false, prepare: false, broadcast: false, server_signing: false };
    } finally {
      state.runtimeCapabilitiesLoading = false;
    }
    updateActionAvailability();
  }

  function renderCatalog(data) {
    text(elements.poolCount, state.catalog.total.toLocaleString());
    const counts = data.counts && typeof data.counts === "object" ? data.counts : {};
    elements.catalogSummary.replaceChildren(
      summaryFragment("V2", counts.v2),
      summaryFragment("V3", counts.v3),
      summaryFragment("V4", counts.v4)
    );

    const rows = state.catalog.rows.map(renderPoolRow);
    if (!rows.length) {
      const empty = el("p", "empty-copy", state.catalog.q ? "No pools match this search." : "No pools were returned by the census.");
      elements.poolList.replaceChildren(empty);
    } else {
      elements.poolList.replaceChildren(...rows);
    }

    const page = Math.floor(state.catalog.offset / state.catalog.limit) + 1;
    const pageCount = Math.max(1, Math.ceil(state.catalog.total / state.catalog.limit));
    text(elements.pageLabel, `Page ${page} / ${pageCount}`);
    elements.pagePrev.disabled = state.catalog.offset <= 0;
    elements.pageNext.disabled = state.catalog.offset + state.catalog.rows.length >= state.catalog.total;
  }

  function summaryFragment(label, value) {
    const node = el("span");
    node.append(document.createTextNode(`${label} `), el("b", "", value == null ? "—" : Number(value).toLocaleString()));
    return node;
  }

  function renderPoolRow(row) {
    const button = el("button", "pool-row");
    button.type = "button";
    const rowId = String(row.id || row.address || "").toLowerCase();
    if (rowId && rowId === state.selectedId) button.classList.add("is-selected");

    const main = el("span", "pool-row-main");
    const pair = el("span", "pool-row-pair");
    pair.append(el("strong", "", pairFromTokens(row)), el("span", "", row.kind || "?"));
    const meta = el("span", "pool-row-meta");
    const fee = row.fee_ppm == null ? "Fee not indexed" : `${formatFee(row.fee_ppm)} fee`;
    meta.append(el("span", "", fee), el("span", "", shortAddress(rowId)));
    main.append(pair, meta);

    const value = el("span", "pool-row-value");
    value.append(el("strong", "", row.swaps_1h == null ? "—" : Number(row.swaps_1h).toLocaleString()), el("small", "", row.swaps_1h == null ? "Not indexed" : "swaps / 1h"));

    const activity = el("span", `pool-row-activity${row.last_swap_at ? " is-live" : ""}`);
    activity.append(el("i"), document.createTextNode(row.last_swap_at ? `Last swap ${formatTime(row.last_swap_at)}` : "No recent swap coverage"));
    button.append(main, value, activity);
    button.addEventListener("click", () => selectPool(row));
    return button;
  }

  function tokenLabel(token) {
    if (typeof token === "string") return shortAddress(token);
    return token && (token.symbol || (token.address && shortAddress(token.address))) || "unresolved token";
  }

  function pairFromTokens(row) {
    if (row.token0 || row.token1) return `${tokenLabel(row.token0)} / ${tokenLabel(row.token1)}`;
    return row.pair || shortAddress(row.id || row.address);
  }

  function formatFee(ppm) {
    if (ppm == null || ppm === "") return "Unavailable";
    const number = Number(ppm);
    if (!Number.isFinite(number)) return "Unavailable";
    return `${(number / 10_000).toLocaleString(undefined, { maximumFractionDigits: 4 })}%`;
  }

  function openCatalog() {
    closeActionDrawer();
    elements.catalogPanel.inert = false;
    elements.catalogPanel.classList.add("is-open");
    elements.catalogPanel.setAttribute("aria-hidden", "false");
    elements.poolSwitcher.setAttribute("aria-expanded", "true");
    elements.catalogScrim.classList.remove("is-hidden");
    if (!state.catalogController) loadCatalog({ quiet: state.catalogLoaded });
  }
  function closeCatalog({ restoreFocus = false } = {}) {
    const hadFocus = elements.catalogPanel.contains(document.activeElement);
    elements.catalogPanel.classList.remove("is-open");
    elements.catalogPanel.setAttribute("aria-hidden", "true");
    elements.catalogPanel.inert = true;
    elements.poolSwitcher.setAttribute("aria-expanded", "false");
    if (state.catalogController) {
      const controller = state.catalogController;
      state.catalogController = null;
      controller.abort();
    }
    if (restoreFocus && hadFocus) elements.poolSwitcher.focus({ preventScroll: true });
  }


  function selectPool(row) {
    const id = String(row && (row.id || row.address) || "").toLowerCase();
    if (!id) return;
    closeCatalog();
    if (id === state.selectedId && state.detail) {
      elements.poolWorkbench.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    state.selectedId = id;
    state.selectedRow = row;
    state.detail = null;
    state.liveBlock = 0;
    state.participantRenderKey = null;
    state.eventRenderKey = null;
    state.allocationRenderKey = null;
    state.actionTicksInitialized = false;
    resetAllocationPreview();
    state.selectedParticipantId = null;
    state.selectedEventId = null;
    state.streamState = "loading";
    invalidateSimulation("Pool changed");
    chart.clear();
    trackingChart.clear();
    if (state.catalogLoaded) renderCatalog({ counts: state.catalog.counts });
    elements.poolEmpty.classList.add("is-hidden");
    elements.poolContent.classList.remove("is-hidden");
    renderPoolShell(row);
    setPoolLive(false, "LOADING");
    setCoverage(elements.poolCoverage, elements.poolError, "");
    const url = new URL("/pool", window.location.origin);
    url.searchParams.set("id", id);
    if (ADDRESS_RE.test(state.owner)) url.searchParams.set("owner", state.owner);
    if (id === INITIAL_POOL && INITIAL_TX) url.searchParams.set("tx", INITIAL_TX);
    if (EMBEDDED) url.searchParams.set("embedded", "1");
    window.history.replaceState(null, "", url);
    loadDetail();
    openStream();
    if (window.innerWidth <= 760) elements.poolWorkbench.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function renderPoolShell(row) {
    text(elements.selectedKind, String(row.kind || "—").toUpperCase());
    text(elements.selectedProtocol, row.protocol || "Protocol unavailable");
    text(elements.selectedPair, pairFromTokens(row));
    text(elements.switcherPair, pairFromTokens(row));
    const identity = row.id || row.address;
    text(elements.selectedAddress, identity ? (identity.length > 42 ? shortAddress(identity, 12, 10) : identity) : "Identifier unavailable");
    elements.selectedAddress.title = identity ? `Copy pool identifier: ${identity}` : "Pool identifier unavailable";
    renderMetrics(row, null);
    updateActionAvailability();
  }

  function poolQuery(id) {
    const params = new URLSearchParams({ id });
    if (ADDRESS_RE.test(state.owner || "")) params.set("owner", state.owner);
    if (id === INITIAL_POOL && INITIAL_TX) params.set("tx", INITIAL_TX);
    return params;
  }

  async function loadDetail({ quiet = false } = {}) {
    if (!state.selectedId || (EMBEDDED && !state.embeddedOpen)) return;
    if (quiet && state.detailController) {
      state.detailRefreshQueued = true;
      return;
    }
    const selectedId = state.selectedId;
    const request = ++state.detailRequest;
    if (state.detailController) state.detailController.abort();
    const controller = new AbortController();
    state.detailController = controller;
    if (!quiet) {
      elements.chartLoading.textContent = "LOADING ONCHAIN LIQUIDITY";
      elements.chartLoading.classList.remove("is-hidden");
    }
    const params = poolQuery(selectedId);
    try {
      const detail = await api(`/pool?${params}`, { signal: controller.signal });
      if (request !== state.detailRequest || selectedId !== state.selectedId) return;
      applyDetail(detail, "request");
    } catch (error) {
      if (error.name === "AbortError" || request !== state.detailRequest || selectedId !== state.selectedId) return;
      setPoolLive(false, "STALE");
      setCoverage(elements.poolCoverage, elements.poolError, `Live pool detail unavailable: ${errorMessage(error)}`);
      elements.chartLoading.textContent = "LIVE LIQUIDITY UNAVAILABLE";
      elements.chartLoading.classList.remove("is-hidden");
    } finally {
      if (state.detailController === controller) state.detailController = null;
      if (state.detailRefreshQueued && selectedId === state.selectedId && (!EMBEDDED || state.embeddedOpen)) {
        state.detailRefreshQueued = false;
        window.setTimeout(() => loadDetail({ quiet: true }), 0);
      }
    }
  }

  function openStream() {
    closeStream();
    if (!state.selectedId || !("EventSource" in window) || (EMBEDDED && !state.embeddedOpen)) return;
    const generation = ++state.streamGeneration;
    const selectedId = state.selectedId;
    const params = poolQuery(selectedId);
    const source = new EventSource(`${API_ROOT}/stream?${params}`);
    state.stream = source;

    const receive = (event) => {
      if (generation !== state.streamGeneration || selectedId !== state.selectedId) return;
      let payload;
      try { payload = JSON.parse(event.data); } catch (_) { return; }
      if (payload && payload.type === "pool" && payload.data) applyDetail(payload.data, "stream");
      else if (payload && (payload.type === "stale" || payload.type === "error")) markStreamStale(payload);
      else if (payload && payload.pool) applyDetail(payload, "stream");
    };
    source.onmessage = receive;
    source.addEventListener("pool", receive);
    source.addEventListener("stale", (event) => {
      if (generation !== state.streamGeneration || selectedId !== state.selectedId) return;
      let payload = {};
      try { payload = JSON.parse(event.data); } catch (_) { payload = { error: event.data }; }
      markStreamStale(payload);
    });
    source.onopen = () => {
      if (generation === state.streamGeneration && selectedId === state.selectedId && !state.liveBlock) {
        state.streamState = "syncing";
        setPoolLive(false, "SYNCING");
      }
    };
    source.onerror = () => {
      if (generation !== state.streamGeneration || selectedId !== state.selectedId) return;
      state.streamState = "reconnecting";
      setPoolLive(false, "RECONNECTING");
      // A CLOSED EventSource never retries itself.
      if (source.readyState === EventSource.CLOSED) {
        window.clearTimeout(state.streamRetryTimer);
        state.streamRetryTimer = window.setTimeout(() => {
          if (generation === state.streamGeneration && selectedId === state.selectedId) openStream();
        }, 1500);
      }
    };
  }

  function closeStream() {
    state.streamGeneration += 1;
    window.clearTimeout(state.streamRetryTimer);
    state.streamRetryTimer = null;
    if (state.stream) state.stream.close();
    state.stream = null;
  }

  function markStreamStale(payload) {
    state.streamState = "stale";
    setPoolLive(false, "STALE");
    const message = payload && (payload.error || payload.message || (payload.data && payload.data.health && payload.data.health.error));
    setCoverage(elements.poolCoverage, elements.poolError, message ? `Live feed stale: ${message}` : "Live feed stale. The last confirmed snapshot remains visible while the stream reconnects.");
  }

  function detailMatchesSelection(detail) {
    if (!detail || !detail.pool) return false;
    const id = String(detail.pool.id || detail.pool.address || "").toLowerCase();
    const address = String(detail.pool.address || "").toLowerCase();
    return !id || id === state.selectedId || address === state.selectedId;
  }

  function detailCoverageMessage(detail) {
    const health = detail.health || {};
    const coverage = detail.coverage || {};
    const positions = coverage.positions;
    const participants = coverage.participants;
    const accounting = coverage.accounting;
    const lpEvents = coverage.lp_events;
    const freshness = detail.freshness && typeof detail.freshness === "object" ? detail.freshness : {};
    const currentEvents = freshness.current_events;
    const curve = freshness.curve;
    const messages = [];
    if (health.error) messages.push(`Selected pool: ${health.error}`);
    if (currentEvents && currentEvents.complete_through_snapshot === false) {
      const through = currentEvents.through_block == null ? "" : ` through #${Number(currentEvents.through_block).toLocaleString()}`;
      messages.push(`Current event coverage incomplete${through}`);
    }
    if (currentEvents && currentEvents.error) messages.push(`Current events: ${currentEvents.error}`);
    if (curve && curve.complete_through_snapshot === false) {
      const through = curve.verified_through_block == null ? "" : ` through #${Number(curve.verified_through_block).toLocaleString()}`;
      messages.push(`Liquidity curve coverage incomplete${through}`);
    }
    if (positions && positions.supported && !positions.complete) {
      const range = positions.from_block != null && positions.through_block != null
        ? ` #${Number(positions.from_block).toLocaleString()}–#${Number(positions.through_block).toLocaleString()}`
        : "";
      messages.push(`Owned positions warming${range}`);
    }
    if (positions && positions.ranges_truncated) messages.push("Owned range list safety-limited");
    if (positions && positions.error) messages.push(`Owned positions: ${positions.error}`);
    if (participants && participants.supported && participants.state !== "complete") {
      const blocks = participants.from_block != null && participants.through_block != null
        ? ` #${Number(participants.from_block).toLocaleString()}–#${Number(participants.through_block).toLocaleString()}`
        : "";
      const counts = participants.resolved_ranges != null && participants.discovered_ranges != null
        ? ` · ${Number(participants.resolved_ranges).toLocaleString()}/${Number(participants.discovered_ranges).toLocaleString()} ranges`
        : "";
      messages.push(`LP positions ${String(participants.state || "indexing")}${blocks}${counts}`);
    }
    if (participants && participants.error) messages.push(`LP positions: ${participants.error}`);
    if (accounting && accounting.state === "baseline_pending") {
      messages.push(`PnL baseline pending${accounting.pending_positions != null ? ` · ${Number(accounting.pending_positions).toLocaleString()} positions` : ""}`);
    }
    if (accounting && accounting.state === "error") messages.push("Interval accounting unavailable");
    if (lpEvents && lpEvents.supported && lpEvents.lifecycle_history_complete === false) {
      const liveFrom = lpEvents.live_swap_from_block == null ? "" : ` · swaps live from #${Number(lpEvents.live_swap_from_block).toLocaleString()}`;
      messages.push(`LP history warming${liveFrom}`);
    }
    if (lpEvents && lpEvents.state === "retention_truncated") messages.push("Event tape retention-limited");
    if (lpEvents && lpEvents.error) messages.push(`LP lifecycle: ${lpEvents.error}`);
    return messages.join(" · ");
  }

  function applyDetail(detail, source) {
    if (!detailMatchesSelection(detail)) return;
    const nextBlock = Number(detail.block) || 0;
    const priorBlock = Number(state.detail && state.detail.block) || 0;
    if (state.detail && (nextBlock < priorBlock
      || (nextBlock === priorBlock && (timestampMs(detail.as_of) || 0) < (timestampMs(state.detail.as_of) || 0)))) return;
    state.detail = detail;
    state.liveBlock = Math.max(state.liveBlock, nextBlock);
    state.detailSource = source;
    state.streamState = source === "stream" ? "live" : state.streamState;
    state.selectedRow = detail.pool;
    refreshPoolLive();
    setCoverage(elements.poolCoverage, elements.poolError, detailCoverageMessage(detail));
    chart.setSnapshot(detail);
    if (poolToolsOpen()) trackingChart.setSnapshot(detail);
    renderDetail(detail);
  }

  function refreshPoolLive() {
    if (!state.detail) return;
    const healthState = String(state.detail.health && state.detail.health.state || "").toLowerCase();
    const freshness = state.detail.freshness && typeof state.detail.freshness === "object"
      ? state.detail.freshness
      : {};
    const detailBlock = Number(freshness.snapshot_block ?? freshness.state_block ?? state.detail.block) || 0;
    const snapshotMs = timestampMs(
      freshness.snapshot_timestamp
      ?? freshness.state_timestamp
      ?? state.detail.block_timestamp
    );
    const ageSeconds = snapshotMs == null ? Infinity : Math.max(0, (Date.now() - snapshotMs) / 1000);
    const streamFailure = state.streamState === "stale" || state.streamState === "reconnecting";
    const streamReady = state.streamState === "live";
    const incomplete = freshness.curve && freshness.curve.complete_through_snapshot === false;
    const hardFailure = streamFailure || incomplete || ["error", "stale", "inactive"].includes(healthState);
    const live = streamReady && detailBlock > 0 && ageSeconds < 5 && !hardFailure;
    const label = live
      ? `LIVE · ${ageSeconds < 1 ? "<1S" : `${Math.floor(ageSeconds)}S`}`
      : streamFailure ? state.streamState.toUpperCase()
        : !streamReady ? "SYNCING"
          : Number.isFinite(ageSeconds) && ageSeconds >= 5 ? `STALE · ${Math.floor(ageSeconds)}S`
            : detailBlock <= 0 || snapshotMs == null ? "WAITING"
              : incomplete ? "PARTIAL" : healthState.toUpperCase() || "STALE";
    setPoolLive(live, label);
    elements.liveBadge.title = [
      `Depth snapshot #${detailBlock || "—"}`,
      freshness.current_events && freshness.current_events.complete_through_snapshot === false
        ? "Activity/history coverage is separate and remains partial." : "",
    ].filter(Boolean).join("\n");
  }

  function setPoolLive(live, label) {
    elements.liveBadge.classList.toggle("is-stale", !live);
    const bold = elements.liveBadge.querySelector("b");
    if (bold) bold.textContent = label;
    renderChartMode();
  }

  function renderChartMode() {
    const inspect = elements.chartMode.dataset.mode === "inspect";
    const zoom = elements.chartMode.dataset.zoom || "1.0";
    const stale = elements.liveBadge.classList.contains("is-stale");
    const freshness = stale ? ` · ${elements.liveBadge.querySelector("b").textContent}` : "";
    text(elements.chartMode, `${inspect ? `VIEW · ${zoom}×` : "LIVE"}${freshness}`);
    elements.chartMode.classList.toggle("is-live", !inspect);
    elements.chartMode.classList.toggle("is-stale", stale);
  }

  function renderDetail(detail) {
    const pool = detail.pool || {};
    renderPoolShell(pool);
    renderMetrics(pool, detail);
    text(elements.liquidityTitle, pool.kind === "v2" ? "CONSTANT-PRODUCT DEPTH" : "INITIALIZED-TICK LIQUIDITY DEPTH");
    renderParticipants(detail);
    renderEvents(detail);
    if (elements.actionDrawer.classList.contains("is-open")) configureActionRange(detail);
    if (poolToolsOpen()) initializePoolTools();
    updatePricePresentation();
    updateActionAvailability();
    elements.chartLoading.classList.toggle("is-hidden", Boolean(detail.liquidity && Array.isArray(detail.liquidity.curve) && detail.liquidity.curve.length));
    if (!detail.liquidity || !Array.isArray(detail.liquidity.curve) || !detail.liquidity.curve.length) {
      elements.chartLoading.textContent = "NO LIVE LIQUIDITY CURVE";
    }
  }

  function renderMetrics(pool, detail) {
    const lp = detail && detail.lp || {};
    const active = detail && detail.block > 0 && detail.liquidity ? detail.liquidity.active : null;
    const ours = detail ? lp.our_active_liquidity : null;
    text(elements.metricLiquidity, active == null ? "—" : Number(active).toExponential(3));
    elements.metricLiquidity.title = active == null ? "" : String(active);
    const freshness = detail && detail.freshness && typeof detail.freshness === "object"
      ? detail.freshness
      : {};
    const valuedBlock = Number(freshness.snapshot_block ?? (detail && detail.block)) || 0;
    const headBlock = Math.max(valuedBlock, state.liveBlock);
    const valuedTimestamp = freshness.snapshot_timestamp ?? (detail && detail.block_timestamp);
    const valuedTime = valuedTimestamp == null ? "TIME UNKNOWN" : formatTime(valuedTimestamp);
    text(elements.metricBlock, valuedBlock > 0 ? `#${valuedBlock.toLocaleString()} · ${valuedTime}` : "AWAITING CHAIN");
    text(elements.poolBlock, headBlock > 0 ? `HEAD ${headBlock.toLocaleString()}` : "HEAD —");
    text(elements.metricTvl, ours == null ? "—" : Number(ours).toExponential(3));
    elements.metricTvl.title = ours == null ? "" : String(ours);
    text(elements.metricTvlNote, ours == null ? "—" : "RAW LIQUIDITY");
    text(elements.metricVolume, lp.active_share_pct == null ? "—" : `${Number(lp.active_share_pct).toLocaleString(undefined, { maximumFractionDigits: 4 })}%`);
    text(elements.metricSwaps, lp.active_share_pct == null ? "—" : "OF POOL AT SPOT");
    text(elements.metricFees, lp.value_usd == null ? "—" : formatUsd(lp.value_usd));
    text(elements.metricFeeTier, lp.value_usd == null ? "—" : "CURRENT PRINCIPAL");
    const rangesReady = lp.in_range != null && lp.position_count != null;
    text(elements.metricRanges, rangesReady ? `${Number(lp.in_range).toLocaleString()} / ${Number(lp.position_count).toLocaleString()}` : "—");
    text(elements.metricRangesNote, rangesReady ? "IN RANGE / TOTAL" : "—");
  }

  function liquidityBig(value) {
    try { return BigInt(value || "0"); } catch (_) { return 0n; }
  }

  function splitPositionSet(positions) {
    const ordered = (Array.isArray(positions) ? [...positions] : []).sort((left, right) => {
      const a = liquidityBig(left.liquidity), b = liquidityBig(right.liquidity);
      return a > b ? -1 : a < b ? 1 : Number(left.lo) - Number(right.lo);
    });
    const active = ordered.filter((position) => liquidityBig(position.liquidity) > 0n);
    const empty = ordered.filter((position) => liquidityBig(position.liquidity) === 0n);
    const realized = empty.filter((position) =>
      Math.abs(Number(position.pnl_usd) || 0) >= .01
      || Number(position.fees_earned_usd) >= .01
      || Number(position.uncollected_usd) >= 1);
    const realizedIds = new Set(realized.map(participantId));
    const dormant = empty.filter((position) => !realizedIds.has(participantId(position)));
    if (!active.length) return { material: realized, dust: [], empty: dormant };
    const maximum = liquidityBig(active[0].liquidity);
    const material = active.filter((position, index) => {
      if (index === 0) return true;
      const share = position.active_share_pct == null ? NaN : Number(position.active_share_pct);
      const principal = position.value_usd == null ? NaN : Math.abs(Number(position.value_usd));
      return (Number.isFinite(share) && share >= .001)
        || (Number.isFinite(principal) && principal >= 1)
        || liquidityBig(position.liquidity) * 10_000n >= maximum;
    });
    const materialIds = new Set(material.map(participantId));
    return { material: [...material, ...realized], dust: active.filter((position) => !materialIds.has(participantId(position))), empty: dormant };
  }

  function renderAllocation(detail) {
    if (!poolToolsOpen()) return;
    const pool = detail.pool || {};
    const lp = detail.lp || {};
    const token0 = tokenLabel(pool.token0);
    const token1 = tokenLabel(pool.token1);
    const renderKey = JSON.stringify([chart.inverted, token0, token1, lp, detail.positions, detail.coverage && detail.coverage.swaps_1m]);
    if (renderKey === state.allocationRenderKey) return;
    state.allocationRenderKey = renderKey;
    text(elements.allocationToken0, token0);
    text(elements.allocationToken1, token1);
    text(elements.allocationAmount0, lp.amount0 == null ? "—" : formatCompact(lp.amount0));
    text(elements.allocationAmount1, lp.amount1 == null ? "—" : formatCompact(lp.amount1));
    elements.allocationAmount0.title = lp.amount0 == null ? "" : String(lp.amount0);
    elements.allocationAmount1.title = lp.amount1 == null ? "" : String(lp.amount1);
    text(elements.allocationState, lp.value_usd == null ? "—" : "CUSTODY NOW");

    const { material: positions, dust } = splitPositionSet(detail.positions);
    const edgeNodes = [el("span", "", "RANGE EDGES")];
    if (!positions.length) {
      edgeNodes.push(el("p", "", "No active ranges."));
    } else {
      positions.slice(0, 3).forEach((position, index) => {
        const low = tickToDisplayPrice(position.lo);
        const high = tickToDisplayPrice(position.hi);
        const row = el("div", `edge-row${index === 0 ? " is-primary" : ""}`);
        row.append(
          el("b", "", index === 0 ? "PRIMARY" : `#${index + 1}`),
          el("span", "", `${formatPrice(Math.min(low, high))} ↔ ${formatPrice(Math.max(low, high))} · ticks ${Number(position.lo).toLocaleString()}…${Number(position.hi).toLocaleString()}`)
        );
        edgeNodes.push(row);
      });
      const hidden = Math.max(0, positions.length - 3) + dust.length;
      if (hidden) edgeNodes.push(el("p", "", `${hidden} SMALLER ACTIVE RANGE${hidden === 1 ? "" : "S"} · SEE TABLE`));
    }
    elements.rangeEdges.replaceChildren(...edgeNodes);

    const coverage = detail.coverage && detail.coverage.swaps_1m || {};
    const complete = coverage.complete === true;
    const span = coverage.covered_span_s == null ? NaN : Number(coverage.covered_span_s);
    const coverageLabel = complete ? "LIVE" : Number.isFinite(span) ? `WARMING ${Math.min(60, Math.max(0, Math.floor(span)))}S / 60S` : "WARMING";
    text(elements.minuteStatus, coverageLabel);
    elements.minuteStatus.classList.toggle("warming", !complete);
    text(elements.lpSwaps1m, lp.swaps_1m == null ? "—" : Number(lp.swaps_1m).toLocaleString());
    text(elements.lpVolume1m, lp.volume_1m_usd == null ? "—" : formatUsd(lp.volume_1m_usd));
    text(elements.lpPoolFees1m, lp.pool_fees_1m_usd == null ? "—" : formatUsd(lp.pool_fees_1m_usd));
    text(elements.lpOurFees1m, lp.our_fees_1m_usd == null ? "—" : formatUsd(lp.our_fees_1m_usd));
  }

  function updatePricePresentation({ renderPlan = false } = {}) {
    if (!state.detail) return;
    const pool = state.detail.pool || {};
    const token0 = tokenLabel(pool.token0);
    const token1 = tokenLabel(pool.token1);
    const raw = state.detail.spot && Number(state.detail.spot.price_token1_per_token0);
    const price = chart.inverted && raw > 0 ? 1 / raw : raw;
    const pair = chart.inverted ? `${token1}/${token0}` : `${token0}/${token1}`;
    text(elements.selectedPair, pair);
    text(elements.switcherPair, pair);
    text(elements.spotLabel, chart.inverted ? `${token0} PER ${token1}` : `${token1} PER ${token0}`);
    text(elements.spotPrice, formatPrice(price));
    text(elements.spotTick, state.detail.spot && state.detail.spot.tick != null ? `TICK ${Number(state.detail.spot.tick).toLocaleString()}` : "TICK —");
    text(elements.axisOrientation, `PRICE · ${chart.inverted ? `${token0} PER ${token1}` : `${token1} PER ${token0}`}`);
    elements.invertPrice.setAttribute("aria-pressed", String(chart.inverted));
    if (elements.actionDrawer.classList.contains("is-open")) updateRangePrices();
    if (poolToolsOpen()) {
      updateAllocationBoundaryFields();
      if (renderPlan && state.allocationPreview) renderAllocationPlan(state.allocationPreview);
      renderAllocation(state.detail);
    }
  }

  function participantRecords(detail) {
    if (Array.isArray(detail.participants)) return detail.participants;
    return (Array.isArray(detail.positions) ? detail.positions : []).map((position) => ({
      ...position,
      ours: true,
      value_usd: null,
      active_share_pct: null,
      fees_earned_usd: null,
      pnl_usd: null,
      accounting_status: "unavailable"
    }));
  }

  function participantBaseline(participant) {
    const parts = [];
    if (participant.pnl_since_timestamp != null && Number.isFinite(Number(participant.pnl_since_timestamp))) {
      parts.push(`since ${formatTime(participant.pnl_since_timestamp)}`);
    }
    if (participant.pnl_since_block != null && Number.isFinite(Number(participant.pnl_since_block))) {
      parts.push(`#${Number(participant.pnl_since_block).toLocaleString()}`);
    }
    return parts.join(" · ") || "interval unknown";
  }

  function participantTokenAmounts(participant, prefix) {
    const pool = state.detail && state.detail.pool || {};
    const token0 = tokenLabel(pool.token0);
    const token1 = tokenLabel(pool.token1);
    const value0 = participant[`${prefix}0`];
    const value1 = participant[`${prefix}1`];
    const parts = [];
    if (value0 != null) parts.push(`${value0} ${token0}`);
    if (value1 != null) parts.push(`${value1} ${token1}`);
    return parts.join(" + ") || "Unknown";
  }

  function accountingIntervalLabel(detail, participants) {
    const baselines = [...new Set(participants.map(participantBaseline).filter((label) => label !== "interval unknown"))];
    if (baselines.length === 1) return `FEES + GROSS PNL ${baselines[0]} · BEFORE GAS`;
    if (baselines.length > 1) return "FEES + GROSS PNL · PER-POSITION VERIFIED BASELINES · BEFORE GAS";
    const accounting = detail.coverage && detail.coverage.accounting || {};
    const timestamp = accounting.since_timestamp == null ? null : accounting.since_timestamp;
    const block = accounting.since_block == null ? null : accounting.since_block;
    const parts = [];
    if (timestamp != null) parts.push(`since ${formatTime(timestamp)}`);
    if (block != null) parts.push(`block #${Number(block).toLocaleString()}`);
    return parts.length
      ? `FEES + GROSS PNL ${parts.join(" · ")} · BEFORE GAS`
      : "FEES + GROSS PNL SINCE VERIFIED BASELINE · INTERVAL UNKNOWN";
  }

  function participantOwnerLabel(participant) {
    return shortAddress(participant.owner || participant.manager || state.owner);
  }

  function participantId(participant) {
    return String(participant.id || `${participant.owner || ""}:${participant.lo}:${participant.hi}`);
  }

  function renderParticipants(detail) {
    const participants = participantRecords(detail);
    const renderKey = JSON.stringify([state.participantSort, state.selectedParticipantId, chart.inverted, participants]);
    if (renderKey === state.participantRenderKey) return;
    state.participantRenderKey = renderKey;
    const { material, dust, empty } = splitPositionSet(participants);
    const ordered = [...material].sort((left, right) => {
      if (state.participantSort === "pnl") {
        const a = left.pnl_usd == null ? -Infinity : Number(left.pnl_usd);
        const b = right.pnl_usd == null ? -Infinity : Number(right.pnl_usd);
        if (a !== b) return b - a;
      }
      const a = liquidityBig(left.liquidity), b = liquidityBig(right.liquidity);
      return a > b ? -1 : a < b ? 1 : Number(left.lo) - Number(right.lo);
    });
    const collapsed = [...dust, ...empty];
    if (state.selectedParticipantId && !participants.some((item) => participantId(item) === state.selectedParticipantId)) {
      state.selectedParticipantId = null;
    }
    const identifiedShare = participants.reduce((sum, participant) => sum + (Number(participant.active_share_pct) || 0), 0);
    text(elements.positionsSummary, `${material.length} RANGES`);
    elements.positionsSummary.title = `${accountingIntervalLabel(detail, participants)} · ${identifiedShare.toFixed(1)}% of active liquidity identified`;
    if (ordered.length) {
      elements.positionsList.replaceChildren(...ordered.map((participant) => participantRow(participant)));
    } else {
      const row = el("tr");
      const cell = el("td", "", "NO INDEXED ACTIVE POSITIONS");
      cell.colSpan = 7;
      row.append(cell);
      elements.positionsList.replaceChildren(row);
    }
    text(elements.historicalPositionsCount, collapsed.length);
    elements.historicalPositionsDetails.classList.toggle("is-hidden", !collapsed.length);
    elements.historicalPositionsList.replaceChildren(...collapsed.map((participant) => participantRow(participant)));
  }

  function participantRow(participant) {
    const id = participantId(participant);
    const selected = state.selectedParticipantId === id;
    const row = el("tr", `position-row${participant.ours ? " is-primary" : ""}${selected ? " is-selected" : ""}`);
    row.tabIndex = 0;
    row.title = participant.manager || participant.owner || id;
    const low = tickToDisplayPrice(participant.lo);
    const high = tickToDisplayPrice(participant.hi);
    const edge = low > 0 && high > 0
      ? `${formatPrice(Math.min(low, high))} ↔ ${formatPrice(Math.max(low, high))}`
      : `ticks ${Number(participant.lo).toLocaleString()} ↔ ${Number(participant.hi).toLocaleString()}`;

    const owner = el("td");
    owner.append(el("span", "participant-value", participantOwnerLabel(participant)));
    const range = el("td");
    range.append(el("span", "participant-value", edge), el("span", "participant-subvalue", `ticks ${Number(participant.lo).toLocaleString()}…${Number(participant.hi).toLocaleString()}`));
    const principal = el("td", "", participant.value_usd == null ? "Unknown" : formatUsd(participant.value_usd));
    const liquidity = el("td");
    liquidity.append(
      el("span", "participant-value", `L ${formatCompact(participant.liquidity)}`),
      el("span", "participant-subvalue", participant.active_share_pct == null ? "Active share unknown" : `${Number(participant.active_share_pct).toLocaleString(undefined, { maximumFractionDigits: 6 })}% at spot`)
    );
    const fees = el("td");
    const earned = participant.fees_earned_usd == null
      ? participantTokenAmounts(participant, "fees_earned")
      : formatUsd(participant.fees_earned_usd);
    const claimable = participant.uncollected_usd == null
      ? participantTokenAmounts(participant, "uncollected")
      : formatUsd(participant.uncollected_usd);
    fees.append(
      el("span", "participant-value", earned),
      el("span", "participant-subvalue", `Claimable ${claimable}`)
    );
    fees.title = `Fees earned ${participantBaseline(participant)}. Claimable includes unpaid fees and any removed principal.`;
    const pnl = el("td");
    pnl.append(
      el("span", "participant-value", participant.pnl_usd == null ? "Unknown" : `${Number(participant.pnl_usd) > 0 ? "+" : ""}${formatUsd(participant.pnl_usd)}`),
      el("span", "participant-subvalue", participant.pnl_usd == null ? String(participant.accounting_status || "Baseline unavailable") : participantBaseline(participant))
    );
    const action = el("td");
    if (participant.ours) {
      const owned = (state.detail.positions || []).find((position) =>
        Number(position.lo) === Number(participant.lo) && Number(position.hi) === Number(participant.hi));
      if (owned && liquidityBig(owned.liquidity) > 0n) {
        const load = el("button", "position-action", "ACTIONS");
        load.type = "button";
        load.addEventListener("click", (event) => { event.stopPropagation(); loadPosition(owned); });
        action.append(load);
      }
    }
    const select = () => {
      state.selectedParticipantId = state.selectedParticipantId === id ? null : id;
      state.selectedEventId = null;
      renderParticipants(state.detail);
      chart.selectParticipant(state.selectedParticipantId);
    };
    row.addEventListener("click", select);
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") { event.preventDefault(); select(); }
    });
    row.append(owner, range, principal, liquidity, fees, pnl, action);
    return row;
  }


  function loadPosition(position) {
    elements.tickLower.value = String(position.lo);
    elements.tickUpper.value = String(position.hi);
    syncRangesFromTicks();
    if (state.action === "add") setAction("remove");
    invalidateSimulation("Position changed");
    chart.requestDraw();
    openActionDrawer();
  }

  function eventKey(event, index = 0) {
    const blockHash = String(event.block_hash || "").toLowerCase();
    const txHash = String(event.tx_hash || "").toLowerCase();
    const logIndex = event.log_index;
    if (blockHash && txHash && logIndex != null) return `${blockHash}:${txHash}:${logIndex}`;
    if (event.id) return String(event.id);
    if (txHash && logIndex != null) return `${txHash}:${logIndex}`;
    return `${txHash || "event"}:${event.kind || "unknown"}:${event.block || 0}:${index}`;
  }

  function groupedLpEvents(detail) {
    const supplied = Array.isArray(detail.lp_events) ? detail.lp_events : [];
    const rawSource = supplied.length ? supplied : (Array.isArray(detail.swaps) ? detail.swaps.map((swap) => ({
      ...swap,
      kind: "swap",
      tick_after: swap.tick,
      price_after: swap.price
    })) : []);
    const unique = new Map();
    rawSource.forEach((event, index) => {
      const identity = eventKey(event, index);
      unique.set(identity, unique.has(identity) ? { ...unique.get(identity), ...event } : event);
    });
    const source = [...unique.values()];
    const byTransaction = new Map();
    source.forEach((event, index) => {
      const transaction = event.tx_hash || `single:${eventKey(event, index)}`;
      if (!byTransaction.has(transaction)) byTransaction.set(transaction, []);
      byTransaction.get(transaction).push(event);
    });
    const consumed = new Set();
    const grouped = [];
    source.forEach((event, index) => {
      if (consumed.has(event)) return;
      const transaction = event.tx_hash || `single:${eventKey(event, index)}`;
      const peers = byTransaction.get(transaction) || [event];
      const rangeMoves = peers.filter((item) => item.kind === "add" || item.kind === "remove");
      const owners = new Set(rangeMoves.map((item) => String(item.owner || "").toLowerCase()));
      if (rangeMoves.length > 1 && owners.size === 1 && !owners.has("") && rangeMoves.some((item) => item.kind === "add") && rangeMoves.some((item) => item.kind === "remove")) {
        rangeMoves.forEach((item) => consumed.add(item));
        grouped.push({
          ...rangeMoves[0],
          id: `rebalance:${transaction}`,
          kind: "rebalance",
          moves: rangeMoves,
          ranges_before: rangeMoves.filter((item) => item.kind === "remove").map((item) => [item.lo, item.hi]),
          ranges_after: rangeMoves.filter((item) => item.kind === "add").map((item) => [item.lo, item.hi]),
          amount0: rangeMoves.every((item) => item.amount0 != null) ? rangeMoves.reduce((sum, item) => sum + Number(item.amount0), 0) : null,
          amount1: rangeMoves.every((item) => item.amount1 != null) ? rangeMoves.reduce((sum, item) => sum + Number(item.amount1), 0) : null,
          share_before_pct: rangeMoves.find((item) => item.share_before_pct != null)?.share_before_pct,
          share_after_pct: [...rangeMoves].reverse().find((item) => item.share_after_pct != null)?.share_after_pct
        });
        peers.filter((item) => !rangeMoves.includes(item)).forEach((item) => {
          consumed.add(item);
          grouped.push(item);
        });
      } else {
        consumed.add(event);
        grouped.push(event);
      }
    });
    return grouped;
  }

  function eventOwner(event) {
    const address = event.owner || (event.ours ? state.owner : null);
    return shortAddress(address) === "—" ? "A trader" : shortAddress(address);
  }
  function rangePair(range) {
    if (Array.isArray(range)) return [range[0], range[1]];
    if (range && typeof range === "object") return [range.lo ?? range.lower, range.hi ?? range.upper];
    return [null, null];
  }

  function eventRanges(event, field) {
    const supplied = Array.isArray(event[field]) ? event[field].map(rangePair) : [];
    if (supplied.length) return supplied;
    const include = field === "ranges_before"
      ? event.kind === "remove"
      : event.kind === "add" || event.kind === "collect" || event.kind === "checkpoint";
    return include ? [[event.lo, event.hi]] : [];
  }


  function eventAmounts(event) {
    const pool = state.detail.pool || {};
    const token0 = tokenLabel(pool.token0);
    const token1 = tokenLabel(pool.token1);
    const a0 = event.amount0 == null ? null : Math.abs(Number(event.amount0));
    const a1 = event.amount1 == null ? null : Math.abs(Number(event.amount1));
    const parts = [];
    if (Number.isFinite(a0)) parts.push(`${formatCompact(a0)} ${token0}`);
    if (Number.isFinite(a1)) parts.push(`${formatCompact(a1)} ${token1}`);
    return parts.join(" + ") || "amounts unavailable";
  }

  function eventRange(event) {
    const ranges = event.kind === "rebalance"
      ? [...eventRanges(event, "ranges_before"), ...eventRanges(event, "ranges_after")]
      : [[event.lo, event.hi]];
    const valid = ranges.filter((range) =>
      range[0] != null && range[1] != null && Number.isFinite(Number(range[0])) && Number.isFinite(Number(range[1])));
    if (!valid.length) return "range unavailable";
    return valid.map(([lo, hi]) => {
      const a = tickToDisplayPrice(lo), b = tickToDisplayPrice(hi);
      return a > 0 && b > 0 ? `${formatPrice(Math.min(a, b))}–${formatPrice(Math.max(a, b))}` : `ticks ${lo}…${hi}`;
    }).join(" → ");
  }

  function shareEffect(event) {
    if (event.share_before_pct == null || event.share_after_pct == null) return "SHARE —";
    const before = Number(event.share_before_pct), after = Number(event.share_after_pct);
    if (!Number.isFinite(before) || !Number.isFinite(after)) return "SHARE —";
    const direction = after > before ? "CONCENTRATED" : after < before ? "DILUTED" : "UNCHANGED";
    if (before !== after && formatCompact(before) === formatCompact(after)) {
      const basisPoints = Math.abs(after - before) * 100;
      return `SHARE ${formatCompact(after)}% · ${direction} ${basisPoints < .01 ? "<0.01" : formatCompact(basisPoints)} BP`;
    }
    return `SHARE ${formatCompact(before)}% → ${formatCompact(after)}% · ${direction}`;
  }

  function eventCopy(event) {
    const owner = eventOwner(event);
    const kind = String(event.kind || "event").toLowerCase();
    if (kind === "swap") {
      const pool = state.detail.pool || {};
      const token0 = tokenLabel(pool.token0);
      const token1 = tokenLabel(pool.token1);
      const amount0 = Number(event.amount0), amount1 = Number(event.amount1);
      let direction = eventAmounts(event);
      if (Number.isFinite(amount0) && Number.isFinite(amount1)) {
        direction = amount0 > 0
          ? `${formatCompact(Math.abs(amount0))} ${token0} → ${formatCompact(Math.abs(amount1))} ${token1}`
          : `${formatCompact(Math.abs(amount1))} ${token1} → ${formatCompact(Math.abs(amount0))} ${token0}`;
      }
      const before = Number(event.price_before), after = Number(event.price_after);
      const displayedBefore = chart.inverted && before > 0 ? 1 / before : before;
      const displayedAfter = chart.inverted && after > 0 ? 1 / after : after;
      const move = displayedBefore > 0 && displayedAfter > 0
        ? `${formatPrice(displayedBefore)} → ${formatPrice(displayedAfter)}`
        : "—";
      return { title: `SWAP · ${direction}`, detail: `PRICE ${move}` };
    }
    if (kind === "rebalance") return { title: `${owner} · REBALANCE`, detail: `${eventRange(event)} · ${shareEffect(event)}` };
    if (kind === "add") return { title: `${owner} · +L · ${eventAmounts(event)}`, detail: `${eventRange(event)} · ${shareEffect(event)}` };
    if (kind === "remove") return { title: `${owner} · -L · ${eventAmounts(event)}`, detail: `${eventRange(event)} · ${shareEffect(event)}` };
    if (kind === "collect") return { title: `${owner} · COLLECT · ${eventAmounts(event)}`, detail: "LIQUIDITY UNCHANGED" };
    if (kind === "checkpoint") return { title: `${owner} · FEE CHECKPOINT`, detail: `${eventRange(event)} · LIQUIDITY UNCHANGED` };
    return { title: `${owner} · LP CHANGE`, detail: `${eventRange(event)} · ${shareEffect(event)}` };
  }

  function renderEvents(detail) {
    const events = groupedLpEvents(detail).sort((left, right) =>
      (Number(right.block) || 0) - (Number(left.block) || 0)
      || (timestampMs(right.timestamp) || 0) - (timestampMs(left.timestamp) || 0));
    const list = state.flowFilter === "lp" ? events.filter((event) => event.kind !== "swap") : events;
    if (state.selectedEventId && !list.some((event, index) => eventKey(event, index) === state.selectedEventId)) {
      state.selectedEventId = null;
    }
    const detailBlock = Number(detail.block) || 0;
    text(elements.swapCount, detailBlock > 0 ? `#${detailBlock.toLocaleString()} · ${list.length} EVENTS` : `${list.length} EVENTS`);
    const renderKey = JSON.stringify([state.flowFilter, state.selectedEventId, chart.inverted, events]);
    if (renderKey === state.eventRenderKey) return;
    state.eventRenderKey = renderKey;
    if (!list.length) {
      elements.swapsList.replaceChildren(el("p", "empty-copy", state.flowFilter === "lp" ? "WAITING FOR LP ACTIVITY" : "WAITING FOR EVENTS"));
      return;
    }
    const nodes = list.slice(0, 48).map((event, index) => {
      const key = eventKey(event, index);
      const copy = eventCopy(event);
      const button = el("button", `swap-item${state.selectedEventId === key ? " is-selected" : ""}`);
      button.type = "button";
      button.title = event.tx_hash || event.id || "";
      const side = el("i", `swap-side${event.kind === "swap" ? " is-swap" : event.kind === "remove" || event.kind === "collect" ? " is-negative" : ""}`);
      const body = el("span");
      body.append(el("strong", "", copy.title), el("p", "", copy.detail));
      button.append(side, body, el("time", "", `#${event.block == null ? "—" : Number(event.block).toLocaleString()} · ${formatTime(event.timestamp)}`));
      button.addEventListener("click", () => {
        state.selectedEventId = state.selectedEventId === key ? null : key;
        state.selectedParticipantId = null;
        renderEvents(state.detail);
        renderParticipants(state.detail);
        chart.selectEvent(state.selectedEventId ? event : null);
      });
      return button;
    });
    elements.swapsList.replaceChildren(...nodes);
  }


  function configureActionRange(detail) {
    if (detail.spot == null || detail.spot.tick == null) return;
    const curve = detail.liquidity && Array.isArray(detail.liquidity.curve) ? detail.liquidity.curve : [];
    const ticks = curve.map((point) => Number(point.tick)).filter(Number.isFinite).sort((a, b) => a - b);
    const spotTick = detail.spot && Number(detail.spot.tick);
    const spacing = Number(detail.pool && detail.pool.tick_spacing);
    const step = Number.isInteger(spacing) && spacing > 0 ? spacing : 1;
    const centerTick = Number.isFinite(spotTick) ? spotTick : ticks.length ? ticks[Math.floor(ticks.length / 2)] : 0;
    const center = Math.floor(centerTick / step) * step;
    let min = ticks.length ? ticks[0] : center - 100 * step;
    let max = ticks.length ? ticks[ticks.length - 1] : center + 100 * step;
    if (min === max) { min -= 100 * step; max += 100 * step; }
    const pad = Math.max(step * 2, Math.round((max - min) * .1));
    min = Math.max(Math.ceil(-887272 / step) * step, Math.floor((min - pad) / step) * step);
    max = Math.min(Math.floor(887272 / step) * step, Math.ceil((max + pad) / step) * step);
    [elements.rangeLower, elements.rangeUpper].forEach((input) => {
      input.min = String(min);
      input.max = String(max);
      input.step = String(step);
    });
    [elements.tickLower, elements.tickUpper].forEach((input) => {
      input.min = String(Math.ceil(-MAX_TICK / step) * step);
      input.max = String(Math.floor(MAX_TICK / step) * step);
      input.step = String(step);
    });
    if (!state.actionTicksInitialized) {
      const radius = Math.max(step * 3, Math.ceil(60 / step) * step);
      const largest = (detail.positions || []).reduce((best, position) =>
        !best || BigInt(position.liquidity || "0") > BigInt(best.liquidity || "0") ? position : best, null);
      elements.tickLower.value = String(largest ? largest.lo : Math.max(min, center - radius));
      elements.tickUpper.value = String(largest ? largest.hi : Math.min(max, center + radius));
      elements.rangeLower.value = elements.tickLower.value;
      elements.rangeUpper.value = elements.tickUpper.value;
      state.actionTicksInitialized = true;
    } else {
      syncRangesFromTicks();
    }
    const pool = detail.pool || {};
    text(elements.amount0Label, `${tokenLabel(pool.token0)} amount`);
    text(elements.amount1Label, `${tokenLabel(pool.token1)} amount`);
    updateRangePrices();
  }

  function tickToRawPrice(tick) {
    if (!state.detail || !state.detail.pool) return null;
    const pool = state.detail.pool;
    const decimals0 = pool.token0 && pool.token0.decimals;
    const decimals1 = pool.token1 && pool.token1.decimals;
    const target = Number(tick);
    if (decimals0 == null || decimals1 == null || !Number.isFinite(target)) return null;
    const value = Math.pow(1.0001, target) * Math.pow(10, Number(decimals0) - Number(decimals1));
    return Number.isFinite(value) && value > 0 ? value : null;
  }

  function tickToDisplayPrice(tick) {
    const raw = tickToRawPrice(tick);
    return raw && chart.inverted ? 1 / raw : raw;
  }

  function updateRangePrices() {
    const spans = elements.rangePrices.querySelectorAll("span");
    const low = tickToDisplayPrice(elements.tickLower.value);
    const high = tickToDisplayPrice(elements.tickUpper.value);
    if (spans[0]) spans[0].textContent = low ? formatPrice(low) : "Lower unavailable";
    if (spans[1]) spans[1].textContent = high ? formatPrice(high) : "Upper unavailable";
  }

  function syncRangesFromTicks() {
    const min = Number(elements.rangeLower.min);
    const max = Number(elements.rangeLower.max);
    const lower = Math.max(min, Math.min(max, Number(elements.tickLower.value)));
    const upper = Math.max(min, Math.min(max, Number(elements.tickUpper.value)));
    if (Number.isFinite(lower)) elements.rangeLower.value = String(lower);
    if (Number.isFinite(upper)) elements.rangeUpper.value = String(upper);
    updateRangePrices();
  }

  function syncTicksFromRanges(changed) {
    let lower = Number(elements.rangeLower.value);
    let upper = Number(elements.rangeUpper.value);
    const step = Math.max(1, Number(elements.rangeLower.step) || 1);
    if (lower >= upper) {
      if (changed === "lower") lower = upper - step;
      else upper = lower + step;
    }
    elements.rangeLower.value = String(lower);
    elements.rangeUpper.value = String(upper);
    elements.tickLower.value = String(lower);
    elements.tickUpper.value = String(upper);
    updateRangePrices();
  }

  function poolToolsOpen() {
    return Boolean(elements.poolTools && elements.poolTools.open);
  }

  function pausePoolTools() {
    window.clearTimeout(state.allocationTimer);
    state.allocationTimer = null;
    if (state.allocationController) state.allocationController.abort();
    state.allocationController = null;
    state.allocationRevision += 1;
    chart.requestDraw();
  }

  function initializePoolTools() {
    if (!poolToolsOpen() || !state.detail) return;
    configureAllocationEditor(state.detail);
  }

  function resetAllocationPreview() {
    window.clearTimeout(state.allocationTimer);
    state.allocationTimer = null;
    if (state.allocationController) state.allocationController.abort();
    state.allocationController = null;
    state.allocationRevision += 1;
    state.allocationTicksInitialized = false;
    state.allocationPreview = null;
    state.allocationResult = null;
    state.allocationSpotKey = null;
    text(elements.allocPositionCount, "—");
    elements.allocBandsList.replaceChildren();
    elements.allocStatus.classList.remove("is-error");
    text(elements.allocStatus, "Waiting for a real pool price.");
    elements.allocHistory.replaceChildren(
      (() => {
        const header = el("header");
        header.append(el("b", "", "REAL SWAP REPLAY"), el("span", "", "WAITING"));
        return header;
      })(),
      el("p", "", "Persisted swap coverage has not loaded.")
    );
  }

  function allocationSupported() {
    const kind = String(state.detail && state.detail.pool && state.detail.pool.kind || "").toLowerCase();
    return kind === "v3" || kind === "v4";
  }
  function allocationSpotReady(detail = state.detail) {
    const pool = detail && detail.pool || {};
    const spot = detail && detail.spot || {};
    const tick = Number(spot.tick);
    const sqrtPriceX96 = Number(spot.sqrt_price_x96);
    const decimals0 = Number(pool.token0 && pool.token0.decimals);
    const decimals1 = Number(pool.token1 && pool.token1.decimals);
    return Number.isFinite(tick)
      && sqrtPriceX96 > 0 && Number.isFinite(sqrtPriceX96)
      && Number.isInteger(decimals0) && Number.isInteger(decimals1);
  }


  function allocationSpacing() {
    const spacing = Number(state.detail && state.detail.pool && state.detail.pool.tick_spacing);
    return Number.isInteger(spacing) && spacing > 0 ? spacing : 1;
  }

  function allocationBounds() {
    const spacing = allocationSpacing();
    return {
      min: Math.ceil(-MAX_TICK / spacing) * spacing,
      max: Math.floor(MAX_TICK / spacing) * spacing,
      spacing
    };
  }

  function snapAllocationTick(value, direction = "nearest") {
    const bounds = allocationBounds();
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return null;
    const units = numeric / bounds.spacing;
    const snappedUnits = direction === "down" ? Math.floor(units) : direction === "up" ? Math.ceil(units) : Math.round(units);
    return Math.max(bounds.min, Math.min(bounds.max, snappedUnits * bounds.spacing));
  }

  function currentRawPrice() {
    const spot = state.detail && state.detail.spot || {};
    const value = Number(spot.price_token1_per_token0);
    if (value > 0 && Number.isFinite(value)) return value;
    return allocationSpotReady() ? tickToRawPrice(spot.tick) : null;
  }

  function currentDisplayPrice() {
    const raw = currentRawPrice();
    return raw && chart.inverted ? 1 / raw : raw;
  }

  function editableNumber(value, significant = 11) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "";
    if (number === 0) return "0";
    if (Math.abs(number) >= 1e9 || Math.abs(number) < 1e-7) return number.toExponential(Math.max(1, significant - 1)).replace(/\.?0+e/, "e");
    return Number(number.toPrecision(significant)).toString();
  }

  function displayPriceToTick(value) {
    const displayed = Number(value);
    if (!(displayed > 0) || !Number.isFinite(displayed) || !state.detail) return null;
    const pool = state.detail.pool || {};
    const decimals0 = Number(pool.token0 && pool.token0.decimals);
    const decimals1 = Number(pool.token1 && pool.token1.decimals);
    if (!Number.isInteger(decimals0) || !Number.isInteger(decimals1)) return null;
    const raw = chart.inverted ? 1 / displayed : displayed;
    const unscaled = raw / Math.pow(10, decimals0 - decimals1);
    if (!(unscaled > 0) || !Number.isFinite(unscaled)) return null;
    const tick = Math.log(unscaled) / Math.log(1.0001);
    return Number.isFinite(tick) ? tick : null;
  }

  function allocationTicks() {
    return {
      lower: Number(elements.allocTickLower.value),
      upper: Number(elements.allocTickUpper.value)
    };
  }

  function setAllocationTicks(lowerValue, upperValue, changed = null, { request = true } = {}) {
    const bounds = allocationBounds();
    let lower = snapAllocationTick(lowerValue);
    let upper = snapAllocationTick(upperValue);
    if (lower == null || upper == null) return false;
    if (lower >= upper) {
      if (changed === "lower") lower = Math.max(bounds.min, upper - bounds.spacing);
      else if (changed === "upper") upper = Math.min(bounds.max, lower + bounds.spacing);
      else {
        lower = Math.max(bounds.min, Math.min(lower, upper) - bounds.spacing);
        upper = Math.min(bounds.max, Math.max(lowerValue, upperValue) + bounds.spacing);
      }
    }
    if (lower >= upper) return false;
    elements.allocTickLower.value = String(lower);
    elements.allocTickUpper.value = String(upper);
    state.allocationTicksInitialized = true;
    updateAllocationBoundaryFields();
    refreshImmediateAllocation();
    if (request) scheduleAllocationRequest();
    chart.requestDraw();
    return true;
  }

  function updateAllocationBoundaryFields() {
    if (!state.detail || !state.allocationTicksInitialized) return;
    const ticks = allocationTicks();
    const spot = currentDisplayPrice();
    const entries = [
      ["lower", ticks.lower, elements.allocPriceLower, elements.allocPctLower],
      ["upper", ticks.upper, elements.allocPriceUpper, elements.allocPctUpper]
    ];
    for (const [, tick, priceInput, percentInput] of entries) {
      const price = tickToDisplayPrice(tick);
      priceInput.value = editableNumber(price);
      percentInput.value = price > 0 && spot > 0 ? editableNumber((price / spot - 1) * 100, 8) : "";
    }
  }

  function applyAllocationPreset(preset, { request = true } = {}) {
    if (!state.detail || !allocationSupported()) return;
    const bounds = allocationBounds();
    const spot = currentDisplayPrice();
    let lower, upper;
    if (preset === "full") {
      lower = bounds.min;
      upper = bounds.max;
    } else if (spot > 0) {
      const width = preset === "wide" ? 0.10 : 0.02;
      const first = displayPriceToTick(spot * (1 - width));
      const second = displayPriceToTick(spot * (1 + width));
      lower = snapAllocationTick(Math.min(first, second), "down");
      upper = snapAllocationTick(Math.max(first, second), "up");
    }
    if (!Number.isFinite(lower) || !Number.isFinite(upper)) {
      const spotTick = Number(state.detail.spot && state.detail.spot.tick);
      const center = snapAllocationTick(spotTick) || 0;
      const radius = Math.max(bounds.spacing, Math.ceil(60 / bounds.spacing) * bounds.spacing);
      lower = Math.max(bounds.min, center - radius);
      upper = Math.min(bounds.max, center + radius);
    }
    elements.allocPresets.querySelectorAll("button[data-preset]").forEach((button) => {
      button.classList.toggle("is-active", button.dataset.preset === preset);
    });
    setAllocationTicks(lower, upper, null, { request });
  }

  function configureAllocationEditor(detail) {
    if (!poolToolsOpen()) return;
    const supported = allocationSupported();
    const ready = supported && allocationSpotReady(detail);
    const controls = [
      elements.allocCapital, elements.allocShape, elements.allocBands, elements.allocHistoryWindow,
      elements.allocPriceLower, elements.allocTickLower, elements.allocPctLower,
      elements.allocPriceUpper, elements.allocTickUpper, elements.allocPctUpper,
      elements.allocEntryGas, elements.allocExitGas
    ];
    controls.forEach((control) => { control.disabled = !ready || (control === elements.allocBands && elements.allocShape.value === "uniform"); });
    elements.allocPresets.querySelectorAll("button").forEach((button) => { button.disabled = !ready; });
    if (!supported) {
      const hadPreview = state.allocationPreview !== null;
      state.allocationPreview = null;
      text(elements.allocPositionCount, "UNAVAILABLE");
      elements.allocStatus.classList.add("is-error");
      text(elements.allocStatus, "V2 uses reserve shares rather than independent tick ranges. The range allocator supports analytical V3/V4 previews only.");
      elements.allocBandsList.replaceChildren();
      if (hadPreview) chart.requestDraw();
      return;
    }
    if (!ready) {
      text(elements.allocPositionCount, "WAITING");
      elements.allocStatus.classList.remove("is-error");
      text(elements.allocStatus, "Waiting for the selected pool's real tick, sqrt price and token decimals.");
      return;
    }
    const bounds = allocationBounds();
    [elements.allocTickLower, elements.allocTickUpper].forEach((input) => {
      input.min = String(bounds.min);
      input.max = String(bounds.max);
      input.step = String(bounds.spacing);
    });
    const spot = detail && detail.spot || {};
    const spotKey = `${spot.sqrt_price_x96 || ""}:${spot.tick ?? ""}`;
    const spotChanged = state.allocationSpotKey !== null && state.allocationSpotKey !== spotKey;
    state.allocationSpotKey = spotKey;
    if (!state.allocationTicksInitialized) {
      applyAllocationPreset("narrow");
    } else if (spotChanged) {
      elements.allocPresets.querySelectorAll("button").forEach((button) => button.classList.remove("is-active"));
      updateAllocationBoundaryFields();
      scheduleAllocationRequest({ marketUpdate: true });
    }
  }

  function allocationRanges(lower, upper, count) {
    const spacing = allocationSpacing();
    const units = Math.round((upper - lower) / spacing);
    const actual = Math.max(1, Math.min(MAX_ALLOCATION_BANDS, count, units));
    const boundaries = [];
    for (let index = 0; index < actual; index += 1) {
      boundaries.push(lower + Math.floor(units * index / actual) * spacing);
    }
    boundaries.push(upper);
    return boundaries.slice(0, -1).map((tick, index) => [tick, boundaries[index + 1]]).filter(([lo, hi]) => lo < hi);
  }

  function allocationWeights(shape, count) {
    if (shape === "uniform" || count === 1) return Array(count).fill(1);
    const center = count - 1;
    return Array.from({ length: count }, (_, index) => (
      shape === "curve" ? count - Math.abs(2 * index - center) : 1 + Math.abs(2 * index - center)
    ));
  }

  function clientAmountText(value) {
    if (value == null) return null;
    const number = Number(value);
    if (!(number >= 0) || !Number.isFinite(number)) return null;
    if (number === 0) return "0";
    return number.toLocaleString("en-US", {
      useGrouping: false,
      maximumSignificantDigits: 14,
      maximumFractionDigits: 18
    });
  }

  function buildImmediateAllocation() {
    if (!poolToolsOpen() || !state.detail || !allocationSupported() || !state.allocationTicksInitialized) return null;
    const pool = state.detail.pool || {};
    const spotTick = Number(state.detail.spot && state.detail.spot.tick);
    const capital = Number(elements.allocCapital.value);
    const shape = elements.allocShape.value;
    const requested = shape === "uniform" ? 1 : Math.max(2, Math.min(MAX_ALLOCATION_BANDS, Math.round(Number(elements.allocBands.value) || 5)));
    const ticks = allocationTicks();
    if (!(capital > 0) || !Number.isFinite(capital) || !Number.isFinite(spotTick) || ticks.lower >= ticks.upper) return null;
    const sqrtWire = Number(state.detail.spot && state.detail.spot.sqrt_price_x96);
    const sqrt = sqrtWire > 0 && Number.isFinite(sqrtWire) ? sqrtWire / Math.pow(2, 96) : Math.pow(1.0001, spotTick / 2);
    if (!(sqrt > 0) || !Number.isFinite(sqrt)) return null;
    const token0 = String(pool.token0 && pool.token0.address || "").toLowerCase();
    const token1 = String(pool.token1 && pool.token1.address || "").toLowerCase();
    const stableSide = token0 === USDG ? 0 : token1 === USDG ? 1 : null;
    const decimals0 = Number(pool.token0 && pool.token0.decimals);
    const decimals1 = Number(pool.token1 && pool.token1.decimals);
    const stableDecimals = stableSide === 0 ? decimals0 : stableSide === 1 ? decimals1 : null;
    const ranges = allocationRanges(ticks.lower, ticks.upper, requested);
    const weights = allocationWeights(shape, ranges.length);
    const weightTotal = weights.reduce((sum, weight) => sum + weight, 0);
    let total0 = 0, total1 = 0;
    const bands = ranges.map(([lower, upper], index) => {
      const budget = capital * weights[index] / weightTotal;
      let liquidity = null, amount0 = null, amount1 = null;
      if (stableSide != null && Number.isInteger(decimals0) && Number.isInteger(decimals1)) {
        const sa = Math.pow(1.0001, lower / 2);
        const sb = Math.pow(1.0001, upper / 2);
        const clamped = Math.max(sa, Math.min(sb, sqrt));
        const unit0 = clamped < sb ? (sb - clamped) / (clamped * sb) : 0;
        const unit1 = clamped > sa ? clamped - sa : 0;
        const rawPrice = sqrt * sqrt;
        const costPerL = stableSide === 0 ? unit0 + unit1 / rawPrice : unit0 * rawPrice + unit1;
        const budgetRaw = budget * Math.pow(10, stableDecimals);
        if (costPerL > 0 && Number.isFinite(costPerL) && Number.isFinite(budgetRaw)) {
          liquidity = budgetRaw / costPerL;
          amount0 = unit0 * liquidity / Math.pow(10, decimals0);
          amount1 = unit1 * liquidity / Math.pow(10, decimals1);
          total0 += amount0;
          total1 += amount1;
        }
      }
      return {
        index: index + 1,
        tick_lower: lower,
        tick_upper: upper,
        weight_pct: weights[index] / weightTotal * 100,
        capital_usd: budget,
        liquidity,
        amount0: clientAmountText(amount0),
        amount1: clientAmountText(amount1),
        in_range: lower <= spotTick && spotTick < upper,
        side: spotTick < lower ? "token0_only" : spotTick >= upper ? "token1_only" : "two_sided"
      };
    });
    let token0Pct = null, token1Pct = null;
    if (stableSide != null) {
      const rawPrice = currentRawPrice();
      const value0 = stableSide === 0 ? total0 : total0 * rawPrice;
      const value1 = stableSide === 1 ? total1 : total1 / rawPrice;
      const total = value0 + value1;
      if (total > 0) {
        token0Pct = value0 / total * 100;
        token1Pct = value1 / total * 100;
      }
    }
    return {
      source: "browser",
      shape,
      actual_positions: bands.length,
      bands,
      capital: { usd: elements.allocCapital.value, basis: stableSide == null ? "unavailable" : "USDG", valuation_available: stableSide != null },
      totals: {
        amount0: clientAmountText(total0),
        amount1: clientAmountText(total1),
        token0_pct: token0Pct,
        token1_pct: token1Pct
      }
    };
  }

  function renderAllocationPlan(plan, statusCopy = null) {
    const chartBands = (value) => (Array.isArray(value && value.bands) ? value.bands : [])
      .map((band) => [band.tick_lower, band.tick_upper]);
    const chartChanged = JSON.stringify(chartBands(state.allocationPreview)) !== JSON.stringify(chartBands(plan));
    state.allocationPreview = plan;
    const pool = state.detail && state.detail.pool || {};
    const token0 = tokenLabel(pool.token0);
    const token1 = tokenLabel(pool.token1);
    const positions = Number(plan && plan.actual_positions) || 0;
    text(elements.allocPositionCount, `${positions} REAL POSITION${positions === 1 ? "" : "S"}`);
    const split = elements.allocSplit.querySelectorAll("div");
    const totals = plan && plan.totals || {};
    if (split[0]) {
      text(split[0].querySelector("span"), token0);
      text(split[0].querySelector("strong"), totals.amount0 == null ? "UNAVAILABLE" : formatCompact(totals.amount0));
      text(split[0].querySelector("small"), totals.token0_pct == null ? "USDG SPLIT UNAVAILABLE" : `${Number(totals.token0_pct).toFixed(2)}% OF CAPITAL`);
      split[0].querySelector("strong").title = totals.amount0 == null ? "" : String(totals.amount0);
    }
    if (split[1]) {
      text(split[1].querySelector("span"), token1);
      text(split[1].querySelector("strong"), totals.amount1 == null ? "UNAVAILABLE" : formatCompact(totals.amount1));
      text(split[1].querySelector("small"), totals.token1_pct == null ? "USDG SPLIT UNAVAILABLE" : `${Number(totals.token1_pct).toFixed(2)}% OF CAPITAL`);
      split[1].querySelector("strong").title = totals.amount1 == null ? "" : String(totals.amount1);
    }
    const exactPlan = plan && plan.source === "server_exact";
    const bands = Array.isArray(plan && plan.bands) ? plan.bands : [];
    elements.allocBandsList.replaceChildren(...bands.map((band) => {
      const row = el("div", "alloc-band");
      row.append(el("b", "", `#${String(band.index).padStart(2, "0")}`));
      const copy = el("div", "alloc-band-copy");
      const low = tickToDisplayPrice(band.tick_lower), high = tickToDisplayPrice(band.tick_upper);
      const budget = band.capital_usd == null ? "USDG unavailable" : `${formatUsd(band.capital_usd)} · ${Number(band.weight_pct).toFixed(2)}%`;
      const side = String(band.side || "").replaceAll("_", " ").toUpperCase();
      copy.append(
        el("strong", "", `${formatPrice(Math.min(low, high))} ↔ ${formatPrice(Math.max(low, high))} · ${budget}`),
        el("span", "", `ticks ${Number(band.tick_lower).toLocaleString()}…${Number(band.tick_upper).toLocaleString()} · ${side} · ${token0} ${band.amount0 == null ? "—" : formatCompact(band.amount0)} / ${token1} ${band.amount1 == null ? "—" : formatCompact(band.amount1)}`)
      );
      const load = el("button", "", exactPlan ? "LOAD ACTION" : "VERIFYING");
      load.type = "button";
      load.disabled = !exactPlan;
      if (exactPlan) load.addEventListener("click", () => loadAllocationBand(band));
      row.append(copy, load);
      return row;
    }));
    elements.allocStatus.classList.remove("is-error");
    text(elements.allocStatus, statusCopy || (
      exactPlan
        ? `${positions} exact independent positions · sized at tick ${Number(plan.spot.tick).toLocaleString()} · floor-rounded budgets · analytical only`
        : plan && plan.capital && plan.capital.valuation_available === false
          ? "Ranges are real, but token budgets are unavailable because this pool has no direct USDG leg."
          : "Immediate browser geometry · server floor-rounded budgets and persisted replay refresh after edits."
    ));
    if (chartChanged) chart.requestDraw();
  }

  function renderAllocationHistory(history) {
    const header = el("header");
    header.append(
      el("b", "", "REAL SWAP REPLAY"),
      el("span", "", String(history && history.state || "unavailable").toUpperCase())
    );
    if (!history || history.available !== true) {
      elements.allocHistory.replaceChildren(header, el("p", "", history && history.reason || "Persisted swap coverage is unavailable."));
      return;
    }
    const metrics = el("div", "history-metrics");
    const hasLpEstimate = history.lp_fees_usd != null;
    const feeValue = hasLpEstimate
      ? formatUsd(history.lp_fees_usd)
      : history.gross_fee_share_constant_liquidity_usd == null ? "UNAVAILABLE" : formatUsd(history.gross_fee_share_constant_liquidity_usd);
    const feePolicy = history.fee_policy || {};
    const entries = [
      [
        hasLpEstimate ? "LP FEE EST." : "GROSS FEE SHARE*",
        feeValue,
        hasLpEstimate
          ? `${Number(feePolicy.covered_swaps || 0).toLocaleString()} swaps with observed protocol cut`
          : `${Number(history.constant_liquidity_swaps || 0).toLocaleString()} constant-L swaps`
      ],
      ["DIVERGENCE VS HOLD", history.divergence_usd == null ? "UNAVAILABLE" : formatUsd(history.divergence_usd), "same ending USDG price"],
      ["USER GAS INPUT", history.costs && history.costs.total_usd != null ? formatUsd(history.costs.total_usd) : "UNAVAILABLE", "entry + exit per real position"],
      ["NET COUNTERFACTUAL", history.net_usd == null ? "UNAVAILABLE" : formatUsd(history.net_usd), history.net_usd == null ? "requires complete fees + both gas inputs" : "fee estimate + divergence − user gas"]
    ];
    for (const [label, value, note] of entries) {
      const node = el("div");
      node.append(el("span", "", label), el("strong", "", value), el("small", "", note));
      metrics.append(node);
    }
    const coverage = history.coverage || {};
    const store = coverage.store || {};
    const reasons = Array.isArray(history.unavailable_reasons) ? history.unavailable_reasons : [];
    const copy = [
      `${Number(history.swaps || 0).toLocaleString()} persisted swaps`,
      coverage.covered_from == null || coverage.covered_to == null ? "coverage timestamps unavailable" : `${formatTime(coverage.covered_from)} → ${formatTime(coverage.covered_to)}`,
      coverage.truncated || store.backfill ? "indexed history coverage partial" : "requested indexed rows covered",
      `${Number(history.crossing_overlapped_swaps || 0).toLocaleString()} in-range crossing rows excluded`,
      history.crossing_gross_fee_upper_bound_usd == null ? "crossing fee bound unavailable" : `${formatUsd(history.crossing_gross_fee_upper_bound_usd)} crossing gross-fee upper bound`,
      history.net_usd == null
        ? (reasons.join("; ") || "LP fee estimate or user-entered costs are unavailable")
        : "Exogenous recorded path · observed protocol cut · user gas inputs · not realized P&L"
    ].join(" · ");
    const limitations = el("div", "history-limitations", copy);
    elements.allocHistory.replaceChildren(header, metrics, limitations);
  }

  function applyAllocationResult(result) {
    if (!result || String(result.pool && result.pool.id || "").toLowerCase() !== state.selectedId) return;
    state.allocationResult = result;
    if (Number.isInteger(Number(result.tick_lower)) && Number.isInteger(Number(result.tick_upper))) {
      elements.allocTickLower.value = String(result.tick_lower);
      elements.allocTickUpper.value = String(result.tick_upper);
      updateAllocationBoundaryFields();
    }
    renderAllocationPlan(result);
    renderAllocationHistory(result.history);
  }

  function refreshImmediateAllocation() {
    if (!poolToolsOpen()) return;
    const plan = buildImmediateAllocation();
    if (plan) renderAllocationPlan(plan);
    else if (allocationSupported()) {
      state.allocationPreview = null;
      elements.allocStatus.classList.add("is-error");
      text(elements.allocStatus, "Enter positive USDG capital and a valid snapped lower/upper range.");
      elements.allocBandsList.replaceChildren();
      chart.requestDraw();
    }
  }

  function allocationPayload() {
    return {
      pool_id: state.selectedId,
      capital_usd: elements.allocCapital.value.trim(),
      tick_lower: Number(elements.allocTickLower.value),
      tick_upper: Number(elements.allocTickUpper.value),
      shape: elements.allocShape.value,
      bands: elements.allocShape.value === "uniform" ? 1 : Number(elements.allocBands.value),
      history_window: elements.allocHistoryWindow.value,
      entry_gas_per_position_usd: elements.allocEntryGas.value.trim() || null,
      exit_gas_per_position_usd: elements.allocExitGas.value.trim() || null
    };
  }

  function validateAllocationPayload(payload) {
    if (!allocationSupported()) return "Only V3/V4 pools have concentrated-liquidity ranges.";
    if (!AMOUNT_RE.test(payload.capital_usd) || Number(payload.capital_usd) <= 0) return "Capital must be a positive USDG decimal.";
    if (!Number.isInteger(payload.tick_lower) || !Number.isInteger(payload.tick_upper) || payload.tick_lower >= payload.tick_upper) return "Lower tick must be below upper tick.";
    if (payload.shape !== "uniform" && (!Number.isInteger(payload.bands) || payload.bands < 2 || payload.bands > MAX_ALLOCATION_BANDS)) return `Real positions must be between 2 and ${MAX_ALLOCATION_BANDS}.`;
    for (const [label, value] of [["entry", payload.entry_gas_per_position_usd], ["exit", payload.exit_gas_per_position_usd]]) {
      if (value != null && (!AMOUNT_RE.test(value) || Number(value) < 0)) return `${label} gas must be a non-negative USDG decimal or blank.`;
    }
    return null;
  }

  function scheduleAllocationRequest({ marketUpdate = false } = {}) {
    if (!poolToolsOpen()) return;
    // A busy live pool must not starve an exact request or hide its last
    // tick-labelled budget. User edits still invalidate obsolete plans.
    if (marketUpdate && (state.allocationTimer !== null || state.allocationController)) return;
    if (!marketUpdate) refreshImmediateAllocation();
    window.clearTimeout(state.allocationTimer);
    state.allocationTimer = null;
    if (state.allocationController) state.allocationController.abort();
    state.allocationController = null;
    const revision = ++state.allocationRevision;
    const payload = allocationPayload();
    const problem = validateAllocationPayload(payload);
    if (problem) {
      elements.allocStatus.classList.add("is-error");
      text(elements.allocStatus, problem);
      return;
    }
    state.allocationTimer = window.setTimeout(async () => {
      state.allocationTimer = null;
      const controller = new AbortController();
      state.allocationController = controller;
      try {
        const result = await api(ALLOCATION_API, {
          method: "POST",
          body: JSON.stringify(payload),
          signal: controller.signal
        });
        if (revision !== state.allocationRevision || state.selectedId !== payload.pool_id) return;
        applyAllocationResult(result);
      } catch (error) {
        if (error.name === "AbortError" || revision !== state.allocationRevision) return;
        elements.allocStatus.classList.add("is-error");
        text(elements.allocStatus, `Immediate allocation remains visible; persisted replay unavailable: ${errorMessage(error)}`);
        renderAllocationHistory({ available: false, state: "unavailable", reason: errorMessage(error) });
      } finally {
        if (revision === state.allocationRevision) state.allocationController = null;
      }
    }, 260);
  }

  function changeAllocationBoundary(boundary, source) {
    const priceInput = boundary === "lower" ? elements.allocPriceLower : elements.allocPriceUpper;
    const tickInput = boundary === "lower" ? elements.allocTickLower : elements.allocTickUpper;
    const percentInput = boundary === "lower" ? elements.allocPctLower : elements.allocPctUpper;
    let tick;
    if (source === "price") tick = displayPriceToTick(priceInput.value);
    else if (source === "percent") {
      const percent = Number(percentInput.value);
      const spot = currentDisplayPrice();
      tick = Number.isFinite(percent) && percent > -100 && spot > 0 ? displayPriceToTick(spot * (1 + percent / 100)) : null;
    } else {
      tick = Number(tickInput.value);
    }
    if (tick == null || !Number.isFinite(tick)) {
      elements.allocStatus.classList.add("is-error");
      text(elements.allocStatus, `Enter a valid ${boundary} ${source}.`);
      return;
    }
    const ticks = allocationTicks();
    setAllocationTicks(boundary === "lower" ? tick : ticks.lower, boundary === "upper" ? tick : ticks.upper, boundary);
    elements.allocPresets.querySelectorAll("button").forEach((button) => button.classList.remove("is-active"));
  }

  function loadAllocationBand(band) {
    setAction("add");
    elements.tickLower.value = String(band.tick_lower);
    elements.tickUpper.value = String(band.tick_upper);
    elements.amount0.value = band.amount0 == null ? "" : String(band.amount0);
    elements.amount1.value = band.amount1 == null ? "" : String(band.amount1);
    syncRangesFromTicks();
    invalidateSimulation("Allocation band loaded");
    openActionDrawer();
    const caps = capabilities();
    if (caps && caps.add) {
      setNotice(elements.walletNotice, `Band #${band.index} loaded as one position. Simulate, review and sign this position explicitly; no other band is bundled or sent.`);
    } else {
      setNotice(elements.walletNotice, `Band #${band.index} loaded for review, but ADD execution is unavailable for this pool. The analytical preview creates no transaction.`);
    }
    chart.requestDraw();
  }

  function capabilities() {
    return state.detail && state.detail.capabilities || null;
  }

  function updateActionAvailability() {
    const caps = capabilities();
    const hasOwner = ADDRESS_RE.test(state.account || state.owner || "");
    const hostAllowsSimulation = state.runtimeCapabilities && state.runtimeCapabilities.simulate === true;
    const supported = Boolean(caps && caps[state.action] && hasOwner && hostAllowsSimulation);
    elements.actionFields.disabled = !state.detail || !supported;
    elements.simulateAction.disabled = !supported;
    if (!state.detail) {
      text(elements.capabilityNotice, "SELECT A POOL");
    } else if (!hasOwner) {
      text(elements.capabilityNotice, "SET AN OWNER OR CONNECT A WALLET TO SIMULATE");
    } else if (!hostAllowsSimulation) {
      text(elements.capabilityNotice, "ANALYSIS ONLY · EXECUTION DISABLED ON THIS HOST");
    } else {
      const protocol = state.detail.pool && state.detail.pool.protocol || String(state.detail.pool && state.detail.pool.kind || "POOL").toUpperCase();
      const map = ["add", "remove", "collect"].map((action) => `${action.toUpperCase()} ${caps && caps[action] ? "YES" : "NO"}`).join(" · ");
      const limitation = caps && caps.why ? ` · ${caps.why}` : "";
      text(elements.capabilityNotice, `${protocol} · ${map}${limitation}`);
    }
    text(elements.simulateAction, `SIMULATE ${state.action.toUpperCase()}`);
  }

  function setAction(action, { invalidate = true } = {}) {
    if (!["add", "remove", "collect"].includes(action)) return;
    state.action = action;
    elements.actionTabs.querySelectorAll("button[data-action]").forEach((button) => {
      const active = button.dataset.action === action;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-selected", String(active));
    });
    elements.amountFields.classList.toggle("is-hidden", action !== "add");
    elements.liquidityControl.classList.toggle("is-hidden", action !== "remove");
    if (invalidate) invalidateSimulation("Action changed");
    updateActionAvailability();
    chart.requestDraw();
  }

  function invalidateSimulation() {
    state.actionRevision += 1;
    if (state.simulationController) state.simulationController.abort();
    state.simulationController = null;
    state.simulation = null;
    state.stepStatus = new Map();
    state.review = null;
    elements.simulationPanel.classList.add("is-hidden");
    elements.transactionSteps.replaceChildren();
    if (elements.transactionDialog.open) elements.transactionDialog.close();
  }

  function actionPayload() {
    return {
      pool_id: state.selectedId,
      owner: state.account || state.owner,
      action: state.action,
      tick_lower: Number(elements.tickLower.value),
      tick_upper: Number(elements.tickUpper.value),
      amount0: state.action === "add" ? elements.amount0.value.trim() || "0" : "0",
      amount1: state.action === "add" ? elements.amount1.value.trim() || "0" : "0",
      liquidity_bps: state.action === "remove" ? Number(elements.liquidityBps.value) : 0,
      slippage_bps: Number(elements.slippageBps.value)
    };
  }

  function validateActionPayload(payload) {
    if (!state.detail || !state.selectedId) return "Select a pool first.";
    const caps = capabilities();
    if (!caps || !caps[state.action]) return caps && caps.why || `${state.action} is not supported for this pool.`;
    if (!ADDRESS_RE.test(payload.owner || "")) return "The owner address is invalid.";
    if (!Number.isInteger(payload.tick_lower) || !Number.isInteger(payload.tick_upper) || payload.tick_lower >= payload.tick_upper) return "Enter a valid range with the lower tick below the upper tick.";
    if (!Number.isInteger(payload.slippage_bps) || payload.slippage_bps < 1 || payload.slippage_bps > 500) return "Slippage must be between 0.01% and 5%.";
    if (payload.action === "add") {
      if (!AMOUNT_RE.test(payload.amount0) || !AMOUNT_RE.test(payload.amount1)) return "Amounts must be non-negative decimal token values.";
      if (Number(payload.amount0) === 0 && Number(payload.amount1) === 0) return "Enter an amount for at least one token.";
    }
    if (payload.action === "remove" && (!Number.isInteger(payload.liquidity_bps) || payload.liquidity_bps < 1 || payload.liquidity_bps > 10_000)) return "Choose how much position liquidity to remove.";
    return null;
  }

  async function simulateAction({ preserveNotice = false } = {}) {
    if (!state.runtimeCapabilities || state.runtimeCapabilities.simulate !== true) {
      setNotice(elements.walletNotice, "EXECUTION DISABLED ON THIS HOST");
      return;
    }
    const payload = actionPayload();
    const validation = validateActionPayload(payload);
    if (validation) {
      setNotice(elements.walletNotice, validation);
      return;
    }
    if (!preserveNotice) setNotice(elements.walletNotice, "");
    invalidateSimulation("New simulation");
    const revision = state.actionRevision;
    const controller = new AbortController();
    state.simulationController = controller;
    elements.simulateAction.disabled = true;
    elements.simulateAction.textContent = "Simulating…";
    try {
      const result = await api("/simulate", { method: "POST", body: JSON.stringify(payload), signal: controller.signal });
      if (revision !== state.actionRevision) return;
      state.simulation = result;
      state.stepStatus = new Map();
      renderSimulation(result);
    } catch (error) {
      if (error.name === "AbortError" || revision !== state.actionRevision) return;
      setNotice(elements.walletNotice, `Simulation failed: ${errorMessage(error)}`);
    } finally {
      if (revision === state.actionRevision) updateActionAvailability();
    }
  }

  function renderSimulation(simulation) {
    elements.simulationPanel.classList.remove("is-hidden");
    text(elements.simulationExpiry, simulation.expires_at ? `Expires ${formatTime(simulation.expires_at)}` : "Expiry unavailable");
    const warnings = Array.isArray(simulation.warnings) ? simulation.warnings : [];
    elements.simulationWarnings.replaceChildren(...warnings.map((warning) => el("div", "warning-item", warning)));

    const summary = simulation.summary || {};
    const pool = state.detail && state.detail.pool || {};
    const action = simulation.action || state.action;
    const amountLabel = action === "add" ? "supplied" : action === "remove" ? "returned" : "collectible";
    const values = [
      [`${tokenLabel(pool.token0)} ${amountLabel}`, summary.amount0],
      [`${tokenLabel(pool.token1)} ${amountLabel}`, summary.amount1]
    ];
    if (summary.liquidity != null) values.push(["Liquidity", summary.liquidity]);
    if (summary.share_pct != null && action === "add") values.push(["Resulting active-pool share", `${summary.share_pct}%`]);
    if (summary.share_pct != null && action === "remove") values.push(["Owned position withdrawn", `${summary.share_pct}%`]);
    const lower = Number(summary.price_lower), upper = Number(summary.price_upper);
    const priceUnit = chart.inverted
      ? `${pool.token0.symbol} per ${pool.token1.symbol}`
      : `${pool.token1.symbol} per ${pool.token0.symbol}`;
    values.push(
      [`Lower · ${priceUnit}`, formatPrice(chart.inverted ? 1 / upper : lower)],
      [`Upper · ${priceUnit}`, formatPrice(chart.inverted ? 1 / lower : upper)],
      ["Estimated gas", summary.gas_estimate]
    );
    elements.simulationSummary.replaceChildren(...values.map(([label, value]) => {
      const node = el("div");
      node.append(el("dt", "", label), el("dd", "", value == null ? "Unavailable" : value));
      return node;
    }));

    const steps = Array.isArray(simulation.steps) ? simulation.steps : [];
    elements.transactionSteps.replaceChildren(...steps.map((step, index) => renderStep(step, index, steps)));
    if (!steps.length) {
      elements.transactionSteps.replaceChildren(el("li", "empty-copy", "The simulation returned no executable transaction steps."));
    }
  }

  function stepIsApproval(step) {
    return step && step.kind === "approval";
  }

  function stepIsRemoval(step) {
    return step && (step.id === "burn" || step.kind === "remove");
  }

  function renderStep(step, index, allSteps) {
    const status = state.stepStatus.get(String(step.id)) || "review";
    const item = el("li", `transaction-step${status === "confirmed" ? " is-complete" : ""}`);
    item.append(
      el("strong", "", step.kind ? String(step.kind).replaceAll("_", " ") : `Step ${index + 1}`),
      el("p", "", step.description || "No description supplied."),
      el("span", "step-state", status === "confirmed" ? "Confirmed" : step.simulation && step.simulation.success === false ? "Blocked" : "Preflight ready")
    );
    const button = el("button", "step-review", status === "confirmed" ? "Done" : "Review");
    button.type = "button";
    const priorComplete = allSteps.slice(0, index).every((prior) => state.stepStatus.get(String(prior.id)) === "confirmed");
    button.disabled = status === "confirmed" || !priorComplete || Boolean(step.simulation && step.simulation.success === false);
    button.addEventListener("click", () => reviewStep(step));
    item.append(button);
    return item;
  }

  async function reviewStep(step) {
    if (!state.account) {
      setNotice(elements.walletNotice, "Connect a browser wallet before reviewing an executable step. Simulation remains read-only.");
      elements.connectWallet.focus();
      return;
    }
    if (!state.simulation || !step || state.stepStatus.get(String(step.id)) === "confirmed") return;
    if (String(state.simulation.owner || "").toLowerCase() !== state.account) {
      invalidateSimulation("Account changed");
      setNotice(elements.walletNotice, "The simulation belongs to a different account. Simulate again with the connected wallet.");
      return;
    }
    try {
      await ensureChain(true);
    } catch (error) {
      setNotice(elements.walletNotice, errorMessage(error));
      return;
    }
    const revision = state.actionRevision;
    const reviewButton = Array.from(elements.transactionSteps.querySelectorAll(".step-review")).find((button) => !button.disabled && button.textContent === "Review");
    if (reviewButton) { reviewButton.disabled = true; reviewButton.textContent = "Preflight…"; }
    try {
      const prepared = await prepareStep(step, revision);
      if (!prepared || revision !== state.actionRevision) return;
      state.review = { step, prepared, revision, freshlyCompared: false };
      showTransactionReview(step, prepared);
    } catch (error) {
      if (revision === state.actionRevision && isPostBurnCollect(step)) {
        await recoverPostBurnCollect(error);
      } else if (revision === state.actionRevision) {
        setNotice(elements.walletNotice, `Fresh preflight failed: ${errorMessage(error)}`);
      }
    } finally {
      if (state.simulation && revision === state.actionRevision) renderSimulation(state.simulation);
    }
  }

  async function prepareStep(step, revision) {
    if (!state.runtimeCapabilities || state.runtimeCapabilities.prepare !== true) {
      throw new Error("Transaction preparation is disabled on this host.");
    }
    const result = await api("/prepare", {
      method: "POST",
      body: JSON.stringify({ simulation_id: state.simulation.simulation_id, owner: state.account, step_id: step.id })
    });
    if (revision !== state.actionRevision) return null;
    validatePreparedTransaction(result.transaction);
    if (!result.simulation || result.simulation.success !== true) throw new Error(result.simulation && result.simulation.error || "Fresh preflight did not succeed.");
    return result;
  }

  function normalizeChainId(value) {
    if (typeof value === "string" && value.startsWith("0x")) return Number.parseInt(value, 16);
    return Number(value);
  }

  function validatePreparedTransaction(transaction) {
    if (!transaction || typeof transaction !== "object") throw new Error("Prepared transaction is missing.");
    if (String(transaction.from || "").toLowerCase() !== state.account) throw new Error("Prepared transaction sender does not match the connected wallet.");
    if (!ADDRESS_RE.test(transaction.to || "")) throw new Error("Prepared transaction target is invalid.");
    if (typeof transaction.data !== "string" || !/^0x(?:[0-9a-fA-F]{2})*$/.test(transaction.data)) throw new Error("Prepared transaction calldata is invalid.");
    if (normalizeChainId(transaction.chainId) !== CHAIN_ID) throw new Error(`Prepared transaction targets the wrong chain; expected ${CHAIN_ID}.`);
    if (transaction.value != null && !/^(?:0x[0-9a-fA-F]+|\d+)$/.test(String(transaction.value))) throw new Error("Prepared transaction value is invalid.");
  }

  function showTransactionReview(step, prepared, changed = false) {
    const transaction = prepared.transaction;
    text(elements.reviewTitle, changed ? "Preflight changed — review again" : "Review transaction");
    text(elements.reviewDescription, step.description || "Review this prepared transaction before opening your wallet.");
    text(elements.reviewFrom, transaction.from);
    text(elements.reviewTo, transaction.to);
    text(elements.reviewValue, transaction.value == null ? "0" : transaction.value);
    text(elements.reviewGas, prepared.simulation && prepared.simulation.gas_estimate != null ? prepared.simulation.gas_estimate : "Unavailable");
    text(elements.reviewData, transaction.data);
    setNotice(elements.reviewError, changed ? "Chain state changed the prepared transaction. Nothing was sent. Review the updated fields, then confirm again." : "");
    elements.confirmTransaction.disabled = false;
    text(elements.confirmTransaction, "Confirm in wallet");
    if (!elements.transactionDialog.open) elements.transactionDialog.showModal();
  }

  function comparableTransaction(transaction) {
    return JSON.stringify({
      from: String(transaction.from || "").toLowerCase(),
      to: String(transaction.to || "").toLowerCase(),
      data: String(transaction.data || "").toLowerCase(),
      value: String(transaction.value == null ? "0" : transaction.value),
      chainId: normalizeChainId(transaction.chainId)
    });
  }

  async function confirmReviewedTransaction() {
    const review = state.review;
    if (!review || review.revision !== state.actionRevision || !state.simulation) {
      setNotice(elements.reviewError, "This review is stale. Close it and simulate again.");
      return;
    }
    elements.confirmTransaction.disabled = true;
    text(elements.confirmTransaction, "Fresh preflight…");
    try {
      await ensureChain(true);
      const fresh = await prepareStep(review.step, review.revision);
      if (!fresh) return;
      if (comparableTransaction(fresh.transaction) !== comparableTransaction(review.prepared.transaction)) {
        state.review.prepared = fresh;
        state.review.freshlyCompared = true;
        showTransactionReview(review.step, fresh, true);
        return;
      }
      const tx = fresh.transaction;
      const walletTx = { from: tx.from, to: tx.to, data: tx.data, value: tx.value == null ? "0x0" : tx.value, chainId: CHAIN_HEX };
      if (tx.gas != null) walletTx.gas = tx.gas;
      text(elements.confirmTransaction, "Confirm in wallet…");
      const hash = await window.ethereum.request({ method: "eth_sendTransaction", params: [walletTx] });
      if (review.revision !== state.actionRevision) return;
      elements.transactionDialog.close();
      state.stepStatus.set(String(review.step.id), "pending");
      renderSimulation(state.simulation);
      toast(`Transaction submitted · ${shortAddress(hash, 10, 6)}`);
      await trackReceipt(hash, review);
    } catch (error) {
      if (review.revision !== state.actionRevision) return;
      setNotice(elements.reviewError, `Not sent: ${errorMessage(error)}`);
      elements.confirmTransaction.disabled = false;
      text(elements.confirmTransaction, "Confirm in wallet");
    }
  }

  async function trackReceipt(hash, review) {
    const revision = review.revision;
    const started = Date.now();
    while (Date.now() - started < 180_000) {
      if (revision !== state.actionRevision) return;
      let receipt;
      try {
        receipt = await window.ethereum.request({ method: "eth_getTransactionReceipt", params: [hash] });
      } catch (error) {
        if (revision === state.actionRevision) toast(`Receipt check paused: ${errorMessage(error)}`);
        return;
      }
      if (receipt) {
        if (receipt.status == null) {
          toast("Receipt has no execution status. The next step remains locked; verify the transaction on chain.", 7000);
          return;
        }
        const success = normalizeChainId(receipt.status) === 1;
        if (!success) {
          state.stepStatus.set(String(review.step.id), "failed");
          renderSimulation(state.simulation);
          toast("Transaction reverted. No subsequent step was enabled.", 6000);
          return;
        }
        state.stepStatus.set(String(review.step.id), "confirmed");
        if (state.simulation) renderSimulation(state.simulation);
        toast(`Transaction confirmed in block ${normalizeChainId(receipt.blockNumber).toLocaleString()}.`);
        await afterConfirmedStep(review.step);
        return;
      }
      await new Promise((resolve) => window.setTimeout(resolve, 1800));
    }
    if (revision === state.actionRevision) toast("Receipt is still pending. Refreshing the pool will not resend it.", 7000);
  }

  function nextSimulationStep(step) {
    const steps = state.simulation && Array.isArray(state.simulation.steps) ? state.simulation.steps : [];
    const index = steps.findIndex((candidate) => String(candidate.id) === String(step.id));
    return index < 0 ? null : steps.slice(index + 1).find((candidate) => state.stepStatus.get(String(candidate.id)) !== "confirmed") || null;
  }

  function isPostBurnCollect(step) {
    if (!state.simulation || !/collect/i.test(`${step && step.kind || ""} ${step && step.description || ""}`)) return false;
    const steps = Array.isArray(state.simulation.steps) ? state.simulation.steps : [];
    const index = steps.findIndex((candidate) => String(candidate.id) === String(step.id));
    return index > 0 && steps.slice(0, index).some((candidate) => stepIsRemoval(candidate) && state.stepStatus.get(String(candidate.id)) === "confirmed");
  }

  async function recoverPostBurnCollect(error) {
    const retained = {
      lower: elements.tickLower.value,
      upper: elements.tickUpper.value,
      slippage: elements.slippageBps.value
    };
    setAction("collect");
    elements.tickLower.value = retained.lower;
    elements.tickUpper.value = retained.upper;
    elements.slippageBps.value = retained.slippage;
    syncRangesFromTicks();
    setNotice(elements.walletNotice, `The burn is confirmed but its collect quote can no longer be prepared (${errorMessage(error)}). Preparing a collect-only quote; no transaction will be sent automatically.`);
    await simulateAction({ preserveNotice: true });
  }

  async function afterConfirmedStep(step) {
    const wasApproval = stepIsApproval(step);
    await loadDetail({ quiet: true });
    if (wasApproval) {
      toast("Approval confirmed. Re-simulating the operation against current chain state.");
      await simulateAction({ preserveNotice: true });
      return;
    }
    const next = nextSimulationStep(step);
    if (next) {
      const message = stepIsRemoval(step)
        ? "Burn confirmed. Principal remains owed until the separately reviewed collect step completes."
        : /poke/i.test(`${step.kind || ""} ${step.description || ""}`)
          ? "Fee checkpoint confirmed. Review the collect step next; it will be freshly preflighted without repeating the checkpoint."
          : "Step confirmed. The next step is ready for separate review.";
      toast(message, 7000);
      if (state.simulation) renderSimulation(state.simulation);
      return;
    }
    invalidateSimulation("Operation confirmed");
  }

  async function connectWallet() {
    if (!window.ethereum || typeof window.ethereum.request !== "function") {
      setNotice(elements.walletNotice, "No EIP-1193 browser wallet was found. You can continue exploring in read-only mode.");
      return;
    }
    elements.connectWallet.disabled = true;
    elements.connectWallet.textContent = "Connecting…";
    try {
      const accounts = await window.ethereum.request({ method: "eth_requestAccounts" });
      if (!Array.isArray(accounts) || !accounts[0] || !ADDRESS_RE.test(accounts[0])) throw new Error("The wallet returned no valid account.");
      await ensureChain(true);
      setAccount(accounts[0]);
      bindWalletEvents();
      setNotice(elements.walletNotice, "Wallet connected on Chain 4663. Transactions still require explicit review and confirmation.");
    } catch (error) {
      setNotice(elements.walletNotice, `Wallet not connected: ${errorMessage(error)}`);
    } finally {
      elements.connectWallet.disabled = false;
      elements.connectWallet.textContent = state.account ? "Connected" : "Connect wallet";
    }
  }

  async function ensureChain(switchIfNeeded) {
    if (!window.ethereum) throw new Error("No browser wallet is available.");
    const chain = await window.ethereum.request({ method: "eth_chainId" });
    state.walletChain = normalizeChainId(chain);
    if (state.walletChain === CHAIN_ID) return;
    if (!switchIfNeeded) throw new Error(`Wallet is on chain ${state.walletChain}; switch to Chain ${CHAIN_ID}.`);
    try {
      await window.ethereum.request({ method: "wallet_switchEthereumChain", params: [{ chainId: CHAIN_HEX }] });
    } catch (error) {
      if (error && Number(error.code) === 4902) throw new Error("Chain 4663 is not configured in this wallet. Add it using your trusted RPC settings, then connect again.");
      throw new Error(`Switch to Chain ${CHAIN_ID} was not approved.`);
    }
    const verified = await window.ethereum.request({ method: "eth_chainId" });
    state.walletChain = normalizeChainId(verified);
    if (state.walletChain !== CHAIN_ID) throw new Error(`Wallet remained on chain ${state.walletChain}; Chain ${CHAIN_ID} is required.`);
  }

  function bindWalletEvents() {
    if (state.walletBound || !window.ethereum || typeof window.ethereum.on !== "function") return;
    state.walletBound = true;
    window.ethereum.on("accountsChanged", (accounts) => {
      const account = Array.isArray(accounts) && ADDRESS_RE.test(accounts[0] || "") ? accounts[0] : null;
      setAccount(account);
      setNotice(elements.walletNotice, account ? "Wallet account changed. Previous simulations were invalidated." : "Wallet disconnected. Returned to read-only observation.");
    });
    window.ethereum.on("chainChanged", (chain) => {
      state.walletChain = normalizeChainId(chain);
      invalidateSimulation("Chain changed");
      updateWalletPresentation();
      if (state.walletChain !== CHAIN_ID) setNotice(elements.walletNotice, `Wallet moved to chain ${state.walletChain}. Switch to Chain ${CHAIN_ID} before preparing transactions.`);
    });
  }

  function setAccount(account) {
    const normalized = account ? String(account).toLowerCase() : null;
    if (normalized === state.account) return;
    state.account = normalized;
    state.owner = normalized || state.readOnlyOwner;
    elements.ownerInput.value = state.owner || "";
    const url = new URL(window.location.href);
    if (ADDRESS_RE.test(state.owner || "")) url.searchParams.set("owner", state.owner);
    else url.searchParams.delete("owner");
    window.history.replaceState(null, "", url);
    invalidateSimulation("Account changed");
    updateWalletPresentation();
    if (state.selectedId) {
      loadDetail();
      openStream();
    }
  }

  function updateWalletPresentation() {
    if (state.account) {
      text(elements.walletState, state.walletChain === CHAIN_ID ? "Wallet ready · Chain 4663" : "Wallet chain mismatch");
      text(elements.walletAccount, state.account);
      text(elements.connectWallet, "Connected");
      text(elements.ownerToggle, shortAddress(state.owner));
    } else if (ADDRESS_RE.test(state.owner || "")) {
      text(elements.walletState, "Read-only mode");
      text(elements.walletAccount, `Watching ${shortAddress(state.owner)}`);
      text(elements.connectWallet, "Connect wallet");
      text(elements.ownerToggle, shortAddress(state.owner));
    } else {
      text(elements.walletState, "Pool-only mode");
      text(elements.walletAccount, "No owner selected");
      text(elements.connectWallet, "Connect wallet");
      text(elements.ownerToggle, "SET OWNER");
    }
    const ownerUrl = ADDRESS_RE.test(state.owner || "")
      ? `/lp?${new URLSearchParams({ owner: state.owner }).toString()}`
      : "/lp";
    elements.ownerBackLink.href = ownerUrl;
    elements.ownerDetailLink.href = ownerUrl;
  }

  function setWatchedOwner(value) {
    const owner = String(value || "").trim().toLowerCase();
    if (!ADDRESS_RE.test(owner)) {
      setCoverage(elements.poolCoverage, elements.poolError, "Enter a valid 0x wallet address to watch.");
      return false;
    }
    if (state.account) {
      setCoverage(elements.poolCoverage, elements.poolError, "Disconnect the browser wallet before changing the read-only owner.");
      return false;
    }
    if (owner === state.owner) return true;
    state.readOnlyOwner = owner;
    state.owner = owner;
    const url = new URL(window.location.href);
    url.searchParams.set("owner", owner);
    window.history.replaceState(null, "", url);
    invalidateSimulation("Owner changed");
    updateWalletPresentation();
    if (state.selectedId) {
      loadDetail();
      openStream();
    }
    return true;
  }

  function openActionDrawer() {
    closeCatalog();
    if (state.detail) configureActionRange(state.detail);
    if (state.runtimeCapabilities == null) loadRuntimeCapabilities();
    elements.actionDrawer.inert = false;
    elements.actionDrawer.classList.add("is-open");
    elements.actionDrawer.setAttribute("aria-hidden", "false");
    elements.actionDrawer.setAttribute("role", "dialog");
    elements.actionDrawer.setAttribute("aria-modal", "true");
    elements.drawerScrim.classList.remove("is-hidden");
    document.body.style.overflow = "hidden";
    elements.mobileActionClose.focus({ preventScroll: true });
    chart.requestDraw();
  }

  function closeActionDrawer() {
    const restoreFocus = elements.actionDrawer.contains(document.activeElement);
    elements.actionDrawer.classList.remove("is-open");
    elements.actionDrawer.setAttribute("aria-hidden", "true");
    elements.actionDrawer.removeAttribute("role");
    elements.actionDrawer.removeAttribute("aria-modal");
    elements.actionDrawer.inert = true;
    elements.drawerScrim.classList.add("is-hidden");
    document.body.style.overflow = "";
    if (restoreFocus) elements.mobileActionOpen.focus({ preventScroll: true });
    chart.requestDraw();
  }

  class LiquidityChart {
    constructor(surface) {
      this.surface = surface;
      this.canvas = surface;
      this.context = surface.getContext("2d", { alpha: false });
      this.detail = null;
      this.points = [];
      this.view = null;
      this.live = true;
      this.inverted = false;
      this.frame = null;
      this.pointers = new Map();
      this.drag = null;
      this.cursor = null;
      this.selectedParticipantId = null;
      this.selectedEvent = null;
      this.width = 0;
      this.height = 0;
      this.pixelRatio = 0;
      this.padding = { left: 60, right: 12, top: 20, bottom: 38 };
      this.overlayKey = "";
      this.snapshotKey = "";
      this.resizeObserver = new ResizeObserver(() => this.resize());
      this.resizeObserver.observe(surface.parentElement);
      this.bind();
      this.resize();
    }

    bind() {
      this.surface.addEventListener("wheel", (event) => {
        event.preventDefault();
        if (!this.view) return;
        this.inspect();
        const rect = this.surface.getBoundingClientRect();
        this.zoom(Math.exp(event.deltaY * .0012), this.tickAtX(event.clientX - rect.left));
      }, { passive: false });
      this.surface.addEventListener("dblclick", () => this.follow());
      this.surface.addEventListener("pointerdown", (event) => this.pointerDown(event));
      this.surface.addEventListener("pointermove", (event) => this.pointerMove(event));
      this.surface.addEventListener("pointerup", (event) => this.pointerUp(event));
      this.surface.addEventListener("pointercancel", (event) => this.pointerUp(event));
      this.surface.addEventListener("pointerleave", () => {
        if (!this.pointers.size) {
          this.cursor = null;
          this.surface.classList.remove("is-boundary-hover");
          this.updateReadout();
          this.requestDraw();
        }
      });
      this.surface.addEventListener("keydown", (event) => {
        if (!this.view) return;
        const span = this.view.max - this.view.min;
        if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
          event.preventDefault();
          this.inspect();
          const screenDirection = event.key === "ArrowLeft" ? -1 : 1;
          const tickDirection = this.inverted ? -screenDirection : screenDirection;
          this.pan(tickDirection * span * (event.shiftKey ? .25 : .08));
        } else if (["+", "="].includes(event.key)) {
          event.preventDefault();
          this.inspect();
          this.zoom(.78, (this.view.min + this.view.max) / 2);
        } else if (["-", "_"].includes(event.key)) {
          event.preventDefault();
          this.inspect();
          this.zoom(1.28, (this.view.min + this.view.max) / 2);
        } else if (event.key === "Home" || event.key === "Escape") {
          event.preventDefault();
          this.follow();
        } else if (event.key.toLowerCase() === "i") {
          event.preventDefault();
          this.toggleInvert();
        }
      });
    }

    resize() {
      const rect = this.surface.getBoundingClientRect();
      if (rect.width < 2 || rect.height < 2) return;
      const width = Math.round(rect.width * 10) / 10;
      const height = Math.round(rect.height * 10) / 10;
      const pixelRatio = Math.max(1, window.devicePixelRatio || 1);
      if (width === this.width && height === this.height && pixelRatio === this.pixelRatio) return;
      this.width = width;
      this.height = height;
      this.pixelRatio = pixelRatio;
      this.padding = width < 480
        ? { left: 48, right: 8, top: 18, bottom: 40 }
        : { left: 60, right: 12, top: 20, bottom: 38 };
      this.canvas.width = Math.max(1, Math.round(width * pixelRatio));
      this.canvas.height = Math.max(1, Math.round(height * pixelRatio));
      this.requestDraw();
    }

    clear() {
      this.detail = null;
      this.points = [];
      this.view = null;
      this.cursor = null;
      this.selectedParticipantId = null;
      this.selectedEvent = null;
      this.live = true;
      this.overlayKey = "";
      this.snapshotKey = "";
      elements.returnLive.classList.add("is-hidden");
      elements.chartReadout.classList.add("is-hidden");
      elements.chartReadout.setAttribute("aria-hidden", "true");
      this.updateMode();
      this.requestDraw();
    }

    rawPoint(point) {
      const tick = Number(point.tick);
      let price = Number(point.price);
      if (!(price > 0)) price = tickToRawPrice(tick);
      const exactLiquidity = point.liquidity ?? point.total_liquidity_raw;
      const exactOurs = point.ours ?? point.ours_liquidity_raw;
      const liquidity = Math.max(0, Number(exactLiquidity) || 0);
      const ours = Math.max(0, Number(exactOurs) || 0);
      if (!Number.isFinite(tick) || !(price > 0) || !Number.isFinite(price)) return null;
      return {
        tick,
        rawPrice: price,
        liquidity,
        ours,
        exactLiquidity: exactLiquidity == null ? "" : String(exactLiquidity),
        exactOurs: exactOurs == null ? "" : String(exactOurs)
      };
    }

    displayPrice(rawPrice) {
      return this.inverted ? 1 / rawPrice : rawPrice;
    }

    setSnapshot(detail) {
      const firstSnapshot = !this.detail;
      if (firstSnapshot && detail.pool && detail.pool.token0) {
        this.inverted = String(detail.pool.token0.address || "").toLowerCase() === USDG;
      }
      const curve = detail && detail.liquidity && Array.isArray(detail.liquidity.curve) ? detail.liquidity.curve : [];
      const nextPoints = curve.map((point) => this.rawPoint(point)).filter(Boolean).sort((a, b) => a.tick - b.tick);
      const ranges = Array.isArray(detail && detail.positions) ? detail.positions : participantRecords(detail).filter((item) => item.ours);
      const spot = Number(detail && detail.spot && detail.spot.tick);
      const selectedParticipant = state.selectedParticipantId
        ? participantRecords(detail).find((item) => participantId(item) === state.selectedParticipantId) || null
        : null;
      const selectedEvent = state.selectedEventId
        ? groupedLpEvents(detail).find((event, index) => eventKey(event, index) === state.selectedEventId) || null
        : null;
      const curveChanged = nextPoints.length !== this.points.length || nextPoints.some((point, index) => {
        const prior = this.points[index];
        return !prior
          || point.tick !== prior.tick
          || point.rawPrice !== prior.rawPrice
          || point.exactLiquidity !== prior.exactLiquidity
          || point.exactOurs !== prior.exactOurs;
      });
      const overlayKey = JSON.stringify([
        ranges.map((item) => [item.lo, item.hi, String(item.liquidity || "")]),
        Number.isFinite(spot) ? spot : null,
        selectedParticipant ? [selectedParticipant.lo, selectedParticipant.hi] : null,
        selectedEvent ? [
          selectedEvent.lo,
          selectedEvent.hi,
          selectedEvent.tick_before,
          selectedEvent.tick_after,
          selectedEvent.ranges_before,
          selectedEvent.ranges_after
        ] : null
      ]);
      const snapshotKey = JSON.stringify([
        detail.block_hash,
        detail.block,
        detail.block_timestamp,
        detail.spot && detail.spot.tick,
        detail.spot && detail.spot.sqrt_price_x96,
        detail.spot && detail.spot.price_token1_per_token0,
        detail.liquidity && detail.liquidity.active
      ]);
      const frameChanged = curveChanged || overlayKey !== this.overlayKey || snapshotKey !== this.snapshotKey;
      this.overlayKey = overlayKey;
      this.snapshotKey = snapshotKey;
      this.points = nextPoints;
      this.detail = detail;
      this.selectedParticipantId = state.selectedParticipantId;
      this.selectedEvent = selectedEvent;
      if (this.live || !this.view) this.setLiveDomain();
      else if (curveChanged) this.view = this.clampDomain(this.view.min, this.view.max);
      this.updateMode();
      this.surface.dataset.points = String(this.points.length);
      this.surface.setAttribute("aria-label", this.points.length
        ? `Graphical pool liquidity depth with ${this.points.length} plotted points. Pool depth is gray, owned liquidity cyan and spot white. Swipe horizontally to pan, pinch or use plus and minus to zoom, and swipe vertically to scroll the page.`
        : "No live liquidity curve is available.");
      this.updateReadout();
      if (frameChanged || firstSnapshot) this.requestDraw();
    }

    fullDomain() {
      if (!this.points.length) return null;
      const min = this.points[0].tick;
      const max = this.points[this.points.length - 1].tick;
      return min === max ? { min: min - 1, max: max + 1 } : { min, max };
    }

    minimumStep() {
      const spacing = Number(this.detail && this.detail.pool && this.detail.pool.tick_spacing);
      return Number.isFinite(spacing) && spacing > 0 ? spacing : 1;
    }

    clampDomain(minimum, maximum) {
      const full = this.fullDomain();
      if (!full) return null;
      const fullSpan = full.max - full.min;
      const minSpan = Math.min(fullSpan, this.minimumStep());
      const span = Math.max(minSpan, Math.min(fullSpan, maximum - minimum));
      let min = (minimum + maximum - span) / 2;
      let max = min + span;
      if (min < full.min) { min = full.min; max = min + span; }
      if (max > full.max) { max = full.max; min = max - span; }
      return { min, max };
    }

    liveDomain() {
      return this.fullDomain();
    }

    updateMode() {
      const full = this.fullDomain();
      const fullSpan = full ? Math.max(1, full.max - full.min) : 1;
      const viewSpan = this.view ? Math.max(1, this.view.max - this.view.min) : fullSpan;
      elements.chartMode.dataset.mode = this.live ? "live" : "inspect";
      elements.chartMode.dataset.zoom = (fullSpan / viewSpan).toFixed(1);
      renderChartMode();
    }

    setLiveDomain() {
      const domain = this.liveDomain();
      if (!domain) return;
      this.live = true;
      this.view = domain;
      elements.returnLive.classList.add("is-hidden");
      this.updateMode();
    }

    fit() {
      this.setLiveDomain();
      this.updateReadout();
      this.requestDraw();
    }

    follow() {
      this.selectedEvent = null;
      state.selectedEventId = null;
      if (state.detail) renderEvents(state.detail);
      this.setLiveDomain();
      this.updateReadout();
      this.requestDraw();
    }

    inspect() {
      this.live = false;
      elements.returnLive.classList.remove("is-hidden");
      this.updateMode();
    }

    toggleInvert() {
      this.inverted = !this.inverted;
      updatePricePresentation({ renderPlan: true });
      if (state.detail) {
        renderParticipants(state.detail);
        renderEvents(state.detail);
      }
      if (state.simulation) renderSimulation(state.simulation);
      this.updateReadout();
      if (poolToolsOpen()) trackingChart.requestDraw();
      this.requestDraw();
    }

    pan(delta) {
      if (!this.view || !Number.isFinite(delta)) return;
      this.view = this.clampDomain(this.view.min + delta, this.view.max + delta);
      this.updateMode();
      this.requestDraw();
    }

    zoom(factor, anchor) {
      if (!this.view || !Number.isFinite(factor) || !Number.isFinite(anchor)) return;
      const currentSpan = this.view.max - this.view.min;
      const span = currentSpan * factor;
      const ratio = (anchor - this.view.min) / currentSpan;
      const minimum = anchor - span * ratio;
      this.view = this.clampDomain(minimum, minimum + span);
      this.updateMode();
      this.requestDraw();
    }

    selectParticipant(id) {
      this.selectedParticipantId = id;
      this.selectedEvent = null;
      const participant = this.selectedParticipant();
      if (participant && this.view) {
        const lo = Number(participant.lo), hi = Number(participant.hi);
        const spot = Number(this.detail && this.detail.spot && this.detail.spot.tick);
        const min = Math.min(lo, Number.isFinite(spot) ? spot : lo);
        const max = Math.max(hi, Number.isFinite(spot) ? spot : hi);
        const pad = Math.max(this.minimumStep() * 2, (max - min) * .15);
        this.inspect();
        this.view = this.clampDomain(min - pad, max + pad);
      }
      this.updateReadout();
      this.requestDraw();
    }

    selectEvent(event) {
      this.selectedEvent = event;
      this.selectedParticipantId = null;
      if (event && this.view) {
        const ticks = [event.lo, event.hi, event.tick_before, event.tick_after]
          .filter((value) => value != null).map(Number).filter(Number.isFinite);
        for (const range of [...eventRanges(event, "ranges_before"), ...eventRanges(event, "ranges_after")]) {
          if (range[0] != null) ticks.push(Number(range[0]));
          if (range[1] != null) ticks.push(Number(range[1]));
        }
        if (ticks.length) {
          const min = Math.min(...ticks), max = Math.max(...ticks);
          const pad = Math.max(this.minimumStep() * 3, (max - min) * .25);
          this.inspect();
          this.view = this.clampDomain(min - pad, max + pad);
        }
      }
      this.updateReadout();
      this.requestDraw();
    }

    allocationEdgeX(tick) {
      return Math.max(this.padding.left, Math.min(this.width - this.padding.right, this.xForTick(tick)));
    }

    selectedParticipant() {
      if (!this.selectedParticipantId || !this.detail) return null;
      return participantRecords(this.detail).find((item) => participantId(item) === this.selectedParticipantId) || null;
    }

    allocationBoundaryAt(point) {
      if (!poolToolsOpen() || !this.view || !state.allocationTicksInitialized || !allocationSupported()) return null;
      if (point.y < this.padding.top || point.y > this.height - this.padding.bottom) return null;
      const ticks = allocationTicks();
      const candidates = [
        { boundary: "lower", distance: Math.abs(point.x - this.allocationEdgeX(ticks.lower)) },
        { boundary: "upper", distance: Math.abs(point.x - this.allocationEdgeX(ticks.upper)) }
      ].sort((left, right) => left.distance - right.distance);
      return candidates[0].distance <= 13 ? candidates[0].boundary : null;
    }

    pointerDown(event) {
      if (event.pointerType === "mouse") event.preventDefault();
      this.surface.setPointerCapture(event.pointerId);
      const point = this.localPointer(event);
      this.pointers.set(event.pointerId, point);
      const boundary = this.pointers.size === 1 ? this.allocationBoundaryAt(point) : null;
      if (boundary) {
        this.drag = { kind: "allocation-boundary", boundary };
        this.surface.classList.remove("is-dragging", "is-boundary-hover");
        this.surface.classList.add("is-boundary-dragging");
        this.updateReadout(point);
        this.requestDraw();
        return;
      }
      if (this.pointers.size === 1) {
        const pending = event.pointerType === "touch";
        this.drag = { kind: "pan", x: point.x, y: point.y, view: this.view ? { ...this.view } : null, pending };
        if (!pending) {
          this.inspect();
          this.surface.classList.add("is-dragging");
        }
      } else if (this.pointers.size === 2) {
        const values = [...this.pointers.values()];
        this.inspect();
        this.drag = { kind: "pinch", distance: Math.max(10, Math.hypot(values[1].x - values[0].x, values[1].y - values[0].y)), view: this.view ? { ...this.view } : null };
        this.surface.classList.remove("is-dragging");
      }
    }

    pointerMove(event) {
      const point = this.localPointer(event);
      this.cursor = point;
      if (this.pointers.has(event.pointerId)) this.pointers.set(event.pointerId, point);
      if (this.drag && this.drag.kind === "allocation-boundary" && this.pointers.has(event.pointerId)) {
        const ticks = allocationTicks();
        const snapped = snapAllocationTick(this.tickAtX(point.x));
        if (snapped != null) {
          setAllocationTicks(
            this.drag.boundary === "lower" ? snapped : ticks.lower,
            this.drag.boundary === "upper" ? snapped : ticks.upper,
            this.drag.boundary
          );
        }
      } else if (this.view && this.drag && this.drag.kind === "pan" && this.pointers.size === 1 && this.drag.view) {
        const dx = point.x - this.drag.x;
        const dy = point.y - this.drag.y;
        if (this.drag.pending && Math.max(Math.abs(dx), Math.abs(dy)) >= 6) {
          if (Math.abs(dy) > Math.abs(dx)) {
            this.drag = { kind: "scroll" };
            this.surface.classList.remove("is-dragging");
          } else {
            this.drag.pending = false;
            this.inspect();
            this.surface.classList.add("is-dragging");
          }
        }
        if (this.drag.kind === "pan" && !this.drag.pending) {
          const span = this.drag.view.max - this.drag.view.min;
          const direction = this.inverted ? 1 : -1;
          const delta = direction * dx / this.plotWidth() * span;
          this.view = this.clampDomain(this.drag.view.min + delta, this.drag.view.max + delta);
          this.updateMode();
        }
      } else if (this.view && this.drag && this.drag.kind === "pinch" && this.pointers.size === 2 && this.drag.view) {
        const values = [...this.pointers.values()];
        const distance = Math.max(10, Math.hypot(values[1].x - values[0].x, values[1].y - values[0].y));
        const factor = this.drag.distance / distance;
        const centerX = (values[0].x + values[1].x) / 2;
        this.view = { ...this.drag.view };
        this.zoom(factor, this.tickAtX(centerX));
      }
      const hovering = !this.drag && Boolean(this.allocationBoundaryAt(point));
      this.surface.classList.toggle("is-boundary-hover", hovering);
      this.updateReadout(point);
      this.requestDraw();
    }

    pointerUp(event) {
      this.pointers.delete(event.pointerId);
      if (!this.pointers.size) {
        if (event.pointerType === "touch") this.cursor = null;
        this.drag = null;
        this.surface.classList.remove("is-dragging", "is-boundary-dragging");
        this.surface.classList.toggle("is-boundary-hover", Boolean(this.cursor && this.allocationBoundaryAt(this.cursor)));
      } else if (!this.drag || this.drag.kind !== "allocation-boundary") {
        const point = [...this.pointers.values()][0];
        this.drag = { kind: "pan", x: point.x, y: point.y, view: this.view ? { ...this.view } : null, pending: false };
        this.surface.classList.add("is-dragging");
      }
      this.updateReadout();
      this.requestDraw();
    }

    localPointer(event) {
      const rect = this.surface.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
    }

    plotWidth() {
      return Math.max(1, this.width - this.padding.left - this.padding.right);
    }

    plotHeight() {
      return Math.max(1, this.height - this.padding.top - this.padding.bottom);
    }

    tickAtX(x) {
      if (!this.view) return 0;
      const ratio = Math.max(0, Math.min(1, (x - this.padding.left) / this.plotWidth()));
      return this.inverted
        ? this.view.max - ratio * (this.view.max - this.view.min)
        : this.view.min + ratio * (this.view.max - this.view.min);
    }

    xForTick(tick) {
      if (!this.view) return 0;
      const ratio = (tick - this.view.min) / (this.view.max - this.view.min);
      return this.inverted
        ? this.padding.left + (1 - ratio) * this.plotWidth()
        : this.padding.left + ratio * this.plotWidth();
    }

    pointAtTick(tick) {
      if (!this.points.length) return null;
      let low = 0, high = this.points.length - 1;
      while (low <= high) {
        const middle = Math.floor((low + high) / 2);
        if (this.points[middle].tick <= tick) low = middle + 1;
        else high = middle - 1;
      }
      return this.points[Math.max(0, Math.min(this.points.length - 1, high))];
    }

    requestDraw() {
      if (this.width > 1 && this.height > 1 && this.frame == null) {
        this.frame = requestAnimationFrame(() => this.draw());
      }
    }

    visibleSteps(field) {
      if (!this.view) return [];
      const initial = this.pointAtTick(this.view.min);
      const steps = [{ tick: this.view.min, value: initial ? initial[field] : 0 }];
      let low = 0, high = this.points.length;
      while (low < high) {
        const middle = Math.floor((low + high) / 2);
        if (this.points[middle].tick <= this.view.min) low = middle + 1;
        else high = middle;
      }
      for (let index = low; index < this.points.length; index += 1) {
        const point = this.points[index];
        if (point.tick >= this.view.max) break;
        steps.push({ tick: point.tick, value: point[field] });
      }
      const finalPoint = this.pointAtTick(this.view.max);
      steps.push({ tick: this.view.max, value: finalPoint ? finalPoint[field] : 0 });
      return steps;
    }

    drawStepProfile(context, steps, maximum, fill, stroke, lineWidth) {
      if (!steps.length) return;
      const bottom = this.height - this.padding.bottom;
      const yFor = (value) => bottom - Math.max(0, value) / maximum * this.plotHeight();
      context.beginPath();
      context.moveTo(this.xForTick(steps[0].tick), bottom);
      context.lineTo(this.xForTick(steps[0].tick), yFor(steps[0].value));
      for (let index = 1; index < steps.length; index += 1) {
        const prior = steps[index - 1];
        const step = steps[index];
        const x = this.xForTick(step.tick);
        context.lineTo(x, yFor(prior.value));
        context.lineTo(x, yFor(step.value));
      }
      context.lineTo(this.xForTick(steps[steps.length - 1].tick), bottom);
      context.closePath();
      context.fillStyle = fill;
      context.fill();
      context.beginPath();
      context.moveTo(this.xForTick(steps[0].tick), yFor(steps[0].value));
      for (let index = 1; index < steps.length; index += 1) {
        const prior = steps[index - 1];
        const step = steps[index];
        const x = this.xForTick(step.tick);
        context.lineTo(x, yFor(prior.value));
        context.lineTo(x, yFor(step.value));
      }
      context.strokeStyle = stroke;
      context.lineWidth = lineWidth;
      context.stroke();
    }

    drawRange(context, lo, hi, { fill, stroke, dashed = false, topBar = false } = {}) {
      lo = Number(lo);
      hi = Number(hi);
      if (!Number.isFinite(lo) || !Number.isFinite(hi) || !this.view) return;
      const low = Math.min(lo, hi), high = Math.max(lo, hi);
      if (high < this.view.min || low > this.view.max) return;
      const top = this.padding.top;
      const bottom = this.height - this.padding.bottom;
      const firstX = this.xForTick(Math.max(this.view.min, low));
      const secondX = this.xForTick(Math.min(this.view.max, high));
      const left = Math.min(firstX, secondX);
      const width = Math.max(1, Math.abs(secondX - firstX));
      if (fill) {
        context.fillStyle = fill;
        context.fillRect(left, top, width, bottom - top);
      }
      if (stroke) {
        context.save();
        context.strokeStyle = stroke;
        context.lineWidth = 1;
        context.setLineDash(dashed ? [4, 4] : []);
        [lo, hi].forEach((tick) => {
          if (tick < this.view.min || tick > this.view.max) return;
          const x = Math.round(this.xForTick(tick)) + .5;
          context.beginPath();
          context.moveTo(x, top);
          context.lineTo(x, bottom);
          context.stroke();
        });
        if (topBar) {
          context.beginPath();
          context.moveTo(left, top + 3.5);
          context.lineTo(left + width, top + 3.5);
          context.stroke();
        }
        context.restore();
      }
    }

    draw() {
      this.frame = null;
      if (!this.context || this.width < 2 || this.height < 2) return;
      const context = this.context;
      context.setTransform(this.pixelRatio, 0, 0, this.pixelRatio, 0, 0);
      context.fillStyle = "#000";
      context.fillRect(0, 0, this.width, this.height);
      if (!this.points.length || !this.view) return;

      const top = this.padding.top;
      const bottom = this.height - this.padding.bottom;
      const left = this.padding.left;
      const right = this.width - this.padding.right;
      const poolSteps = this.visibleSteps("liquidity");
      const ownedSteps = this.visibleSteps("ours");
      let maximum = 1;
      for (const step of poolSteps) maximum = Math.max(maximum, step.value);
      context.font = this.width < 480
        ? '10px "SFMono-Regular", "SF Mono", Menlo, monospace'
        : '9px "SFMono-Regular", "SF Mono", Menlo, monospace';
      context.textBaseline = "middle";

      context.save();
      context.beginPath();
      context.rect(left, top, right - left, bottom - top);
      context.clip();

      const ownedRanges = Array.isArray(this.detail && this.detail.positions)
        ? this.detail.positions
        : this.participants().filter((item) => item.ours);
      ownedRanges.filter((item) => liquidityBig(item.liquidity) > 0n).forEach((item) => {
        this.drawRange(context, item.lo, item.hi, {
          fill: "rgba(95,215,255,.045)",
          stroke: "rgba(95,215,255,.28)",
          topBar: true
        });
      });
      const selected = this.selectedParticipant();
      if (selected) {
        this.drawRange(context, selected.lo, selected.hi, {
          fill: "rgba(216,216,216,.10)",
          stroke: "#d8d8d8",
          topBar: true
        });
      }
      if (this.selectedEvent) {
        [...eventRanges(this.selectedEvent, "ranges_before"), ...eventRanges(this.selectedEvent, "ranges_after")]
          .forEach(([lo, hi]) => this.drawRange(context, lo, hi, {
            fill: "rgba(216,216,216,.07)",
            stroke: "#d8d8d8",
            dashed: true,
            topBar: true
          }));
      }
      const plan = poolToolsOpen() ? state.allocationPreview : null;
      const bands = Array.isArray(plan && plan.bands) ? plan.bands.slice(0, MAX_ALLOCATION_BANDS) : [];
      bands.forEach((band) => this.drawRange(context, band.tick_lower, band.tick_upper, {
        fill: "rgba(95,215,255,.035)",
        stroke: "rgba(95,215,255,.55)",
        dashed: true
      }));

      context.strokeStyle = "#242424";
      context.lineWidth = 1;
      context.setLineDash([]);
      for (let division = 0; division <= 2; division += 1) {
        const y = Math.round(top + (bottom - top) * division / 2) + .5;
        context.beginPath();
        context.moveTo(left, y);
        context.lineTo(right, y);
        context.stroke();
      }
      for (let division = 0; division <= 4; division += 1) {
        const x = Math.round(left + (right - left) * division / 4) + .5;
        context.beginPath();
        context.moveTo(x, top);
        context.lineTo(x, bottom);
        context.stroke();
      }

      this.drawStepProfile(context, poolSteps, maximum, "rgba(216,216,216,.14)", "rgba(216,216,216,.55)", 1.25);
      this.drawStepProfile(context, ownedSteps, maximum, "rgba(95,215,255,.20)", "#5fd7ff", 1.5);

      if (poolToolsOpen() && state.allocationTicksInitialized && allocationSupported()) {
        const ticks = allocationTicks();
        this.drawRange(context, ticks.lower, ticks.upper, {
          fill: "rgba(95,215,255,.045)",
          stroke: "#5fd7ff",
          dashed: true,
          topBar: true
        });
        context.fillStyle = "#5fd7ff";
        [ticks.lower, ticks.upper].forEach((tick) => {
          if (tick < this.view.min || tick > this.view.max) return;
          const x = this.xForTick(tick);
          context.fillRect(x - 3, top, 6, 6);
          context.fillRect(x - 3, bottom - 6, 6, 6);
        });
      }

      if (this.selectedEvent) {
        context.save();
        context.strokeStyle = "#d8d8d8";
        context.setLineDash([2, 3]);
        [this.selectedEvent.tick_before, this.selectedEvent.tick_after].forEach((tick) => {
          tick = Number(tick);
          if (!Number.isFinite(tick) || tick < this.view.min || tick > this.view.max) return;
          const x = Math.round(this.xForTick(tick)) + .5;
          context.beginPath();
          context.moveTo(x, top);
          context.lineTo(x, bottom);
          context.stroke();
        });
        context.restore();
      }

      const spot = Number(this.detail && this.detail.spot && this.detail.spot.tick);
      if (Number.isFinite(spot) && spot >= this.view.min && spot <= this.view.max) {
        const x = Math.round(this.xForTick(spot)) + .5;
        context.save();
        context.strokeStyle = "#d8d8d8";
        context.lineWidth = 1;
        context.setLineDash([5, 3]);
        context.beginPath();
        context.moveTo(x, top);
        context.lineTo(x, bottom);
        context.stroke();
        context.fillStyle = "#d8d8d8";
        context.beginPath();
        context.moveTo(x - 4, top);
        context.lineTo(x + 4, top);
        context.lineTo(x, top + 6);
        context.closePath();
        context.fill();
        context.restore();
      }

      if (this.cursor && !this.drag
        && this.cursor.x >= left && this.cursor.x <= right
        && this.cursor.y >= top && this.cursor.y <= bottom) {
        context.save();
        context.strokeStyle = "rgba(216,216,216,.28)";
        context.setLineDash([2, 3]);
        context.beginPath();
        context.moveTo(Math.round(this.cursor.x) + .5, top);
        context.lineTo(Math.round(this.cursor.x) + .5, bottom);
        context.moveTo(left, Math.round(this.cursor.y) + .5);
        context.lineTo(right, Math.round(this.cursor.y) + .5);
        context.stroke();
        context.restore();
      }
      context.restore();

      context.strokeStyle = "#242424";
      context.lineWidth = 1;
      context.beginPath();
      context.moveTo(left + .5, top);
      context.lineTo(left + .5, bottom + .5);
      context.lineTo(right, bottom + .5);
      context.stroke();

      context.fillStyle = "rgba(216,216,216,.62)";
      context.textAlign = "right";
      for (let division = 0; division <= 2; division += 1) {
        const value = maximum * (2 - division) / 2;
        const y = top + (bottom - top) * division / 2;
        context.fillText(formatCompact(value), left - 6, y);
      }

      const ticks = this.inverted
        ? [this.view.max, (this.view.min + this.view.max) / 2, this.view.min]
        : [this.view.min, (this.view.min + this.view.max) / 2, this.view.max];
      ticks.forEach((tick, index) => {
        const x = left + (right - left) * index / 2;
        context.textAlign = index === 0 ? "left" : index === 2 ? "right" : "center";
        context.fillStyle = "#d8d8d8";
        context.fillText(formatPrice(this.tickPrice(tick)), x, bottom + 13);
        context.fillStyle = "rgba(216,216,216,.62)";
        context.fillText(`T ${Math.round(tick).toLocaleString()}`, x, bottom + 27);
      });
    }

    tickPrice(tick) {
      const raw = tickToRawPrice(tick);
      return raw ? this.displayPrice(raw) : null;
    }

    participants() {
      return this.detail ? participantRecords(this.detail) : [];
    }

    showReadout(price, liquidity, detail) {
      text(elements.readoutPrice, price);
      text(elements.readoutLiquidity, liquidity);
      text(elements.readoutDetail, detail);
      elements.chartReadout.classList.remove("is-hidden");
      elements.chartReadout.setAttribute("aria-hidden", "false");
    }

    hideReadout() {
      elements.chartReadout.classList.add("is-hidden");
      elements.chartReadout.setAttribute("aria-hidden", "true");
    }

    updateReadout(pointer = null) {
      if (this.drag && this.drag.kind === "allocation-boundary") {
        const boundary = this.drag.boundary;
        const tick = Number(boundary === "lower" ? elements.allocTickLower.value : elements.allocTickUpper.value);
        const plan = state.allocationPreview;
        this.showReadout(
          `${boundary.toUpperCase()} PLAN · ${formatPrice(tickToDisplayPrice(tick))}`,
          `TICK ${tick.toLocaleString()} · STEP ${allocationSpacing().toLocaleString()}`,
          `${Number(plan && plan.actual_positions || 0).toLocaleString()} positions · analysis only`
        );
        return;
      }
      const participant = this.selectedParticipant();
      if (participant) {
        const low = this.tickPrice(participant.lo), high = this.tickPrice(participant.hi);
        const earned = participant.fees_earned_usd == null ? participantTokenAmounts(participant, "fees_earned") : formatUsd(participant.fees_earned_usd);
        const claimable = participant.uncollected_usd == null ? participantTokenAmounts(participant, "uncollected") : formatUsd(participant.uncollected_usd);
        this.showReadout(
          `${participantOwnerLabel(participant)} · ${formatPrice(Math.min(low, high))}–${formatPrice(Math.max(low, high))}`,
          `L ${formatCompact(participant.liquidity)} · ${participant.active_share_pct == null ? "SHARE —" : `${formatCompact(participant.active_share_pct)}% SHARE`}`,
          `PRINCIPAL ${participant.value_usd == null ? "—" : formatUsd(participant.value_usd)} · FEES ${earned} · CLAIMABLE ${claimable} · PNL ${participant.pnl_usd == null ? "—" : formatUsd(participant.pnl_usd)}`
        );
        return;
      }
      if (this.selectedEvent) {
        const copy = eventCopy(this.selectedEvent);
        this.showReadout(copy.title, copy.detail, `#${this.selectedEvent.block == null ? "—" : Number(this.selectedEvent.block).toLocaleString()} · ${formatTime(this.selectedEvent.timestamp)}`);
        return;
      }
      const point = pointer || this.cursor;
      if (!point || !this.view || point.x < this.padding.left || point.x > this.width - this.padding.right) {
        this.hideReadout();
        return;
      }
      const tick = this.tickAtX(point.x);
      const step = this.pointAtTick(tick);
      if (!step) {
        this.hideReadout();
        return;
      }
      const share = step.liquidity > 0 ? step.ours / step.liquidity * 100 : null;
      this.showReadout(
        `${formatPrice(this.tickPrice(tick))} · TICK ${Math.round(tick).toLocaleString()}`,
        `POOL L ${formatCompact(step.exactLiquidity)} · OWNED L ${formatCompact(step.exactOurs)}`,
        share == null ? "NO ACTIVE LIQUIDITY" : `OWNED SHARE ${share.toLocaleString(undefined, { maximumFractionDigits: 6 })}%`
      );
    }
  }

  class TrackingChart {
    constructor(surface) {
      this.surface = surface;
      this.canvas = surface;
      this.context = surface.getContext("2d", { alpha: false });
      this.samples = [];
      this.sampleKey = "";
      this.width = 0;
      this.height = 0;
      this.pixelRatio = 0;
      this.frame = null;
      this.resizeObserver = new ResizeObserver(() => this.resize());
      this.resizeObserver.observe(surface.parentElement);
      this.resize();
    }

    clear() {
      this.samples = [];
      this.sampleKey = "";
      text(elements.trackingStatus, "—");
      elements.trackingStatus.classList.add("warming");
      elements.priceChartMessage.textContent = "AWAITING SAMPLES";
      elements.priceChartMessage.classList.remove("is-hidden");
      this.surface.setAttribute("aria-label", "Waiting for sampled prices for the selected pool.");
      this.requestDraw();
    }

    setSnapshot(detail) {
      const rows = Array.isArray(detail && detail.tracking) ? detail.tracking : [];
      const byBlock = new Map();
      rows.forEach((sample) => {
        const block = Number(sample.block);
        const timestamp = timestampMs(sample.timestamp);
        const price = Number(sample.price_token1_per_token0);
        if (Number.isFinite(block) && timestamp != null && price > 0 && Number.isFinite(price)) {
          byBlock.set(block, { block, timestamp: timestamp / 1000, rawPrice: price, activeLiquidity: sample.active_liquidity });
        }
      });
      const samples = [...byBlock.values()].sort((a, b) => a.timestamp - b.timestamp || a.block - b.block);
      const sampleKey = JSON.stringify(samples.map((sample) => [sample.block, sample.timestamp, sample.rawPrice, sample.activeLiquidity]));
      const changed = sampleKey !== this.sampleKey;
      this.samples = samples;
      this.sampleKey = sampleKey;
      const count = this.samples.length;
      const span = count > 1 ? Math.max(0, this.samples[count - 1].timestamp - this.samples[0].timestamp) : 0;
      const sufficient = count >= 2;
      text(elements.trackingStatus, sufficient
        ? `${count} · ${span < 60 ? `${Math.round(span)}S` : `${(span / 60).toFixed(1)}M`}`
        : `${count} SAMPLE${count === 1 ? "" : "S"}`);
      elements.trackingStatus.classList.toggle("warming", !sufficient);
      elements.priceChartMessage.textContent = count ? "NEED 2 SAMPLES" : "AWAITING SAMPLES";
      elements.priceChartMessage.classList.toggle("is-hidden", sufficient);
      this.surface.dataset.samples = String(count);
      this.surface.setAttribute("aria-label", sufficient
        ? `Graphical price track from ${count} block samples, ${formatTime(this.samples[0].timestamp)} through ${formatTime(this.samples[count - 1].timestamp)}.`
        : `Price track has ${count} block sample${count === 1 ? "" : "s"}.`);
      if (changed) this.requestDraw();
    }

    resize() {
      const rect = this.surface.getBoundingClientRect();
      if (rect.width < 2 || rect.height < 2) return;
      const width = Math.round(rect.width * 10) / 10;
      const height = Math.round(rect.height * 10) / 10;
      const pixelRatio = Math.max(1, window.devicePixelRatio || 1);
      if (width === this.width && height === this.height && pixelRatio === this.pixelRatio) return;
      this.width = width;
      this.height = height;
      this.pixelRatio = pixelRatio;
      this.canvas.width = Math.max(1, Math.round(width * pixelRatio));
      this.canvas.height = Math.max(1, Math.round(height * pixelRatio));
      this.requestDraw();
    }

    requestDraw() {
      if (this.width > 1 && this.height > 1 && this.frame == null) {
        this.frame = requestAnimationFrame(() => this.draw());
      }
    }

    draw() {
      this.frame = null;
      if (!this.context || this.width < 2 || this.height < 2) return;
      const context = this.context;
      context.setTransform(this.pixelRatio, 0, 0, this.pixelRatio, 0, 0);
      context.fillStyle = "#000";
      context.fillRect(0, 0, this.width, this.height);
      if (!this.samples.length) return;

      const points = this.samples.map((sample) => ({
        ...sample,
        price: chart.inverted ? 1 / sample.rawPrice : sample.rawPrice
      })).filter((sample) => sample.price > 0 && Number.isFinite(sample.price));
      if (!points.length) return;
      let minPrice = points[0].price;
      let maxPrice = points[0].price;
      for (const sample of points) {
        minPrice = Math.min(minPrice, sample.price);
        maxPrice = Math.max(maxPrice, sample.price);
      }
      if (minPrice === maxPrice) {
        const pad = minPrice * .0005 || 1;
        minPrice -= pad;
        maxPrice += pad;
      } else {
        const pad = (maxPrice - minPrice) * .08;
        minPrice -= pad;
        maxPrice += pad;
      }

      const left = this.width < 360 ? 49 : 57;
      const right = this.width - 7;
      const top = 8;
      const bottom = this.height - 21;
      const minTime = points[0].timestamp;
      const maxTime = points[points.length - 1].timestamp;
      const xFor = (sample, index) => left + (maxTime === minTime
        ? (points.length === 1 ? .5 : index / (points.length - 1))
        : (sample.timestamp - minTime) / (maxTime - minTime)) * (right - left);
      const yFor = (price) => top + (maxPrice - price) / (maxPrice - minPrice) * (bottom - top);

      context.strokeStyle = "#242424";
      context.lineWidth = 1;
      for (let division = 0; division <= 2; division += 1) {
        const y = Math.round(top + (bottom - top) * division / 2) + .5;
        context.beginPath();
        context.moveTo(left, y);
        context.lineTo(right, y);
        context.stroke();
      }

      context.beginPath();
      points.forEach((sample, index) => {
        const x = xFor(sample, index);
        const y = yFor(sample.price);
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.strokeStyle = "#5fd7ff";
      context.lineWidth = 1.5;
      context.stroke();

      points.forEach((sample, index) => {
        const x = xFor(sample, index);
        const y = yFor(sample.price);
        context.fillStyle = index === points.length - 1 ? "#d8d8d8" : "#5fd7ff";
        const size = index === points.length - 1 ? 5 : 3;
        context.fillRect(Math.round(x - size / 2), Math.round(y - size / 2), size, size);
      });

      context.strokeStyle = "#242424";
      context.beginPath();
      context.moveTo(left + .5, top);
      context.lineTo(left + .5, bottom + .5);
      context.lineTo(right, bottom + .5);
      context.stroke();

      context.font = '8px "SFMono-Regular", "SF Mono", Menlo, monospace';
      context.textBaseline = "middle";
      context.fillStyle = "rgba(216,216,216,.62)";
      context.textAlign = "right";
      context.fillText(formatPrice(maxPrice), left - 4, top + 1);
      context.fillText(formatPrice(minPrice), left - 4, bottom - 1);
      context.textAlign = "left";
      const shortTime = (timestamp) => formatTime(timestamp).slice(11);
      context.fillText(`#${points[0].block.toLocaleString()} · ${shortTime(minTime)}`, left, bottom + 12);
      const end = `#${points[points.length - 1].block.toLocaleString()} · ${shortTime(maxTime)}`;
      context.textAlign = "right";
      context.fillText(end, right, bottom + 12);
      context.fillStyle = "#d8d8d8";
      context.fillText(formatPrice(points[points.length - 1].price), right, top + 1);
    }
  }


  const chart = new LiquidityChart(elements.chartCanvas);
  const trackingChart = new TrackingChart(elements.priceCanvas);

  function bindControls() {
    let searchTimer = null;
    elements.poolSwitcher.addEventListener("click", () => {
      if (elements.catalogPanel.classList.contains("is-open")) closeCatalog({ restoreFocus: true });
      else {
        openCatalog();
        elements.search.focus({ preventScroll: true });
      }
    });
    elements.catalogClose.addEventListener("click", () => closeCatalog({ restoreFocus: true }));
    elements.catalogScrim.addEventListener("click", () => closeCatalog({ restoreFocus: true }));
    elements.search.addEventListener("input", () => {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(() => {
        state.catalog.q = elements.search.value.trim();
        state.catalog.offset = 0;
        loadCatalog();
      }, 220);
    });
    elements.kindFilter.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-kind]");
      if (!button) return;
      state.catalog.kind = button.dataset.kind;
      state.catalog.offset = 0;
      elements.kindFilter.querySelectorAll("button").forEach((item) => item.classList.toggle("is-active", item === button));
      loadCatalog();
    });
    elements.sort.addEventListener("change", () => {
      state.catalog.sort = elements.sort.value;
      state.catalog.offset = 0;
      loadCatalog();
    });
    elements.pagePrev.addEventListener("click", () => {
      state.catalog.offset = Math.max(0, state.catalog.offset - state.catalog.limit);
      loadCatalog();
      elements.catalogPanel && elements.catalogPanel.scrollIntoView({ behavior: "smooth", block: "start" });
    });
    elements.pageNext.addEventListener("click", () => {
      state.catalog.offset += state.catalog.limit;
      loadCatalog();
      elements.catalogPanel && elements.catalogPanel.scrollIntoView({ behavior: "smooth", block: "start" });
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "/" && !elements.transactionDialog.open && !elements.actionDrawer.classList.contains("is-open") && !/^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName)) {
        event.preventDefault();
        openCatalog();
        elements.search.focus({ preventScroll: true });
      }
      if (!elements.transactionDialog.open && elements.catalogPanel.classList.contains("is-open") && event.key === "Escape") {
        event.preventDefault();
        closeCatalog({ restoreFocus: true });
      }
      if (!elements.transactionDialog.open && elements.actionDrawer.classList.contains("is-open")) {
        if (event.key === "Escape") closeActionDrawer();
        if (event.key === "Tab") {
          const focusable = [...elements.actionDrawer.querySelectorAll('button:enabled, input:enabled, select:enabled, a[href], [tabindex="0"]')]
            .filter((element) => element.getClientRects().length);
          const first = focusable[0], last = focusable[focusable.length - 1];
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault(); last.focus();
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault(); first.focus();
          }
        }
      }
    });

    elements.selectedAddress.addEventListener("click", async () => {
      const pool = state.detail && state.detail.pool || state.selectedRow;
      const value = pool && (pool.id || pool.address);
      if (!value || !navigator.clipboard) return;
      try { await navigator.clipboard.writeText(value); toast("Pool identifier copied."); } catch (_) { toast("Could not copy the pool identifier."); }
    });
    elements.invertPrice.addEventListener("click", () => chart.toggleInvert());
    elements.returnLive.addEventListener("click", () => chart.follow());
    elements.fitChart.addEventListener("click", () => chart.fit());
    elements.participantSort.addEventListener("change", () => {
      state.participantSort = elements.participantSort.value;
      if (state.detail) renderParticipants(state.detail);
    });
    elements.flowFilter.addEventListener("change", () => {
      state.flowFilter = elements.flowFilter.value;
      state.selectedEventId = null;
      if (state.detail) renderEvents(state.detail);
      chart.selectEvent(null);
    });

    elements.ownerToggle.addEventListener("click", () => elements.ownerForm.classList.toggle("is-hidden"));
    elements.ownerForm.addEventListener("submit", (event) => {
      event.preventDefault();
      if (setWatchedOwner(elements.ownerInput.value)) elements.ownerForm.classList.add("is-hidden");
    });
    if (elements.poolTools) {
      elements.poolTools.addEventListener("toggle", () => {
        if (poolToolsOpen()) {
          const alreadyInitialized = state.allocationTicksInitialized;
          trackingChart.resize();
          if (state.detail) trackingChart.setSnapshot(state.detail);
          initializePoolTools();
          updatePricePresentation();
          if (alreadyInitialized) scheduleAllocationRequest();
          chart.requestDraw();
        } else {
          pausePoolTools();
        }
      });
    }

    elements.allocPresets.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-preset]");
      if (button) applyAllocationPreset(button.dataset.preset);
    });
    elements.allocShape.addEventListener("change", () => {
      elements.allocBands.disabled = !allocationSupported() || elements.allocShape.value === "uniform";
      scheduleAllocationRequest();
    });
    [elements.allocCapital, elements.allocBands, elements.allocHistoryWindow, elements.allocEntryGas, elements.allocExitGas].forEach((input) => {
      input.addEventListener("input", scheduleAllocationRequest);
      input.addEventListener("change", scheduleAllocationRequest);
    });
    [
      ["lower", "price", elements.allocPriceLower],
      ["lower", "tick", elements.allocTickLower],
      ["lower", "percent", elements.allocPctLower],
      ["upper", "price", elements.allocPriceUpper],
      ["upper", "tick", elements.allocTickUpper],
      ["upper", "percent", elements.allocPctUpper]
    ].forEach(([boundary, source, input]) => {
      input.addEventListener("change", () => changeAllocationBoundary(boundary, source));
    });

    elements.actionTabs.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-action]");
      if (button) setAction(button.dataset.action);
    });
    elements.actionForm.addEventListener("submit", (event) => { event.preventDefault(); simulateAction(); });
    elements.actionForm.addEventListener("input", (event) => {
      if (event.target === elements.rangeLower) syncTicksFromRanges("lower");
      else if (event.target === elements.rangeUpper) syncTicksFromRanges("upper");
      else if (event.target === elements.tickLower || event.target === elements.tickUpper) syncRangesFromTicks();
      if (event.target === elements.liquidityBps) text(elements.liquidityOutput, `${(Number(elements.liquidityBps.value) / 100).toFixed(2)}%`);
      if (event.target === elements.slippageBps) text(elements.slippageOutput, `${(Number(elements.slippageBps.value) / 100).toFixed(2)}%`);
      invalidateSimulation("Form changed");
      chart.requestDraw();
    });

    elements.connectWallet.addEventListener("click", connectWallet);
    elements.confirmTransaction.addEventListener("click", confirmReviewedTransaction);
    elements.transactionDialog.addEventListener("close", () => {
      state.review = null;
      setNotice(elements.reviewError, "");
    });
    elements.mobileActionOpen.addEventListener("click", openActionDrawer);
    elements.mobileActionClose.addEventListener("click", closeActionDrawer);
    elements.drawerScrim.addEventListener("click", closeActionDrawer);
    elements.embeddedClose.addEventListener("click", () => {
      setEmbeddedVisibility(false);
      postEmbedded({ type: "lp-workbench:close" });
    });
    elements.embeddedOpenOwner.addEventListener("click", () => {
      postEmbedded({ type: "lp-workbench:open-owner", owner: state.owner, poolId: state.selectedId });
    });
    window.addEventListener("message", (event) => {
      if (!EMBEDDED || event.origin !== window.location.origin || event.source !== window.parent) return;
      const payload = event.data;
      if (!payload || payload.type !== "lp-workbench:visibility" || typeof payload.open !== "boolean") return;
      setEmbeddedVisibility(payload.open);
    });
    const stopSubscriptions = () => {
      closeStream();
      window.clearTimeout(state.allocationTimer);
      if (state.detailController) state.detailController.abort();
      if (state.allocationController) state.allocationController.abort();
      if (state.simulationController) state.simulationController.abort();
    };
    window.addEventListener("beforeunload", stopSubscriptions);
    window.addEventListener("pagehide", stopSubscriptions);
  }

  function restoreSelectionFromUrl() {
    const url = new URL(window.location.href);
    const owner = String(url.searchParams.get("owner") || "").toLowerCase();
    if (ADDRESS_RE.test(owner)) {
      state.readOnlyOwner = owner;
      state.owner = owner;
      elements.ownerInput.value = owner;
      updateWalletPresentation();
    }
    const id = url.searchParams.get("id");
    if (!id) return false;
    const normalized = id.toLowerCase();
    const row = state.catalog.rows.find((item) => String(item.id || item.address || "").toLowerCase() === normalized);
    if (row) selectPool(row);
    else selectPool({ id: normalized, address: ADDRESS_RE.test(normalized) ? normalized : null, pair: "LOADING POOL" });
    return true;
  }

  function initialize() {
    updateClock();
    window.setInterval(updateClock, 1000);
    bindControls();
    closeCatalog();
    closeActionDrawer();
    updateWalletPresentation();
    setAction("add", { invalidate: false });
    if (!restoreSelectionFromUrl()) {
      selectPool({ id: DEFAULT_POOL, address: DEFAULT_POOL, kind: "v3", protocol: "v3-compatible", pair: "USDG/NVDA" });
    }
    loadMarketStatus();
    window.setInterval(() => {
      if (!document.hidden && (!EMBEDDED || state.embeddedOpen)) {
        if (elements.catalogPanel.classList.contains("is-open")) loadCatalog({ quiet: true });
        loadMarketStatus();
      }
    }, 15_000);
  }

  initialize();
})();
