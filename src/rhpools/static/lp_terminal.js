(() => {
  "use strict";

  const API_ROOT = "/api/lp";
  const MAX_TAPE_ROWS = 150;
  const TABLE_LIMIT = 100;
  const OWNER_STREAM_LIMIT = 200;
  const REFRESH_MS = 12_000;
  const HISTORY_REFRESH_MS = 15_000;
  const FILTER_DELAY_MS = 280;
  const ROBINSCAN = "https://robinscan.io";
  const WINDOW_KEYS = { "1": "1h", "2": "24h", "3": "7d", "4": "30d", "5": "all" };
  const POOL_SORT = { pools: ["fees", "desc"], flow: ["flow", "desc"], fresh: ["created", "desc"] };
  const OWNER_SORT_API = { activity: "activity", net_pnl_usd: "net", fees_usd: "fees" };
  const ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;
  const SEARCH_KINDS = new Set(["pool", "token", "protocol", "owner", "custody", "transaction", "position"]);
  const LP_EVENT_KINDS = new Set(["add", "remove", "collect", "checkpoint", "donate", "fee"]);
  const TOUCH_NAVIGATION = window.matchMedia("(hover: none) and (pointer: coarse)");
  const PANE_STORAGE_KEY = "lp-terminal-pane-layout-v1";
  const PANE_LAYOUT_MEDIA = window.matchMedia("(max-width: 520px), (max-height: 520px) and (max-width: 900px)");
  const PANE_PROPERTIES = ["--pane-tape-size", "--pane-owners-size", "--pane-pools-size"];
  const PANE_MINIMUMS = {
    desktop: [84, 104, 104],
    mobile: [280, 340, 340]
  };
  const POOL_SORT_FIELDS = {
    fee: "fee_ppm", tvl: "tvl_usd", active_tvl: "active_tvl_usd",
    observed_active_tvl: "observed_active_tvl_usd", volume: "volume_usd",
    fees: "fees_usd", flow: "net_deposits_usd", swaps: "swaps", adds: "adds",
    removes: "removes", lps: "lp_count", price: "price", change: "price_change_pct", created: "created_block"
  };

  const byId = (id) => document.getElementById(id);
  const elements = {
    streamState: byId("stream-state"),
    clock: byId("terminal-clock"),
    freshness: byId("strip-freshness"),
    context: byId("strip-context"),
    liveBlockLink: byId("live-block-link"),
    liveBlockAge: byId("live-block-age"),
    liveBlockGap: byId("live-block-gap"),
    status: byId("status-readout"),
    indexStatus: byId("index-status"),
    indexStatusShell: byId("index-status-shell"),
    indexStatusClose: byId("index-status-close"),
    indexStatusDetails: byId("index-status-details"),
    overview: byId("overview-readout"),
    endpointHealth: byId("endpoint-health"),
    marketFilter: byId("market-filter"),
    protocolFilter: byId("protocol-filter"),
    tapeKind: byId("tape-kind"),
    followControl: byId("follow-control"),
    followState: byId("follow-state"),
    filterControl: byId("filter-control"),
    ownerSort: byId("owner-sort"),
    ownerScope: byId("owner-scope"),
    ownersAccountingReadout: byId("owners-accounting-readout"),
    tabs: byId("pool-tabs"),
    poolPanel: byId("pool-panel"),
    tapeScroll: byId("tape-scroll"),
    tapeArrivals: byId("tape-arrivals"),
    footer: byId("footer-state"),
    paneReset: byId("pane-reset"),
    paneSeparators: Array.from(document.querySelectorAll(".pane-separator")),
    panes: [byId("tape-section"), byId("owners-section"), byId("pools-section")],
    lpSearchForm: byId("lp-search-form"),
    lpSearchInput: byId("lp-search-input"),
    lpSearchStatus: byId("lp-search-status"),
    lpSearchResults: byId("lp-search-results"),
    modal: byId("owner-modal"),
    dialog: byId("owner-dialog"),
    modalClose: byId("owner-close"),
    modalTitle: byId("owner-dialog-title"),
    modalAddress: byId("owner-dialog-address"),
    modalIdentity: byId("owner-dialog-identity"),
    modalError: byId("owner-dialog-error"),
    ownerSummary: byId("owner-summary"),
    ownerCurve: byId("owner-curve"),
    ownerCurveNote: byId("owner-curve-note"),
    ownerFollow: byId("owner-follow"),
    ownerLiveState: byId("owner-live-state"),
    copyStatus: byId("copy-status"),
    terminalMain: byId("terminal-main"),
    poolInspector: byId("pool-inspector"),
    poolInspectorShell: byId("pool-inspector-shell"),
    poolInspectorTitle: byId("pool-inspector-title"),
    poolInspectorContext: byId("pool-inspector-context"),
    poolInspectorNewTab: byId("pool-inspector-new-tab"),
    poolInspectorClose: byId("pool-inspector-close"),
    poolInspectorFrame: byId("pool-inspector-frame")
  };

  const state = {
    window: "24h",
    q: "",
    protocol: "",
    tapeKind: "lp",
    follow: true,
    tab: "pools",
    poolSort: "fees",
    poolOrder: "desc",
    poolScope: "",
    poolViews: new Map(),
    poolRequests: new Map(),
    ownerSort: "activity",
    ownerScope: "wallets",
    status: null,
    liveBlock: null,
    liveBlockSignature: "",
    liveBlockAgeLabel: "",
    liveBlockSequence: null,
    liveBlockFeedEpoch: "",
    namedBlockSeen: false,
    canonicalBlocks: new Map(),
    overview: null,
    tapeEnvelope: null,
    tapeRows: new Map(),
    tapeWatermark: null,
    arrivalFloor: 0,
    seenTapeKeys: new Map(),
    heldTapeRows: new Map(),
    ownersEnvelope: null,
    ownersReady: false,
    ownerViewCache: new Map(),
    poolsEnvelope: null,
    revision: null,
    epoch: null,
    stream: null,
    streamOwnersEnabled: true,
    streamGeneration: 0,
    streamRetryTimer: null,
    streamRetryMs: 1_000,
    aggregateController: null,
    aggregateGeneration: 0,
    refreshTimer: null,
    historyTimer: null,
    tapeController: null,
    tapeGeneration: 0,
    tapeLoading: false,
    pendingStreamFrames: [],
    pendingStreamReset: false,
    filterTimer: null,
    hidden: document.hidden,
    health: new Map(),
    renderQueue: new Map(),
    renderFrame: 0,
    ownerController: null,
    ownersVisible: true,
    poolsVisible: false,
    summaryObserver: null,
    ownerRequest: 0,
    ownerAddress: "",
    ownerDetail: null,
    ownerDetailAddress: "",
    ownerFollow: true,
    ownerLastSuccessAt: 0,
    ownerLastError: "",
    searchController: null,
    searchRequest: 0,
    searchTimer: null,
    modalReturnFocus: null,
    inspectorReturnFocus: null,
    inspectorScrollTop: 0,
    inspectorUrl: "",
    lastTapeAgeTick: -1,
    resizeFrame: 0,
    paneLayout: PANE_LAYOUT_MEDIA.matches ? "mobile" : "desktop",
    paneDrag: null,
    healthSignature: "",
    healthDetails: [],
  };

  window.__lpTerminalPerf = {
    createdRows: 0,
    patchedRows: 0,
    reconciles: 0,
    arrivalFlashes: 0,
    blockFrames: 0,
    activityFrames: 0,
    searches: 0,
    orphanRowsDropped: 0
  };

  const formats = {
    integer: new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }),
    decimal: new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }),
    dollars: new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
    smallDollars: new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 }),
    units: new Intl.NumberFormat(undefined, { maximumFractionDigits: 5 }),
    smallUnits: new Intl.NumberFormat(undefined, { maximumSignificantDigits: 5 })
  };

  function el(tag, className, value) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (value != null) node.textContent = String(value);
    return node;
  }

  function finite(value) {
    if (value == null || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function numeric(value, fallback = 0) {
    const number = finite(value);
    return number == null ? fallback : number;
  }

  function shortIdentifier(value, head = 7, tail = 5) {
    if (value == null || value === "") return "—";
    const text = String(value);
    return text.length > head + tail + 1 ? `${text.slice(0, head)}…${text.slice(-tail)}` : text;
  }

  function formatCount(value) {
    const number = finite(value);
    if (number == null) return "—";
    const absolute = Math.abs(number);
    if (absolute >= 1e12) return `${(number / 1e12).toFixed(2)}T`;
    if (absolute >= 1e9) return `${(number / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${(number / 1e6).toFixed(2)}M`;
    if (absolute >= 1e3) return `${(number / 1e3).toFixed(2)}K`;
    return (Number.isInteger(number) ? formats.integer : formats.decimal).format(number);
  }

  function formatUsd(value) {
    const number = finite(value);
    if (number == null) return "—";
    const absolute = Math.abs(number);
    const sign = number < 0 ? "-" : "";
    if (absolute >= 1e9) return `${sign}$${(absolute / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${sign}$${(absolute / 1e6).toFixed(2)}M`;
    if (absolute >= 1e4) return `${sign}$${formats.integer.format(Math.round(absolute))}`;
    return `${sign}$${(absolute < 1 ? formats.smallDollars : formats.dollars).format(absolute)}`;
  }

  function formatSignedUsd(value, forcedSign = false) {
    const number = finite(value);
    if (number == null) return "—";
    if (number > 0 || (forcedSign && number === 0)) return `+${formatUsd(number)}`;
    return formatUsd(number);
  }

  function valueClass(value) {
    const number = finite(value);
    if (number == null || Math.abs(number) < 0.0000001) return "neutral";
    return number > 0 ? "positive" : "negative";
  }

  function formatPercent(value, ratio = false) {
    let number = finite(value);
    if (number == null) return "—";
    if (ratio) number *= 100;
    return `${number > 0 ? "+" : ""}${number.toFixed(1)}%`;
  }

  function formatRate(value) {
    const number = finite(value);
    if (number == null) return "—";
    const percent = Math.abs(number) <= 1 ? number * 100 : number;
    return `${percent.toFixed(1)}%`;
  }

  function formatPrice(value) {
    const number = finite(value);
    if (number == null || number <= 0) return "—";
    if (number >= 1000) return formats.decimal.format(number);
    if (number >= 1) return number.toFixed(4);
    if (number >= 0.0001) return number.toFixed(6);
    return number.toExponential(3);
  }

  function toDate(value) {
    if (value == null || value === "") return null;
    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : new Date(value.getTime());
    }
    const text = String(value).trim();
    if (/^[+-]?\d+(?:\.\d+)?$/.test(text)) {
      const number = Number(text);
      if (!Number.isFinite(number)) return null;
      const date = new Date(Math.abs(number) < 1e11 ? number * 1000 : number);
      return Number.isNaN(date.getTime()) ? null : date;
    }
    const iso = text.replace(" ", "T");
    const normalized = /^\d{4}-\d{2}-\d{2}$/.test(iso)
      ? `${iso}T00:00:00Z`
      : /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/.test(iso)
        ? `${iso}Z`
        : iso;
    const date = new Date(normalized);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function formatTime(value, seconds = false) {
    const date = toDate(value);
    if (!date) return "—";
    return `${date.toISOString().slice(11, seconds ? 19 : 16)} UTC`;
  }

  function formatStamp(value) {
    const date = toDate(value);
    if (!date) return "—";
    return `${date.toISOString().slice(0, 19).replace("T", " ")} UTC`;
  }

  function formatDate(value) {
    const date = toDate(value);
    if (!date) return "—";
    return date.toISOString().slice(0, 10);
  }

  function formatAge(seconds) {
    const number = finite(seconds);
    if (number == null) return "—";
    if (number < 60) return `${Math.max(0, Math.round(number))}s`;
    if (number < 3600) return `${Math.round(number / 60)}m`;
    if (number < 86400) return `${(number / 3600).toFixed(1)}h`;
    return `${(number / 86400).toFixed(1)}d`;
  }

  function formatDuration(seconds) {
    const number = finite(seconds);
    if (number == null) return "—";
    if (number < 60) return `${Math.round(number)}s`;
    if (number < 3600) return `${Math.floor(number / 60)}m${String(Math.round(number % 60)).padStart(2, "0")}s`;
    if (number < 86400) return `${(number / 3600).toFixed(1)}h`;
    return `${(number / 86400).toFixed(1)}d`;
  }

  function formatUnits(value, decimals) {
    if (value == null || value === "") return "—";
    const text = String(value).trim();
    if (!/^[+-]?\d+(?:\.\d+)?$/.test(text)) return "—";
    if (decimals == null || decimals === "") return "—";
    const decimalCount = Number(decimals);
    if (!Number.isInteger(decimalCount) || decimalCount < 0 || decimalCount > 255) return "—";
    let display = text;
    if (!text.includes(".")) {
      const sign = text.startsWith("-") ? "-" : text.startsWith("+") ? "+" : "";
      const digits = text.replace(/^[+-]/, "").padStart(decimalCount + 1, "0");
      const split = digits.length - decimalCount;
      display = decimalCount ? `${sign}${digits.slice(0, split)}.${digits.slice(split)}` : `${sign}${digits}`;
    }
    const number = Number(display);
    if (!Number.isFinite(number)) return display;
    const absolute = Math.abs(number);
    if (absolute >= 1e12) return `${(number / 1e12).toFixed(3)}T`;
    if (absolute >= 1e9) return `${(number / 1e9).toFixed(3)}B`;
    if (absolute >= 1e6) return `${(number / 1e6).toFixed(3)}M`;
    if (absolute >= 1e3) return `${(number / 1e3).toFixed(3)}K`;
    if (absolute === 0) return "0";
    return (absolute >= 1 ? formats.units : formats.smallUnits).format(number);
  }

  function tokenMeta(row, side) {
    const nested = row && row[`token${side}`];
    if (nested && typeof nested === "object") {
      return {
        address: nested.address || "",
        symbol: nested.symbol || row[`symbol${side}`] || null,
        decimals: nested.decimals != null ? nested.decimals : row[`decimals${side}`]
      };
    }
    return {
      address: typeof nested === "string" ? nested : "",
      symbol: row && (row[`symbol${side}`] || row[`token${side}_symbol`]) || null,
      decimals: row && row[`decimals${side}`]
    };
  }

  function tokenLabel(token) {
    return token.symbol || (token.address ? shortIdentifier(token.address) : "unresolved token");
  }

  function pairFor(row) {
    const token0 = tokenMeta(row || {}, 0);
    const token1 = tokenMeta(row || {}, 1);
    if (token0.address || token1.address || token0.symbol || token1.symbol) {
      return `${tokenLabel(token0)} / ${tokenLabel(token1)}`;
    }
    const pair = row && row.pair;
    return pair && !/^0x[0-9a-f]+$/i.test(pair)
      ? pair : shortIdentifier(pair || row && (row.pool_id || row.id));
  }

  function flowFor(row, side, transaction = false) {
    const token = tokenMeta(row, side);
    const raw = transaction ? row[`transaction_flow${side}`]
      : row[`cashflow${side}`] != null ? row[`cashflow${side}`] : row[`amount${side}`];
    const amount = formatUnits(raw, token.decimals);
    if (amount === "—") return amount;
    const rawText = String(raw).trim();
    const signed = !rawText.startsWith("-") && /[1-9]/.test(rawText) ? `+${amount.replace(/^\+/, "")}` : amount;
    return `${signed} ${tokenLabel(token)}`;
  }

  function eventUsd(row) {
    if (row.cashflow_usd != null) return finite(row.cashflow_usd);
    const kind = String(row.kind || "").toLowerCase();
    if (kind === "add" || kind === "donate") {
      const deposit = finite(row.deposit_usd);
      return deposit == null ? null : -Math.abs(deposit);
    }
    if (kind === "remove") {
      const withdrawal = finite(row.withdrawal_usd);
      return withdrawal == null ? null : Math.abs(withdrawal);
    }
    if (kind === "collect" || kind === "fee") {
      const fees = finite(row.fees_usd);
      if (fees != null) return Math.abs(fees);
      const withdrawal = finite(row.withdrawal_usd);
      return withdrawal == null ? null : Math.abs(withdrawal);
    }
    return finite(row.size_usd != null ? row.size_usd : row.volume_usd);
  }

  function eventKey(row) {
    if (row && row.block_hash && row.tx_hash && row.log_index != null) {
      return `event:${row.block_hash}:${row.tx_hash}:${row.log_index}`;
    }
    if (row && row.id != null) return `event:${row.id}`;
    return `event:${row && (row.block_hash || row.block_number) || ""}:${row && row.tx_hash || ""}:${row && row.log_index != null ? row.log_index : ""}`;
  }


  function compareCanonicalAscending(left, right) {
    let order = numeric(left && left.block_number, -1) - numeric(right && right.block_number, -1);
    if (order) return order;
    order = numeric(left && left.tx_index, -1) - numeric(right && right.tx_index, -1);
    if (order) return order;
    order = numeric(left && left.log_index, -1) - numeric(right && right.log_index, -1);
    if (order) return order;
    const leftTime = toDate(left && left.timestamp);
    const rightTime = toDate(right && right.timestamp);
    order = (leftTime ? leftTime.getTime() : -1) - (rightTime ? rightTime.getTime() : -1);
    if (order) return order;
    order = numeric(left && left.id, -1) - numeric(right && right.id, -1);
    return order || eventKey(left).localeCompare(eventKey(right));
  }

  function compareCanonicalDescending(left, right) {
    return compareCanonicalAscending(right, left);
  }

  function normalizedAddress(value) {
    const address = String(value || "").toLowerCase();
    return ADDRESS_RE.test(address) ? address : "";
  }

  function ownerIdentityKey(row) {
    const owner = normalizedAddress(row && row.owner);
    if (owner) return `owner:${owner}`;
    const custody = normalizedAddress(row && row.custody);
    return custody ? `custody:${custody}` : "";
  }


  function activitySignature(activity) {
    if (!activity || typeof activity !== "object") return "";
    return [
      activity.block_number, activity.timestamp, activity.pool_id, activity.pair,
      activity.kind, activity.tx_hash, activity.event_count, activity.qualification
    ].join("|");
  }


  function compareOwnerActivity(left, right) {
    let order = numeric(left && left.block_number, -1) - numeric(right && right.block_number, -1);
    if (order) return order;
    const leftTime = toDate(left && left.timestamp);
    const rightTime = toDate(right && right.timestamp);
    order = (leftTime ? leftTime.getTime() : -1) - (rightTime ? rightTime.getTime() : -1);
    if (order) return order;
    return String(left && left.tx_hash || "").localeCompare(String(right && right.tx_hash || ""));
  }

  function isCurrentOnlyOwner(row) {
    if (!row || !row.activity || row.activity.qualification !== "provisional_canonical") return false;
    return ["positions", "open_positions", "closed_episodes", "fees_usd", "gross_pnl_usd",
      "gas_usd", "net_pnl_usd", "volume_usd", "win_rate"].every((field) => row[field] == null);
  }

  function errorMessage(error) {
    if (!error) return "unknown error";
    if (typeof error === "string") return error;
    return error.message ? String(error.message) : "unknown error";
  }

  function describeCoverage(coverage) {
    if (coverage == null || coverage === "") return "coverage unknown";
    if (typeof coverage === "string") return coverage;
    if (typeof coverage === "number") return `${coverage <= 1 ? (coverage * 100).toFixed(1) : coverage.toFixed(1)}% coverage`;
    if (typeof coverage !== "object") return "coverage unknown";
    const label = coverage.label || coverage.state || coverage.status || coverage.level;
    const progress = finite(coverage.progress_pct != null ? coverage.progress_pct : coverage.percent);
    const from = coverage.history_from || coverage.from;
    const to = coverage.history_to || coverage.to;
    const pieces = [];
    if (label) pieces.push(String(label));
    else if (coverage.complete === true) pieces.push("complete");
    else if (coverage.complete === false || coverage.partial === true) pieces.push("partial");
    else if (coverage.history) pieces.push(`history ${coverage.history}`);
    if (coverage.qualified === true) pieces.push("qualified");
    else if (coverage.qualified === false) pieces.push("unqualified");
    if (progress != null) pieces.push(`${progress.toFixed(1)}%`);
    if (from || to) pieces.push(`${formatDate(from)}→${formatDate(to)}`);
    if (Array.isArray(coverage.reasons) && coverage.reasons.length) pieces.push(`${coverage.reasons.length} caveat${coverage.reasons.length === 1 ? "" : "s"}`);
    return pieces.length ? pieces.join(" · ") : "coverage reported; scope unspecified";
  }


  function setTextCell(cell, value, className, title) {
    const text = value == null || value === "" ? "—" : String(value);
    const nextClass = className || "";
    const nextTitle = title || "";
    if (cell.dataset.value !== text) {
      cell.textContent = text;
      cell.dataset.value = text;
    }
    if (cell.className !== nextClass) cell.className = nextClass;
    if (cell.title !== nextTitle) cell.title = nextTitle;
  }

  function setNodeCell(cell, signature, className, builder) {
    const sig = String(signature);
    if (cell.dataset.signature !== sig) {
      const content = builder();
      cell.replaceChildren(...(Array.isArray(content) ? content : [content]));
      cell.dataset.signature = sig;
    }
    if (cell.className !== (className || "")) cell.className = className || "";
  }

  function internalPoolLink(poolId, owner, label = "open", txHash = "") {
    const link = el("a", "row-link", label);
    if (!poolId) {
      link.removeAttribute("href");
      link.textContent = "—";
      return link;
    }
    const params = new URLSearchParams({ id: String(poolId) });
    if (owner) params.set("owner", String(owner));
    if (txHash) params.set("tx", String(txHash));
    link.href = `/pool?${params.toString()}`;
    return link;
  }

  function externalLink(href, label, title) {
    const link = el("a", "row-link", label);
    link.href = href;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    if (title) link.title = title;
    return link;
  }

  function copyButton(value, label = shortIdentifier(value)) {
    const button = el("button", "address-control", label);
    button.type = "button";
    button.dataset.copy = String(value);
    button.title = `Copy ${value}`;
    button.setAttribute("aria-label", `Copy ${value}`);
    return button;
  }

  function internalOwnerHref(address) {
    return `/lp?${new URLSearchParams({ owner: String(address), window: state.window }).toString()}`;
  }

  function ownerNodes(row, copyOnClick = true) {
    const address = String(row && (row.owner || row.custody) || "");
    if (!address) return [el("span", "dim", "—")];
    const link = el("a", "address-control", shortIdentifier(address));
    link.dataset.owner = address;
    if (copyOnClick) link.dataset.copy = address;
    link.href = internalOwnerHref(address);
    link.title = `${address}\n${!row.owner && row.custody ? "Custody contract; not an individual wallet.\n" : ""}${copyOnClick ? "Tap for details. With a mouse: click to copy; double-click for details." : "Open positions and accounting."}`;
    link.setAttribute("aria-label", `LP ${address}`);
    return [link];
  }

  function selectedIdentity(address) {
    const normalized = String(address || "").toLowerCase();
    return ownerSourceRows().find((row) =>
      [row && row.owner, row && row.custody]
        .filter(Boolean)
        .some((candidate) => String(candidate).toLowerCase() === normalized)
    ) || null;
  }

  function safeSearchHref(value) {
    if (!value) return "";
    try {
      const url = new URL(String(value), window.location.origin);
      if (url.username || url.password) return "";
      if (url.origin === window.location.origin && ["/lp", "/pool", "/pools"].includes(url.pathname)) {
        return `${url.pathname}${url.search}${url.hash}`;
      }
      if (url.origin === new URL(ROBINSCAN).origin && url.protocol === "https:") return url.href;
    } catch (_) {
      return "";
    }
    return "";
  }

  function renderSearchResults(payload, query) {
    const source = payload && Array.isArray(payload.rows) ? payload.rows : [];
    const rows = source.slice(0, 30).map((row) => ({
      kind: String(row && row.kind || "").toLowerCase(),
      id: String(row && row.id || ""),
      label: String(row && row.label || row && row.id || ""),
      subtitle: String(row && row.subtitle || ""),
      href: safeSearchHref(row && row.href)
    })).filter((row) => SEARCH_KINDS.has(row.kind) && row.id && row.label && row.href);
    const coverage = describeCoverage(payload && payload.coverage);
    elements.lpSearchResults.replaceChildren();
    elements.lpSearchStatus.title = coverage;
    elements.lpSearchResults.title = coverage;
    elements.lpSearchResults.setAttribute("aria-label", `Search results for ${query}`);
    if (!rows.length) {
      const empty = el("div", "lp-result-empty", "NO INDEXED MATCH");
      empty.setAttribute("role", "note");
      elements.lpSearchResults.append(empty);
      elements.lpSearchStatus.className = "lp-search-status";
      elements.lpSearchStatus.textContent = "0 results";
      return;
    }
    for (const row of rows) {
      const link = el("a", "search-result");
      link.href = row.href;
      link.setAttribute("role", "listitem");
      link.title = [row.kind, row.label, row.subtitle, row.id].filter(Boolean).join(" · ");
      if (row.href.startsWith("https://")) {
        link.target = "_blank";
        link.rel = "noopener noreferrer";
      } else if (["owner", "custody"].includes(row.kind) && ADDRESS_RE.test(row.id)) {
        link.dataset.owner = row.id.toLowerCase();
      }
      link.append(
        el("span", "search-kind", `[${row.kind}]`),
        el("span", "search-label", row.label),
        el("span", "search-open", ">"),
        el("span", "search-subtitle", row.subtitle || row.id)
      );
      elements.lpSearchResults.append(link);
    }
    elements.lpSearchStatus.className = "lp-search-status";
    elements.lpSearchStatus.textContent = `${rows.length} results`;
    elements.lpSearchStatus.title = coverage;
  }

  async function searchUniversal(query) {
    const normalized = String(query || "").trim();
    if (state.searchController) state.searchController.abort();
    if (!normalized) {
      elements.lpSearchResults.setAttribute("aria-busy", "false");
      elements.lpSearchResults.replaceChildren();
      elements.lpSearchStatus.className = "lp-search-status";
      elements.lpSearchStatus.textContent = "";
      elements.lpSearchStatus.title = "";
      return;
    }
    const request = ++state.searchRequest;
    const controller = new AbortController();
    state.searchController = controller;
    elements.lpSearchResults.replaceChildren();
    elements.lpSearchResults.setAttribute("aria-busy", "true");
    elements.lpSearchStatus.className = "lp-search-status";
    elements.lpSearchStatus.textContent = "SEARCHING INDEX";
    try {
      const payload = await api("/search", { q: normalized, limit: 30 }, controller.signal);
      window.__lpTerminalPerf.searches += 1;
      if (request !== state.searchRequest || controller.signal.aborted) return;
      renderSearchResults(payload || {}, normalized);
    } catch (error) {
      if (error.name === "AbortError" || request !== state.searchRequest) return;
      elements.lpSearchStatus.className = "lp-search-status is-stale";
      elements.lpSearchStatus.textContent = `SEARCH ERROR · ${errorMessage(error)}`;
    } finally {
      if (request === state.searchRequest) {
        state.searchController = null;
        elements.lpSearchResults.setAttribute("aria-busy", "false");
      }
    }
  }


  function makeTableRow(cellCount) {
    const row = document.createElement("tr");
    for (let index = 0; index < cellCount; index += 1) row.append(document.createElement("td"));
    window.__lpTerminalPerf.createdRows += 1;
    return row;
  }

  class KeyedTable {
    constructor(body, keyFor, createRow, patchRow, columns) {
      this.body = body;
      this.keyFor = keyFor;
      this.createRow = createRow;
      this.patchRow = patchRow;
      this.columns = columns;
      this.rows = new Map();
      this.emptyRow = null;
      this.items = new Map();
    }

    reconcile(data, emptyLabel) {
      window.__lpTerminalPerf.reconciles += 1;
      const desired = [];
      const keys = new Set();
      for (const item of data) {
        const key = this.keyFor(item);
        if (!key || keys.has(key)) continue;
        keys.add(key);
        desired.push([key, item]);
      }
      const scroller = this.body.closest(".table-wrap");
      let anchorRow = null;
      let anchorTop = null;
      if (scroller && !(scroller === elements.tapeScroll && state.follow && scroller.scrollTop <= 4)) {
        const activeRow = document.activeElement && document.activeElement.closest
          ? document.activeElement.closest("tr")
          : null;
        const hoveredRow = this.body.querySelector("tr:hover");
        if (activeRow && this.body.contains(activeRow)) anchorRow = activeRow;
        else if (hoveredRow) anchorRow = hoveredRow;
        else if (scroller.scrollTop > 0) {
          const scrollerTop = scroller.getBoundingClientRect().top;
          anchorRow = Array.from(this.body.children).find((row) => row.getBoundingClientRect().bottom > scrollerTop) || null;
        }
        if (anchorRow) anchorTop = anchorRow.getBoundingClientRect().top;
      }
      if (this.emptyRow) {
        this.emptyRow.remove();
        this.emptyRow = null;
      }
      for (const [key, row] of this.rows) {
        if (!keys.has(key)) {
          row.remove();
          this.rows.delete(key);
          this.items.delete(key);
        }
      }
      desired.forEach(([key, item], index) => {
        let row = this.rows.get(key);
        if (!row) {
          row = this.createRow(item, key);
          row.dataset.key = key;
          this.rows.set(key, row);
        }
        if (this.items.get(key) !== item) {
          this.patchRow(row, item, key);
          this.items.set(key, item);
          window.__lpTerminalPerf.patchedRows += 1;
        }
        const reference = this.body.children[index] || null;
        if (reference !== row) this.body.insertBefore(row, reference);
      });
      if (!desired.length) {
        const row = document.createElement("tr");
        const cell = el("td", "empty-cell", emptyLabel);
        cell.colSpan = this.columns;
        row.append(cell);
        this.body.append(row);
        this.emptyRow = row;
      }
      if (anchorRow && anchorTop != null && anchorRow.isConnected) {
        scroller.scrollTop += anchorRow.getBoundingClientRect().top - anchorTop;
      }
    }

    rowFor(key) {
      return this.rows.get(key);
    }
  }

  function positionText(row) {
    const identity = row.token_id != null ? `NFT ${row.token_id}` : row.position_key ? shortIdentifier(row.position_key, 8, 5) : "—";
    if (row.tick_lower != null || row.tick_upper != null) return `${identity} [${row.tick_lower == null ? "—" : row.tick_lower},${row.tick_upper == null ? "—" : row.tick_upper}]`;
    return identity;
  }

  function eventTimeLabel(item) {
    const date = toDate(item && item.timestamp);
    const block = item && item.block_number != null ? `#${item.block_number}` : "#—";
    if (!date) return { text: "—", title: `Block ${block}` };
    const age = formatAge(Math.max(0, (Date.now() - date.getTime()) / 1000));
    return { text: age, title: `${formatStamp(date)} · block ${block}` };
  }

  function transactionTransferTitle(item) {
    const transfers = Array.isArray(item && item.transaction_transfers) ? item.transaction_transfers : [];
    const details = transfers.slice(0, 8).map((transfer) => {
      const token = shortIdentifier(transfer && transfer.token, 7, 5);
      const from = shortIdentifier(transfer && transfer.from, 5, 4);
      const to = shortIdentifier(transfer && transfer.to, 5, 4);
      return `${token} raw ${transfer && transfer.amount != null ? transfer.amount : "—"} ${from}→${to} log ${transfer && transfer.log_index != null ? transfer.log_index : "—"}`;
    });
    return [
      "Verified receipt transfers; transaction-scoped, not LP position principal",
      "Amounts without verified token decimals are unavailable.",
      ...details,
      transfers.length > details.length ? `+${transfers.length - details.length} more` : null
    ].filter(Boolean).join("\n");
  }
  function transactionTransferLabel(item, transfer) {
    if (!transfer) return "TX NO ERC20 FLOW";
    const address = String(transfer.token || "").toLowerCase();
    for (const side of [0, 1]) {
      const token = tokenMeta(item, side);
      if (token.address.toLowerCase() === address && token.decimals != null) {
        return `TX ${formatUnits(transfer.amount, token.decimals)} ${tokenLabel(token)}`;
      }
    }
    return `TX — ${shortIdentifier(address, 4, 3)}`;
  }


  function patchTapeRow(row, item) {
    const kind = String(item.kind || "unknown").toLowerCase();
    row.className = `event-${kind}`;
    const cells = row.cells;
    const eventTime = eventTimeLabel(item);
    setTextCell(cells[0], eventTime.text, "dim", eventTime.title);
    setTextCell(cells[1], kind, "event-kind");
    const poolId = item.pool_id || "";
    setNodeCell(cells[2], `${poolId}|${pairFor(item)}|${item.protocol}`, "pair-symbol", () => {
      const link = internalPoolLink(poolId, "", `${pairFor(item)} ${String(item.protocol || "").toUpperCase()}`, item.tx_hash);
      link.title = `Open ${pairFor(item)} liquidity depth\n${poolId}`;
      return link;
    });
    setNodeCell(cells[3], `${item.owner || ""}|${item.custody || ""}`, "cyan", () => ownerNodes(item));
    const transactionFlow = item.flow_scope === "transaction" && item.flow_complete === true;
    const transfers = Array.isArray(item.transaction_transfers) ? item.transaction_transfers : [];
    const netTransfers = transactionFlow && (item.transaction_flow0 != null || item.transaction_flow1 != null);
    const flows = netTransfers
      ? [flowFor(item, 0, true), flowFor(item, 1, true)].filter((value) => value !== "—").map((value) => `TX ${value}`)
      : transactionFlow ? transfers.slice(0, 2).map((transfer) => transactionTransferLabel(item, transfer))
        : [flowFor(item, 0), flowFor(item, 1)].filter((value) => value !== "—");
    const flowTitle = transactionFlow ? transactionTransferTitle(item) : positionText(item);
    setNodeCell(cells[4], flows.join("\n"), "numeric token-flows", () =>
      flows.length ? flows.map((value) => el("span", "token-flow", value)) : document.createTextNode("—"));
    cells[4].title = flowTitle;
    const usdFlow = finite(item.usdg_flow_usd) ?? eventUsd(item);
    setTextCell(cells[5], kind === "swap" ? formatUsd(usdFlow) : formatSignedUsd(usdFlow, true), `numeric desktop-column ${kind === "swap" ? "neutral" : valueClass(usdFlow)}`);
    const feeValue = finite(item.fees_usd);
    const feePpm = finite(item.pool_fee_ppm);
    const feeRate = feePpm != null && feePpm >= 0 && feePpm <= 1e6
      ? `${(feePpm / 1e4).toFixed(4).replace(/\.?0+$/, "")}% rate` : null;
    const dynamicFee = item.pool_fee_basis === "dynamic_fee_flag_not_a_rate";
    const feeLabel = feeValue != null ? formatUsd(feeValue) : feeRate || (dynamicFee ? "dynamic" : "—");
    const feeTitle = feeValue != null
      ? ["Fee value in USD", item.fees_scope, item.fees_basis, item.fees_qualification].filter(Boolean).join("\n").replaceAll("_", " ")
      : feeRate ? "Verified pool fee rate, not earned fees. This event does not attribute fees to an LP position."
        : dynamicFee ? "Dynamic pool fee; this event does not establish the rate or earned fees."
          : String(item.fees_qualification || "Fee valuation unavailable").replaceAll("_", " ");
    setTextCell(cells[6], feeLabel, `numeric desktop-column ${feeValue == null ? "dim" : valueClass(feeValue)}`, feeTitle);
    const tx = item.tx_hash || "";
    setNodeCell(cells[7], tx, "cyan desktop-column", () => tx ? externalLink(`${ROBINSCAN}/tx/${encodeURIComponent(tx)}`, shortIdentifier(tx), tx) : el("span", "dim", "—"));
  }
  function ownerActivityAgeView(activity) {
    const eventTime = eventTimeLabel(activity);
    const qualification = String(activity && activity.qualification || "");
    return {
      text: eventTime.text,
      title: [
        eventTime.title,
        qualification ? `Activity qualification: ${qualification.replaceAll("_", " ")}` : "Activity qualification unavailable",
        activity && activity.kind ? `Action: ${String(activity.kind).toLowerCase()}` : null,
        activity && (activity.pair || activity.pool_id) ? `Pool: ${activity.pair || activity.pool_id}` : null,
        activity && activity.block_number != null ? `Block #${activity.block_number}` : null,
        activity && activity.tx_hash ? `Transaction ${activity.tx_hash}` : null
      ].filter(Boolean).join("\n"),
      className: `owner-activity-age${qualification === "provisional_canonical" ? " is-live" : qualification === "durable_canonical_index" ? " is-ledger" : ""}`
    };
  }

  function ownerActivityNodes(item) {
    const activity = item.activity;
    if (!activity || typeof activity !== "object") return el("span", "dim", "—");
    const ageView = ownerActivityAgeView(activity);
    const age = el("span", ageView.className, ageView.text);
    age.title = ageView.title;
    return age;
  }

  function patchOwnerRow(row, item) {
    const cells = row.cells;
    const currentOnly = isCurrentOnlyOwner(item);
    const custody = !item.owner && Boolean(item.custody);
    const activity = item.activity || {};
    const kind = String(activity.kind || "").toLowerCase();
    row.className = `event-${kind}${currentOnly ? " owner-current-only" : ""}`;
    setNodeCell(cells[0], `${item.owner || ""}|${item.custody || ""}`, "cyan", () => ownerNodes(item, false));
    const action = { add: "ADD LIQUIDITY", remove: "REMOVE LIQUIDITY", collect: "COLLECT", checkpoint: "FEE CHECKPOINT", transfer: "TRANSFER", donate: "DONATE", fee: "FEE UPDATE" }[kind] || (kind ? kind.toUpperCase() : "UNKNOWN");
    setTextCell(cells[1], action, "event-kind", ownerActivityAgeView(activity).title);
    const poolId = activity.pool_id || "";
    const poolLabel = activity.pair || poolId ? pairFor(activity) : "Not resolved";
    setNodeCell(cells[2], `${poolId}|${poolLabel}|${item.owner || ""}`, "pair-symbol", () =>
      poolId ? internalPoolLink(poolId, item.owner || "", poolLabel, activity.tx_hash || "") : el("span", "dim", poolLabel));
    setNodeCell(cells[3], activitySignature(activity), "owner-activity-cell", () => ownerActivityNodes(item));
    setTextCell(cells[4], formatCount(item.open_positions), `numeric ledger-first ${item.open_positions == null ? "unknown" : ""}`, "Open positions in the historical indexed snapshot; newer activity may not be accounted yet.");
    setTextCell(cells[5], formatUsd(item.fees_usd), `numeric ${item.fees_usd == null ? "unknown" : "positive"}`);
    setTextCell(cells[6], formatSignedUsd(item.net_pnl_usd), `numeric ${valueClass(item.net_pnl_usd)}`);
    const pending = item.financials && item.financials.pending;
    const coverage = custody ? "CUSTODY ≠ OWNER" : currentOnly ? "ACTIVITY ONLY"
      : pending ? "ACCOUNTING BEHIND" : item.coverage && item.coverage.cost_qualified ? "ACCOUNTED" : "PARTIAL HISTORY";
    setTextCell(cells[7], coverage, coverage === "ACCOUNTED" ? "dim" : "unknown", [
      describeCoverage(item.coverage),
      "Fees and net P/L use USDG quote units, not a fiat oracle.",
      "Select the wallet for position details, cashflows, gas and attribution."
    ].filter(Boolean).join("\n"));
  }
  function patchPoolRow(row, item) {
    const cells = row.cells;
    const poolId = item.id || item.pool_id || "";
    setNodeCell(cells[0], `${poolId}|${pairFor(item)}`, "pair-symbol", () => {
      const link = internalPoolLink(poolId, "", pairFor(item));
      link.title = `Open ${pairFor(item)} liquidity depth\n${poolId}`;
      return link;
    });
    setTextCell(cells[1], item.protocol || "—", "protocol");
    const fee = finite(item.fee_ppm);
    setTextCell(cells[2], fee == null ? "—" : `${(fee / 10_000).toFixed(fee % 100 === 0 ? 2 : 4)}%`, "numeric dim");
    setTextCell(cells[3], formatUsd(item.tvl_usd), `numeric ${item.tvl_usd == null ? "unknown" : ""}`, item.tvl_basis || "");
    setTextCell(cells[4], formatUsd(item.active_tvl_usd), `numeric desktop-column ${item.active_tvl_usd == null ? "unknown" : ""}`);
    setTextCell(cells[5], formatUsd(item.observed_active_tvl_usd), `numeric desktop-column ${item.observed_active_tvl_usd == null ? "unknown" : ""}`);
    setTextCell(cells[6], formatUsd(item.volume_usd), `numeric ${item.volume_usd == null ? "unknown" : ""}`);
    setTextCell(cells[7], formatUsd(item.fees_usd), `numeric ${item.fees_usd == null ? "unknown" : "positive"}`);
    setTextCell(cells[8], formatSignedUsd(item.net_deposits_usd), `numeric ${valueClass(item.net_deposits_usd)}`);
    setTextCell(cells[9], formatCount(item.swaps), "numeric dim desktop-column");
    setTextCell(cells[10], formatCount(item.adds), "numeric dim desktop-column");
    setTextCell(cells[11], formatCount(item.removes), "numeric dim desktop-column");
    setTextCell(cells[12], formatCount(item.lp_count), "numeric");
    setTextCell(cells[13], formatPrice(item.price), "numeric dim desktop-column", "Token1 per token0; canonical pool orientation");
    setTextCell(cells[14], formatPercent(item.price_change_pct), `numeric desktop-column ${valueClass(item.price_change_pct)}`);
  }

  function patchPositionRow(row, item) {
    const cells = row.cells;
    setTextCell(cells[0], pairFor(item), "pair-symbol");
    setTextCell(cells[1], item.protocol || "—", "protocol");
    setTextCell(cells[2], positionText(item), "event-position", item.position_key || "");
    setTextCell(cells[3], formatUnits(item.liquidity, 0), "numeric dim");
    setTextCell(cells[4], formatUsd(item.principal_usd), `numeric ${item.principal_usd == null ? "unknown" : ""}`);
    setTextCell(cells[5], formatUsd(item.uncollected_fees_usd), `numeric ${item.uncollected_fees_usd == null ? "unknown" : "positive"}`);
    setTextCell(cells[6], formatUsd(item.equity_usd), `numeric ${item.equity_usd == null ? "unknown" : ""}`);
    const valuation = item.valuation_timestamp
      ? `${formatStamp(item.valuation_timestamp)} · ${item.valuation_basis || "unknown"}`
      : item.valuation_block != null
        ? `#${formats.integer.format(item.valuation_block)} · ${item.valuation_basis || "unknown"}`
        : item.valuation_basis || "—";
    setTextCell(cells[7], valuation, "dim");
    setTextCell(cells[8], item.status || "open", "dim");
    const poolId = item.pool_id || "";
    setNodeCell(cells[9], `${poolId}|${state.ownerAddress}`, "cyan", () => internalPoolLink(poolId, state.ownerAddress));
  }

  function patchOwnerClosedRow(row, item) {
    const cells = row.cells;
    setTextCell(cells[0], formatStamp(item.closed_at), "dim");
    setTextCell(cells[1], pairFor(item), "pair-symbol");
    setTextCell(cells[2], item.protocol || "—", "protocol");
    setTextCell(cells[3], formatUsd(item.deposit_usd), `numeric ${item.deposit_usd == null ? "unknown" : "negative"}`);
    setTextCell(cells[4], formatUsd(item.withdrawal_usd), `numeric ${item.withdrawal_usd == null ? "unknown" : "positive"}`);
    setTextCell(cells[5], formatUsd(item.fees_usd), `numeric ${item.fees_usd == null ? "unknown" : "positive"}`);
    setTextCell(cells[6], formatSignedUsd(item.gross_pnl_usd), `numeric ${valueClass(item.gross_pnl_usd)}`);
    setTextCell(cells[7], item.gas_usd == null ? "—" : `-${formatUsd(Math.abs(numeric(item.gas_usd)))}`, `numeric ${item.gas_usd == null ? "unknown" : "negative"}`);
    setTextCell(cells[8], formatSignedUsd(item.net_pnl_usd), `numeric ${valueClass(item.net_pnl_usd)}`);
    setTextCell(cells[9], formatPercent(item.return_pct), `numeric ${valueClass(item.return_pct)}`);
    setTextCell(cells[10], formatDuration(item.duration_s), "numeric dim");
    const poolId = item.pool_id || "";
    setNodeCell(cells[11], `${poolId}|${state.ownerAddress}`, "cyan", () => internalPoolLink(poolId, state.ownerAddress));
  }

  const tapeTable = new KeyedTable(byId("tape-body"), eventKey, () => makeTableRow(8), patchTapeRow, 8);
  const ownersTable = new KeyedTable(byId("owners-body"), (row) => ownerIdentityKey(row), () => makeTableRow(8), patchOwnerRow, 8);
  const poolsTable = new KeyedTable(byId("pools-body"), (row) => `pool:${row.id || row.pool_id || row.address || pairFor(row)}`, () => makeTableRow(15), patchPoolRow, 15);
  const ownerPositionsTable = new KeyedTable(byId("owner-positions-body"), (row) => `position:${row.position_key || row.token_id || row.id || `${row.pool_id || ""}:${row.tick_lower || ""}:${row.tick_upper || ""}`}`, () => makeTableRow(10), patchPositionRow, 10);
  const ownerClosedTable = new KeyedTable(byId("owner-closed-body"), (row) => `owner-closed:${row.id || `${row.position_key || row.pool_id || ""}:${row.opened_at || ""}:${row.closed_at || ""}`}`, () => makeTableRow(12), patchOwnerClosedRow, 12);

  function scheduleRender(name, callback) {
    state.renderQueue.set(name, callback);
    if (!state.renderFrame && !state.hidden) state.renderFrame = requestAnimationFrame(flushRenders);
  }

  function flushRenders() {
    state.renderFrame = 0;
    if (state.hidden) return;
    let remaining = 2;
    for (const [name, callback] of state.renderQueue) {
      state.renderQueue.delete(name);
      callback();
      remaining -= 1;
      if (!remaining) break;
    }
    if (state.renderQueue.size) state.renderFrame = requestAnimationFrame(flushRenders);
  }

  function eventMatchesFilters(row) {
    if (state.protocol && String(row.protocol || "").toLowerCase() !== state.protocol) return false;
    if (state.tapeKind === "lp" && !LP_EVENT_KINDS.has(String(row.kind || "").toLowerCase())) return false;
    if (!state.q) return true;
    const token0 = tokenMeta(row, 0);
    const token1 = tokenMeta(row, 1);
    const haystack = [row.pair, row.pool_id, row.owner, row.custody, row.tx_hash, row.position_key, row.token_id, row.kind, row.protocol, token0.symbol, token0.address, token1.symbol, token1.address].filter(Boolean).join(" ").toLowerCase();
    return haystack.includes(state.q);
  }

  function genericMatches(row) {
    const activity = row && row.activity && typeof row.activity === "object" ? row.activity : {};
    const reportedProtocol = String(row.protocol || activity.protocol || "").toLowerCase();
    if (state.protocol && reportedProtocol && reportedProtocol !== state.protocol) return false;
    if (!state.q) return true;
    const haystack = [
      row.pair, row.id, row.pool_id, row.address, row.owner, row.custody, row.position_key,
      row.protocol, row.symbol0, row.symbol1, activity.pair, activity.pool_id, activity.kind,
      activity.tx_hash, tokenMeta(row, 0).symbol, tokenMeta(row, 1).symbol
    ].filter(Boolean).join(" ").toLowerCase();
    return haystack.includes(state.q);
  }

  function renderTape() {
    const rows = capTapeRows().filter(eventMatchesFilters);
    const health = state.health.get("tape");
    const empty = health && health.ok ? "NO CANONICAL FLOW MATCHES" : "FLOW UNAVAILABLE · RETRYING";
    tapeTable.reconcile(rows, empty);
  }


  function ownerSourceRows() {
    return state.ownersEnvelope && Array.isArray(state.ownersEnvelope.rows)
      ? state.ownersEnvelope.rows
      : [];
  }

  function ownerRenderSignature(row, rank) {
    return [
      rank, row.owner, row.custody, row.identity_basis, row.positions, row.open_positions,
      row.closed_episodes, row.fees_usd, row.gross_pnl_usd, row.gas_usd, row.net_pnl_usd,
      row.volume_usd, row.win_rate, activitySignature(row.activity), describeCoverage(row.coverage)
    ].join("|");
  }

  function sortedOwners() {
    const rows = ownerSourceRows().filter((row) => genericMatches(row) && (state.ownerScope === "wallets" ? Boolean(row.owner) : !row.owner && Boolean(row.custody)));
    const field = state.ownerSort;
    rows.sort((left, right) => {
      const identityOrder = String(left.owner || left.custody || "").localeCompare(String(right.owner || right.custody || ""));
      if (field === "activity") {
        const leftActivity = left.activity;
        const rightActivity = right.activity;
        if (!leftActivity || !rightActivity) return leftActivity ? -1 : rightActivity ? 1 : identityOrder;
        return compareOwnerActivity(rightActivity, leftActivity) || identityOrder;
      }
      const a = finite(left[field]);
      const b = finite(right[field]);
      if (a == null && b == null) return identityOrder;
      if (a == null) return 1;
      if (b == null) return -1;
      return b - a || identityOrder;
    });
    const selected = rows.slice(0, TABLE_LIMIT);
    const activeKeys = new Set();
    const output = selected.map((row, index) => {
      const key = ownerIdentityKey(row);
      const signature = ownerRenderSignature(row, index + 1);
      activeKeys.add(key);
      const cached = state.ownerViewCache.get(key);
      if (cached && cached.signature === signature) return cached.row;
      const view = { ...row, __rank: index + 1 };
      state.ownerViewCache.set(key, { signature, row: view });
      return view;
    });
    for (const key of state.ownerViewCache.keys()) {
      if (!activeKeys.has(key)) state.ownerViewCache.delete(key);
    }
    return output;
  }

  function renderOwnerFreshness() {
    const envelope = state.ownersEnvelope || {};
    const current = envelope.current_activity && typeof envelope.current_activity === "object"
      ? envelope.current_activity : {};
    const accountingDate = toDate(envelope.accounting_as_of);
    const accountingAge = accountingDate
      ? formatAge(Math.max(0, (Date.now() - accountingDate.getTime()) / 1000))
      : null;
    const qualification = String(current.qualification || "").replaceAll("_", " ");
    const label = !state.ownersReady
      ? "Loading wallet activity…"
      : `${ownerSourceRows().length} ${state.ownerScope === "wallets" ? "wallets" : "custody rows"} · ${accountingDate ? `accounting ${accountingAge} behind` : "accounting unavailable"} · — = not verified`;
    if (elements.ownersAccountingReadout.textContent !== label) {
      elements.ownersAccountingReadout.textContent = label;
    }
    elements.ownersAccountingReadout.title = [
      accountingDate
        ? `Financial totals are historical accounting as of ${formatStamp(accountingDate)} (${accountingAge} old)`
        : "Historical accounting timestamp unavailable; financial values are not current-stream values",
      "Latest activity is a separate wallet freshness indicator, not part of the financial totals",
      current.head != null ? `Current activity head #${formats.integer.format(current.head)}` : null,
      current.observed_from != null ? `Current activity observed from block #${formats.integer.format(current.observed_from)}` : null,
      qualification ? `Current activity qualification: ${qualification}` : null,
      envelope.coverage ? describeCoverage(envelope.coverage) : null
    ].filter(Boolean).join("\n");
  }

  function renderOwners() {
    renderOwnerFreshness();
    if (!state.ownersReady) {
      ownersTable.reconcile([], ownersPanelActive() ? "Loading indexed wallet activity and positions…" : "Wallet activity paused while hidden");
      return;
    }
    const rows = sortedOwners();
    const health = state.health.get("owners");
    ownersTable.reconcile(rows, health && health.ok ? `No ${state.ownerScope === "wallets" ? "LP wallets" : "custody contracts"} match this window and filter` : "Wallet data unavailable · retrying");
  }

  function renderPools() {
    if (!state.poolsVisible) {
      poolsTable.reconcile([], "POOL SUMMARY LOADS WHEN VISIBLE");
      return;
    }
    if (!state.poolsEnvelope) {
      poolsTable.reconcile([], "POOL SUMMARY SYNCING");
      return;
    }
    const source = Array.isArray(state.poolsEnvelope.rows) ? state.poolsEnvelope.rows : [];
    const field = POOL_SORT_FIELDS[state.poolSort];
    const direction = state.poolOrder === "asc" ? 1 : -1;
    const rows = source.filter(genericMatches).sort((a, b) => {
      const av = finite(a[field]), bv = finite(b[field]);
      if (av == null || bv == null) return av == null ? bv == null ? 0 : 1 : -1;
      return direction * (av - bv);
    }).slice(0, TABLE_LIMIT);
    const health = state.health.get("pools");
    const empty = !health ? "POOL SUMMARY SYNCING" : health.ok ? "NO POOL MATCHES" : "POOL DATA UNAVAILABLE · RETRYING";
    poolsTable.reconcile(rows, empty);
  }

  function renderAllTables() {
    scheduleRender("tape", renderTape);
    scheduleRender("owners", renderOwners);
    scheduleRender("pools", renderPools);
  }


  function renderLiveBlockAge() {
    const block = state.liveBlock;
    const stamp = block && toDate(block.timestamp != null ? block.timestamp : block.as_of);
    const label = stamp ? formatAge(Math.max(0, (Date.now() - stamp.getTime()) / 1000)) : "—";
    if (label === state.liveBlockAgeLabel) return;
    state.liveBlockAgeLabel = label;
    elements.liveBlockAge.textContent = label;
  }


  function invalidateOwners(reason) {
    state.ownersEnvelope = null;
    state.ownersReady = false;
    state.ownerViewCache.clear();
    byId("owners-table").setAttribute("aria-busy", String(ownersPanelActive()));
    elements.ownersAccountingReadout.title = reason || "Waiting for a fresh wallet summary";
    scheduleRender("owners", renderOwners);
  }

  function applyOwnersEnvelope(payload) {
    if (!payload || typeof payload !== "object") return;
    // Projection revisions are independent of feed and durable-ledger cursors.
    state.ownersEnvelope = payload;
    state.ownersReady = true;
    byId("owners-table").setAttribute("aria-busy", "false");
    healthSuccess("owners");
    scheduleRender("owners", renderOwners);
  }

  function currentIndexGap() {
    const reported = finite(state.status && state.status.lag_blocks);
    const liveHead = finite(state.liveBlock && state.liveBlock.number)
      ?? finite(state.status && state.status.head);
    const indexedHead = finite(state.status && state.status.indexed_head);
    const observed = liveHead != null && indexedHead != null ? Math.max(0, liveHead - indexedHead) : null;
    if (reported == null) return observed;
    return observed == null ? Math.max(0, reported) : Math.max(0, reported, observed);
  }

  function renderGlobalGap() {
    const indexGap = currentIndexGap();
    const feedGap = state.liveBlock && state.liveBlock.gap;
    const hasIndexGap = indexGap != null && indexGap > 0;
    const hasFeedGap = feedGap != null && feedGap !== false && feedGap !== 0;
    const feedGapCount = typeof feedGap === "number"
      ? feedGap
      : feedGap && typeof feedGap === "object"
        ? finite(feedGap.count != null ? feedGap.count : feedGap.missing)
        : null;
    elements.liveBlockGap.hidden = !hasIndexGap && !hasFeedGap;
    if (elements.liveBlockGap.hidden) {
      elements.liveBlockGap.title = "";
      return;
    }
    if (hasIndexGap) {
      elements.liveBlockGap.textContent = `INDEX GAP ${formatCount(indexGap)}${hasFeedGap ? " · FEED GAP" : ""}`;
      elements.liveBlockGap.title = [
        `Live chain head is ${formats.integer.format(indexGap)} block${indexGap === 1 ? "" : "s"} ahead of the durable index`,
        hasFeedGap ? `Feed discontinuity: ${typeof feedGap === "object" ? JSON.stringify(feedGap) : String(feedGap)}` : null
      ].filter(Boolean).join("\n");
      return;
    }
    elements.liveBlockGap.textContent = feedGapCount != null ? `FEED GAP ${formatCount(feedGapCount)}` : "FEED GAP";
    elements.liveBlockGap.title = typeof feedGap === "object" ? JSON.stringify(feedGap) : String(feedGap);
  }

  function pruneConfirmedOrphanRows(block) {
    const gap = block && block.gap;
    const gapDetails = gap && typeof gap === "object" ? gap : {};
    const rejected = new Set([
      gapDetails.replaced_hash,
      gapDetails.orphaned_hash
    ].filter((hash) => hash && state.canonicalBlocks.has(String(hash))).map(String));
    const previous = state.liveBlock;
    if (previous && previous.hash && block.hash
        && state.canonicalBlocks.has(String(previous.hash))
        && String(previous.number) === String(block.number)
        && String(previous.hash) !== String(block.hash)) {
      rejected.add(String(previous.hash));
    }
    const parentMismatch = previous && previous.hash && block.parent_hash
      && state.canonicalBlocks.has(String(previous.hash))
      && String(previous.hash) !== String(block.parent_hash)
      && String(previous.hash) !== String(block.hash);
    let canonical = null;
    if (parentMismatch || gapDetails.reason === "parent_hash_mismatch") {
      canonical = new Set([block.hash, block.parent_hash].filter(Boolean).map(String));
      let cursor = block.parent_hash ? String(block.parent_hash) : "";
      for (let depth = 0; cursor && depth < 256; depth += 1) {
        const known = state.canonicalBlocks.get(cursor);
        if (!known || !known.parent_hash) break;
        cursor = String(known.parent_hash);
        canonical.add(cursor);
      }
    }
    let removed = 0;
    for (const collection of [state.tapeRows, state.heldTapeRows]) {
      for (const [key, row] of collection) {
        if (!row || row.__streamQualification !== "provisional_canonical") continue;
        const hash = row.block_hash ? String(row.block_hash) : "";
        const confirmedFork = canonical && hash && state.canonicalBlocks.has(hash) && !canonical.has(hash);
        if (rejected.has(hash) || confirmedFork) {
          collection.delete(key);
          removed += 1;
        }
      }
    }
    const reorgDetected = rejected.size > 0 || canonical !== null;
    for (const hash of rejected) state.canonicalBlocks.delete(hash);
    if (canonical) {
      for (const hash of state.canonicalBlocks.keys()) {
        if (!canonical.has(hash)) state.canonicalBlocks.delete(hash);
      }
    }
    if (removed) {
      window.__lpTerminalPerf.orphanRowsDropped += removed;
      scheduleRender("tape", renderTape);
      updateHeldTapeCount();
    }
    if (reorgDetected) invalidateOwners("Canonical chain changed; waiting for a fresh wallet summary");
  }

  function applyBlockFrame(block, named = false) {
    if (!block || typeof block !== "object" || block.number == null) return;
    const incomingNumber = finite(block.number);
    const previousNumber = finite(state.liveBlock && state.liveBlock.number);
    const incomingSequence = finite(block.sequence);
    const incomingEpoch = String(block.feed_epoch || "");
    const sameEpoch = Boolean(state.liveBlock)
      && (!incomingEpoch || !state.liveBlockFeedEpoch || state.liveBlockFeedEpoch === incomingEpoch);
    if (sameEpoch && incomingNumber != null && previousNumber != null && incomingNumber < previousNumber) return;
    if (sameEpoch && incomingSequence != null && state.liveBlockSequence != null && incomingSequence <= state.liveBlockSequence) return;
    const signature = [
      block.number,
      block.hash,
      block.parent_hash,
      block.timestamp,
      block.as_of,
      block.source,
      block.feed_epoch,
      JSON.stringify(block.gap == null ? null : block.gap)
    ].join("|");
    const hadNamedBlock = state.namedBlockSeen;
    if (named && hadNamedBlock && sameEpoch) pruneConfirmedOrphanRows(block);
    if (named) {
      if (incomingEpoch && state.liveBlockFeedEpoch && state.liveBlockFeedEpoch !== incomingEpoch) {
        state.canonicalBlocks.clear();
        invalidateOwners("Live feed epoch changed; waiting for a fresh wallet summary");
      }
      if (block.hash) {
        const hash = String(block.hash);
        state.canonicalBlocks.delete(hash);
        state.canonicalBlocks.set(hash, { ...block });
        while (state.canonicalBlocks.size > 256) {
          state.canonicalBlocks.delete(state.canonicalBlocks.keys().next().value);
        }
      }
      state.namedBlockSeen = true;
      if (!sameEpoch) state.liveBlockSequence = incomingSequence;
    }
    state.liveBlock = { ...block };
    if (incomingSequence != null) state.liveBlockSequence = incomingSequence;
    if (incomingEpoch) state.liveBlockFeedEpoch = incomingEpoch;
    if (signature === state.liveBlockSignature) {
      renderLiveBlockAge();
      return;
    }
    state.liveBlockSignature = signature;
    state.liveBlockAgeLabel = "";
    window.__lpTerminalPerf.blockFrames += 1;
    elements.liveBlockLink.textContent = `#${formats.integer.format(block.number)}`;
    elements.liveBlockLink.href = `${ROBINSCAN}/block/${encodeURIComponent(String(block.number))}`;
    elements.liveBlockLink.title = [
      block.hash ? `hash ${block.hash}` : null,
      block.parent_hash ? `parent ${block.parent_hash}` : null,
      block.source ? `source ${block.source}` : null
    ].filter(Boolean).join(" · ");
    renderGlobalGap();
    renderStatus();
    renderLiveBlockAge();
  }



  function reportedErrorDetails(errors) {
    if (!errors) return [];
    if (typeof errors === "string") return errors ? [errors] : [];
    if (Array.isArray(errors)) return errors.map(String);
    if (typeof errors === "object") return Object.entries(errors).map(([key, value]) => `${key}: ${value}`);
    return [String(errors)];
  }


  function renderIndexDetails(details) {
    if (elements.indexStatus.hidden) return;
    const status = state.status;
    if (!status) {
      elements.indexStatusDetails.textContent = "Waiting for index status. HTTP availability alone does not establish index freshness.";
      return;
    }
    const lines = [
      details, "",
      "Time lag is the age of indexed blocks, not a catch-up ETA.",
      "A current live index does not imply complete historical accounting.",
      "",
      `Historical backfill: ${status.backfill === true ? "in progress" : status.backfill === false ? "complete" : "unknown"}`
    ];
    for (const [field, label] of [
      ["pending_enrichment", "Receipt enrichment transactions"],
      ["pending_reprojection", "Accounting reprojection events"],
      ["pending_balances", "Position balance refreshes"]
    ]) {
      const count = finite(status[field]);
      lines.push(`${label}: ${count == null ? "unknown" : formats.integer.format(count)}`);
    }
    for (const [field, label] of [["live_scan", "LIVE"], ["history_scan", "HISTORY"]]) {
      const scan = status[field];
      lines.push("", `LAST ${label} BATCH`);
      if (!scan || typeof scan !== "object") {
        lines.push("No completed batch reported.");
        continue;
      }
      const observed = finite(scan.observed_at);
      lines.push(`Sample age: ${observed == null ? "unknown" : formatAge(Math.max(0, Date.now() / 1000 - observed))}`);
      const blocks = finite(scan.blocks);
      if (blocks != null) lines.push(`Blocks: ${formats.integer.format(blocks)}`);
      for (const [key, name] of [
        ["seconds", "Total"], ["fetch_seconds", "RPC fetch"],
        ["decode_seconds", "Decode"], ["store_lock_wait_seconds", "Writer wait"],
        ["store_seconds", "Store"], ["postprocess_seconds", "Post-processing"],
        ["publish_seconds", "Publish"]
      ]) {
        const seconds = finite(scan[key]);
        if (seconds != null) lines.push(`${name}: ${seconds.toFixed(3)} s`);
      }
    }
    lines.push("", "Batch timings are individual samples, not sustained throughput.");
    elements.indexStatusDetails.textContent = lines.join("\n");
  }

  function closeIndexStatus() {
    elements.indexStatus.hidden = true;
    elements.status.focus({ preventScroll: true });
  }

  function renderStatus() {
    const status = state.status;
    if (!status) {
      elements.status.textContent = "CONNECTING";
      elements.footer.textContent = "INDEX —";
      renderIndexDetails("");
      renderGlobalGap();
      return;
    }
    const liveHead = finite(state.liveBlock && state.liveBlock.number) ?? finite(status.head);
    const indexedHead = finite(status.indexed_head);
    const gap = currentIndexGap();
    const lag = finite(status.lag_s);
    const parts = [
      liveHead != null ? `HEAD #${formats.integer.format(liveHead)}` : "HEAD —",
      indexedHead != null ? `INDEX #${formats.integer.format(indexedHead)}` : "INDEXING"
    ];
    if (gap != null && gap > 0) {
      parts.push(`CATCH-UP ${formats.integer.format(gap)} BLOCK${gap === 1 ? "" : "S"}`);
    } else if (lag != null && lag > 2) {
      parts.push(`CATCH-UP ${formatAge(lag)}`);
    } else if (gap === 0) {
      parts.push("INDEX CURRENT");
    }
    elements.status.textContent = parts.join(" · ");
    elements.footer.textContent = gap != null && gap > 0
      ? `INDEX ${formats.integer.format(gap)} BLOCK${gap === 1 ? "" : "S"} BEHIND${lag != null && lag > 0 ? ` · ${formatAge(lag)}` : ""}`
      : lag != null && lag > 2 ? `INDEX ${formatAge(lag)} BEHIND` : "INDEX CURRENT";
    const details = [
      liveHead != null ? `Live head #${formats.integer.format(liveHead)}` : null,
      indexedHead != null ? `Durable indexed head #${formats.integer.format(indexedHead)}` : null,
      gap != null ? `Live index gap ${formats.integer.format(gap)} block${gap === 1 ? "" : "s"}` : null,
      lag != null ? `Indexed block time lag ${formatAge(lag)}` : null,
      ...reportedErrorDetails(status.errors)
    ].filter(Boolean).join("\n");
    elements.status.title = `Open index status details\n${details}`;
    elements.footer.title = details;
    renderIndexDetails(details);
    renderGlobalGap();
  }

  function renderOverview() {
    const overview = state.overview;
    if (!overview) {
      elements.overview.textContent = `${state.window} · metrics —`;
      elements.overview.title = "";
      return;
    }
    elements.overview.textContent = [
      String(overview.window || state.window).toUpperCase(),
      `VOL ${formatUsd(overview.volume_usd)}`,
      `FEE ${formatUsd(overview.fees_usd)}`,
      `FLOW ${formatSignedUsd(overview.net_deposits_usd)}`,
      `POOL ${formatCount(overview.active_pools)}`,
      `LP ${formatCount(overview.active_owners)}`
    ].join(" · ");
    elements.overview.title = [
      `Swaps ${formatCount(overview.swaps)}`,
      `Adds ${formatCount(overview.adds)}`,
      `Removes ${formatCount(overview.removes)}`,
      `Collects ${formatCount(overview.collects)}`,
      describeCoverage(overview.coverage)
    ].join(" · ");
  }

  function healthSuccess(name) {
    state.health.set(name, { ok: true, at: Date.now(), error: "" });
    renderHealth();
  }

  function healthFailure(name, error) {
    const previous = state.health.get(name);
    state.health.set(name, { ok: false, at: previous && previous.at || 0, failedAt: Date.now(), error: errorMessage(error) });
    renderHealth();
  }

  function renderHealth() {
    const now = Date.now();
    const fragments = [];
    const names = ["status", "overview", "tape", "owners", "pools"];
    for (const name of names) {
      const health = state.health.get(name);
      if (!health) continue;
      const age = health.at ? Math.max(0, (now - health.at) / 1000) : null;
      const ageLabel = age == null ? "" : age < 60 ? "<1m" : formatAge(age);
      if (!health.ok) fragments.push({ className: "bad", text: `${name} ERROR${ageLabel ? ` ${ageLabel} OLD` : ""}: ${health.error}` });
      else if (age > REFRESH_MS * 2.5 / 1000) fragments.push({ className: "stale", text: `${name} STALE ${ageLabel}` });
    }
    const signature = fragments.map((fragment) => `${fragment.className}:${fragment.text}`).join("|");
    if (signature === state.healthSignature) return;
    state.healthSignature = signature;
    state.healthDetails = fragments;
    elements.endpointHealth.textContent = fragments.map((fragment) => fragment.text).join(" · ");
    renderStatus();
  }

  function setStreamState(label, className = "") {
    elements.streamState.textContent = label;
    elements.streamState.className = `strip-state${className ? ` ${className}` : ""}`;
  }

  function advanceRevision(value) {
    if (value == null || value === "") return;
    if (state.revision == null) {
      state.revision = value;
      return;
    }
    try {
      if (BigInt(value) > BigInt(state.revision)) state.revision = value;
    } catch (_) {
      state.revision = value;
    }
  }

  function applyStatus(status) {
    if (!status || typeof status !== "object") return;
    const observedAt = finite(status.as_of);
    const currentAt = finite(state.status && state.status.as_of);
    if (observedAt != null && currentAt != null && observedAt < currentAt) return;
    state.status = { ...(state.status || {}), ...status };
    if ((!state.liveBlock || state.liveBlock.source === "status") && status.head != null) {
      applyBlockFrame({
        number: status.head,
        timestamp: status.head_timestamp,
        as_of: status.as_of,
        source: "status"
      });
    }
    healthSuccess("status");
    renderStatus();
  }

  function updateHeldTapeCount() {
    const count = state.heldTapeRows.size;
    elements.tapeArrivals.classList.toggle("is-empty", count === 0);
    elements.tapeArrivals.disabled = count === 0;
    elements.tapeArrivals.setAttribute("aria-hidden", String(count === 0));
    elements.tapeArrivals.textContent = `+${count} NEW`;
    elements.tapeArrivals.title = count ? `${count} live event${count === 1 ? "" : "s"} held while you read` : "";
  }

  function tapeIsBeingRead() {
    return !state.follow || elements.tapeScroll.scrollTop > 4;
  }

  function queueTapeRows(rows) {
    for (const item of rows) {
      if (!item || typeof item !== "object" || !eventMatchesFilters(item)) continue;
      const key = eventKey(item);
      state.heldTapeRows.delete(key);
      state.heldTapeRows.set(key, item);
    }
    while (state.heldTapeRows.size > MAX_TAPE_ROWS) {
      state.heldTapeRows.delete(state.heldTapeRows.keys().next().value);
    }
    updateHeldTapeCount();
  }

  function releaseHeldTapeRows() {
    if (!state.heldTapeRows.size) return;
    const rows = Array.from(state.heldTapeRows.values());
    state.heldTapeRows.clear();
    updateHeldTapeCount();
    applyStreamRows(rows, false, true);
    requestAnimationFrame(() => { elements.tapeScroll.scrollTop = 0; });
  }

  function updateWatermark(row) {
    if (!state.tapeWatermark || compareCanonicalAscending(row, state.tapeWatermark) > 0) state.tapeWatermark = row;
  }

  function tapeRenderSignature(row, fallbackCoverage = null) {
    const token0 = tokenMeta(row, 0);
    const token1 = tokenMeta(row, 1);
    return [
      eventKey(row), row.timestamp, row.block_number, row.tx_index, row.log_index, row.tx_hash, row.kind, row.pair, row.protocol,
      row.owner, row.custody, row.identity_basis, row.position_key, row.token_id, row.tick_lower, row.tick_upper,
      row.cashflow0, row.cashflow1, row.amount0, row.amount1, row.deposit_usd, row.withdrawal_usd,
      row.transaction_flow0, row.transaction_flow1, row.cashflow_usd, row.usdg_flow_usd,
      row.fees_usd, row.size_usd, row.volume_usd, row.pricing_basis, row.accounting_basis,
      row.pool_fee_ppm, row.pool_fee_basis, row.fees_scope, row.fees_basis, row.fees_qualification,
      row.flow_complete, row.flow_scope, row.flow_qualification, row.qualification, row.__streamQualification,
      token0.address, token0.symbol, token0.decimals, token1.address, token1.symbol, token1.decimals,
      describeCoverage(row.coverage != null ? row.coverage : fallbackCoverage),
      Array.isArray(row.transaction_transfers)
        ? row.transaction_transfers.map((entry) => `${entry && entry.token || ""}:${entry && entry.amount != null ? entry.amount : ""}:${entry && entry.from || ""}:${entry && entry.to || ""}:${entry && entry.log_index != null ? entry.log_index : ""}`).join(",")
        : ""
    ].join("|");
  }

  function isLiveArrival(row, allowArrival) {
    if (!allowArrival || state.hidden) return false;
    if (state.tapeWatermark) return compareCanonicalAscending(row, state.tapeWatermark) > 0;
    const timestamp = toDate(row.timestamp);
    return timestamp != null && timestamp.getTime() / 1000 >= state.arrivalFloor;
  }

  function rememberTapeKey(key) {
    state.seenTapeKeys.delete(key);
    state.seenTapeKeys.set(key, true);
    while (state.seenTapeKeys.size > MAX_TAPE_ROWS * 4) {
      state.seenTapeKeys.delete(state.seenTapeKeys.keys().next().value);
    }
  }
  function capTapeRows() {
    const keep = Array.from(state.tapeRows.values()).sort(compareCanonicalDescending).slice(0, MAX_TAPE_ROWS);
    if (state.tapeRows.size > MAX_TAPE_ROWS) {
      const keys = new Set(keep.map(eventKey));
      for (const key of state.tapeRows.keys()) if (!keys.has(key)) state.tapeRows.delete(key);
    }
    return keep;
  }

  function applyTapeSnapshot(payload) {
    const liveRows = Array.from(state.tapeRows.entries()).filter(([, row]) => row && row.__streamQualification === "provisional_canonical");
    const rows = payload && Array.isArray(payload.rows) ? payload.rows.slice().sort(compareCanonicalDescending).slice(0, MAX_TAPE_ROWS) : [];
    const replacement = new Map();
    for (const row of rows) {
      const key = eventKey(row);
      const existing = state.tapeRows.get(key);
      const unchanged = existing
        && tapeRenderSignature(existing, state.tapeEnvelope && state.tapeEnvelope.coverage) === tapeRenderSignature(row, payload && payload.coverage);
      replacement.set(key, unchanged ? existing : row);
      rememberTapeKey(key);
    }
    for (const [key, row] of liveRows) {
      if (!replacement.has(key)) replacement.set(key, row);
    }
    state.tapeRows = replacement;
    const combined = capTapeRows();
    state.tapeEnvelope = payload || { rows: [] };
    state.tapeWatermark = combined.length ? combined[0] : null;
    const reportedAsOf = toDate(payload && (payload.as_of || payload.history_to));
    const statusLag = finite(state.status && state.status.lag_s);
    state.arrivalFloor = reportedAsOf ? reportedAsOf.getTime() / 1000 : Date.now() / 1000 - Math.max(60, (statusLag || 0) + 30);
    for (const key of replacement.keys()) state.heldTapeRows.delete(key);
    updateHeldTapeCount();
    if (payload && payload.revision != null) state.revision = payload.revision;
    if (payload && payload.epoch != null) state.epoch = payload.epoch;
    if (state.stream && state.stream.readyState === EventSource.OPEN) setStreamState("STREAM LIVE");
    healthSuccess("tape");
    scheduleRender("tape", renderTape);
  }

  function applyStreamRows(rows, allowArrival, forceRender = false) {
    let changed = forceRender;
    let hasArrival = false;
    for (const item of rows) {
      if (!item || typeof item !== "object") continue;
      const key = eventKey(item);
      const existing = state.tapeRows.get(key);
      const seen = state.seenTapeKeys.has(key);
      if (existing
          && item.__streamQualification === "provisional_canonical"
          && existing.__streamQualification !== "provisional_canonical") {
        rememberTapeKey(key);
        continue;
      }
      const matches = eventMatchesFilters(item);
      const arrival = !existing && !seen && matches && isLiveArrival(item, allowArrival);
      rememberTapeKey(key);
      updateWatermark(item);
      if (!matches && !existing) continue;
      if (existing && tapeRenderSignature(existing) === tapeRenderSignature(item)) continue;
      state.tapeRows.set(key, item);
      changed = true;
      if (arrival) hasArrival = true;
    }
    // Batch normal arrivals into the next paint; bound unusually large replay bursts.
    if (state.tapeRows.size > MAX_TAPE_ROWS * 2) capTapeRows();
    if (changed) scheduleRender("tape", renderTape);
    if (state.follow && hasArrival) requestAnimationFrame(() => { elements.tapeScroll.scrollTop = 0; });
  }


  function applyStreamFrame(payload) {
    if (!payload || typeof payload !== "object") return;
    if (payload.reset) {
      advanceRevision(payload.revision);
      if (payload.epoch != null) state.epoch = payload.epoch;
      if (payload.status) applyStatus(payload.status);
      setStreamState("RESETTING", "is-stale");
      state.pendingStreamFrames.length = 0;
      invalidateOwners("Feed reset; waiting for a fresh wallet summary");
      if (state.tapeLoading) state.pendingStreamReset = true;
      else reloadTape("stream reset");
      return;
    }
    if (state.tapeLoading && payload.type !== "activity") {
      if (state.pendingStreamFrames.length >= 32) {
        state.pendingStreamFrames.shift();
        state.pendingStreamReset = true;
      }
      state.pendingStreamFrames.push(payload);
      return;
    }
    advanceRevision(payload.revision);
    if (payload.epoch != null) state.epoch = payload.epoch;
    if (payload.status) applyStatus(payload.status);
    if (payload.snapshot) {
      applyTapeSnapshot(payload);
      return;
    }
    let removedAny = false;
    for (const id of payload.removed || []) {
      for (const collection of [state.tapeRows, state.heldTapeRows]) {
        if (collection.delete(`event:${id}`)) removedAny = true;
        for (const [key, row] of collection) {
          if (row && String(row.id) === String(id)) {
            collection.delete(key);
            removedAny = true;
          }
        }
      }
    }
    if (Array.isArray(payload.removed) && payload.removed.length) {
      invalidateOwners("Canonical activity was withdrawn; waiting for a fresh wallet summary");
    }
    updateHeldTapeCount();
    const liveCurrent = payload.type === "activity"
      && payload.lane === "current"
      && payload.qualification === "provisional_canonical";
    if (liveCurrent) window.__lpTerminalPerf.activityFrames += 1;
    if (liveCurrent && payload.block && !state.namedBlockSeen && !state.liveBlock) {
      applyBlockFrame({ ...payload.block, source: payload.block.source || "activity" });
    }
    const incoming = Array.isArray(payload.rows) ? payload.rows : [];
    const rows = liveCurrent
      ? incoming.filter((row) => row && typeof row === "object").map((row) => ({
        ...row,
        timestamp: payload.block && payload.block.timestamp != null ? payload.block.timestamp : row.timestamp,
        block_number: row.block_number != null ? row.block_number : payload.block && payload.block.number,
        block_hash: row.block_hash || payload.block && payload.block.hash,
        __streamLane: payload.lane,
        __streamQualification: payload.qualification
      }))
      : incoming;
    const allowArrival = liveCurrent || (payload.type === "tape" && payload.backfill !== true && payload.enrichment !== true);
    if (liveCurrent && tapeIsBeingRead()) {
      const updates = rows.filter((row) => state.tapeRows.has(eventKey(row)));
      const additions = rows.filter((row) => !state.tapeRows.has(eventKey(row)));
      applyStreamRows(updates, false, removedAny);
      queueTapeRows(additions);
    } else {
      applyStreamRows(rows, allowArrival, removedAny);
    }
    healthSuccess("tape");
  }

  async function api(path, params, signal) {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(params || {})) {
      if (value != null && value !== "") query.set(key, String(value));
    }
    const response = await fetch(`${API_ROOT}${path}${query.size ? `?${query.toString()}` : ""}`, {
      method: "GET",
      headers: { Accept: "application/json" },
      cache: "no-store",
      signal
    });
    if (!response.ok) {
      let detail = "";
      try {
        const body = await response.json();
        detail = body && (body.error || body.message) ? `: ${body.error || body.message}` : "";
      } catch (_) {
        detail = "";
      }
      throw new Error(`${path} ${response.status}${detail}`);
    }
    return response.json();
  }

  function commonParams(limit = TABLE_LIMIT) {
    return { window: state.window, q: state.q, protocol: state.protocol, limit, offset: 0 };
  }

  async function loadResource(name, path, params, controller, apply) {
    try {
      const payload = await api(path, params, controller.signal);
      if (controller.signal.aborted) return;
      apply(payload || {});
      healthSuccess(name);
    } catch (error) {
      if (error.name === "AbortError" || controller.signal.aborted) return;
      healthFailure(name, error);
    }
  }

  function ownersPanelActive() {
    return !state.hidden && state.ownersVisible && elements.modal.hidden && elements.poolInspector.hidden;
  }

  function poolPanelActive() {
    return !state.hidden && state.poolsVisible && elements.modal.hidden && elements.poolInspector.hidden;
  }

  function syncOwnerProjection(reason) {
    const enabled = ownersPanelActive();
    if (enabled === state.streamOwnersEnabled) return;
    state.streamOwnersEnabled = enabled;
    if (enabled) invalidateOwners(reason || "Wallet summary became visible");
    else byId("owners-table").setAttribute("aria-busy", "false");
    if (state.stream) openStream(true);
  }

  function startSummaryObservers() {
    const ownersSection = byId("owners-section");
    const poolsSection = byId("pools-section");
    if (!("IntersectionObserver" in window)) {
      state.poolsVisible = true;
      return;
    }
    const viewport = elements.terminalMain.getBoundingClientRect();
    const initiallyVisible = (section) => {
      const bounds = section.getBoundingClientRect();
      return bounds.width > 0 && bounds.bottom > viewport.top && bounds.top < viewport.bottom;
    };
    state.ownersVisible = initiallyVisible(ownersSection);
    state.poolsVisible = initiallyVisible(poolsSection);
    state.summaryObserver = new IntersectionObserver((entries) => {
      let ownerVisibilityChanged = false;
      let poolBecameVisible = false;
      for (const entry of entries) {
        if (entry.target === ownersSection) {
          const visible = entry.isIntersecting;
          if (visible !== state.ownersVisible) {
            state.ownersVisible = visible;
            ownerVisibilityChanged = true;
          }
        } else if (entry.target === poolsSection) {
          const visible = entry.isIntersecting;
          poolBecameVisible = visible && !state.poolsVisible;
          state.poolsVisible = visible;
        }
      }
      if (ownerVisibilityChanged) syncOwnerProjection("Wallet summary visibility changed");
      if (poolBecameVisible) refreshPools();
      if (!poolPanelActive()) abortPoolRequests();
    }, { root: elements.terminalMain, threshold: 0.01 });
    state.summaryObserver.observe(ownersSection);
    state.summaryObserver.observe(poolsSection);
  }

  function poolViewKey(sort = state.poolSort, order = state.poolOrder) {
    return JSON.stringify([state.window, state.q, state.protocol, sort, order]);
  }

  function abortPoolRequests() {
    for (const request of state.poolRequests.values()) request.controller.abort();
    state.poolRequests.clear();
  }

  function activatePoolView() {
    const cached = state.poolViews.get(poolViewKey());
    state.poolsEnvelope = cached ? cached.payload : null;
    scheduleRender("pools", renderPools);
  }

  function fetchPoolView(sort, order) {
    const key = poolViewKey(sort, order);
    const cached = state.poolViews.get(key);
    if (cached && Date.now() - cached.at < REFRESH_MS) return Promise.resolve(cached.payload);
    const pending = state.poolRequests.get(key);
    if (pending) return pending.promise;
    const controller = new AbortController();
    const params = { ...commonParams(), sort, order };
    const promise = (async () => {
      try {
        const payload = await api("/pools", params, controller.signal);
        if (controller.signal.aborted) return;
        state.poolViews.set(key, { payload, at: Date.now() });
        if (key === poolViewKey()) {
          state.poolsEnvelope = payload;
          scheduleRender("pools", renderPools);
          healthSuccess("pools");
        }
        return payload;
      } catch (error) {
        if (error.name !== "AbortError" && !controller.signal.aborted && key === poolViewKey()) healthFailure("pools", error);
      } finally {
        if (state.poolRequests.get(key)?.controller === controller) state.poolRequests.delete(key);
        if (key === poolViewKey() && !state.poolRequests.has(key)) byId("pools-table").setAttribute("aria-busy", "false");
      }
    })();
    state.poolRequests.set(key, { controller, promise });
    return promise;
  }

  async function refreshPools() {
    if (!poolPanelActive()) return;
    const scope = JSON.stringify([state.window, state.q, state.protocol]);
    if (scope !== state.poolScope) {
      abortPoolRequests();
      state.poolViews.clear();
      state.poolScope = scope;
    }
    activatePoolView();
    const key = poolViewKey();
    byId("pools-table").setAttribute("aria-busy", "true");
    await fetchPoolView(state.poolSort, state.poolOrder);
    if (key === poolViewKey() && !state.poolRequests.has(key)) {
      byId("pools-table").setAttribute("aria-busy", "false");
    }
  }

  function scheduleAggregateRefresh(delay = REFRESH_MS) {
    clearTimeout(state.refreshTimer);
    if (state.hidden) return;
    state.refreshTimer = setTimeout(() => refreshAggregates("timer"), delay);
  }

  async function refreshAggregates(reason) {
    if (state.hidden) return;
    if (state.aggregateController) {
      if (reason === "timer") return;
      state.aggregateController.abort();
    }
    const generation = ++state.aggregateGeneration;
    const controller = new AbortController();
    state.aggregateController = controller;
    const requests = [];
    if (elements.modal.hidden && elements.poolInspector.hidden) {
      requests.push(loadResource("overview", "/overview", { window: state.window }, controller, (payload) => {
        state.overview = payload;
        if (payload.status && typeof payload.status === "object") {
          applyStatus(payload.status);
          healthSuccess("status");
        }
        scheduleRender("overview", renderOverview);
      }));
    } else {
      requests.push(loadResource("status", "/status", {}, controller, applyStatus));
    }
    if (poolPanelActive()) refreshPools();
    await Promise.all(requests);
    if (reason === "timer" && !elements.modal.hidden && state.ownerFollow && state.ownerAddress) {
      loadOwner(state.ownerAddress);
    }
    if (generation === state.aggregateGeneration && state.aggregateController === controller) {
      state.aggregateController = null;
      scheduleAggregateRefresh();
    }
  }

  function scheduleHistoryRefresh() {
    clearTimeout(state.historyTimer);
    state.historyTimer = null;
    if (state.hidden) return;
    state.historyTimer = setTimeout(() => reloadTape("history timer"), HISTORY_REFRESH_MS);
  }

  async function reloadTape(reason) {
    if (state.hidden) return;
    if (reason === "history timer" && (state.tapeLoading || !elements.modal.hidden || !elements.poolInspector.hidden)) {
      scheduleHistoryRefresh();
      return;
    }
    clearTimeout(state.historyTimer);
    state.historyTimer = null;
    const restartStream = !["stream reset", "queued reset", "history timer"].includes(reason);
    if (restartStream) {
      if (reason !== "reconnect") invalidateOwners(`Filters changed (${reason}); waiting for a fresh wallet summary`);
      setStreamState("SYNCING", "is-stale");
    }
    const generation = ++state.tapeGeneration;
    if (state.tapeController) state.tapeController.abort();
    const controller = new AbortController();
    state.tapeController = controller;
    state.tapeLoading = true;
    byId("tape-table").setAttribute("aria-busy", "true");
    if (restartStream) openStream(reason === "reconnect");
    try {
      const payload = await api("/tape", { ...commonParams(MAX_TAPE_ROWS), kind: state.tapeKind }, controller.signal);
      if (generation !== state.tapeGeneration || controller.signal.aborted) return;
      applyTapeSnapshot(payload || {});
    } catch (error) {
      if (error.name !== "AbortError" && generation === state.tapeGeneration) healthFailure("tape", error);
    } finally {
      if (generation !== state.tapeGeneration) return;
      state.tapeLoading = false;
      state.tapeController = null;
      byId("tape-table").setAttribute("aria-busy", "false");
      if (state.pendingStreamReset) {
        state.pendingStreamReset = false;
        state.pendingStreamFrames.length = 0;
        reloadTape("queued reset");
        return;
      }
      const frames = state.pendingStreamFrames.splice(0);
      for (const frame of frames) applyStreamFrame(frame);
      scheduleHistoryRefresh();
    }
  }

  function closeStream() {
    state.streamGeneration += 1;
    clearTimeout(state.streamRetryTimer);
    state.streamRetryTimer = null;
    if (state.stream) state.stream.close();
    state.stream = null;
  }

  function openStream(resume = false) {
    if (state.hidden) return;
    closeStream();
    if (!("EventSource" in window)) {
      setStreamState("SSE UNSUPPORTED", "is-error");
      return;
    }
    const generation = state.streamGeneration;
    const ownersEnabled = ownersPanelActive();
    state.streamOwnersEnabled = ownersEnabled;
    const params = new URLSearchParams();
    params.set("current_only", "1");
    params.set("channel", "both");
    params.set("view", "terminal");
    params.set("window", state.window);
    params.set("kind", state.tapeKind);
    if (state.q) params.set("q", state.q);
    if (state.protocol) params.set("protocol", state.protocol);
    params.set("owner_sort", OWNER_SORT_API[state.ownerSort] || "activity");
    params.set("owner_limit", String(OWNER_STREAM_LIMIT));
    params.set("identity_scope", state.ownerScope);
    if (!ownersEnabled) params.set("owners", "0");
    if (resume && state.liveBlockFeedEpoch && state.liveBlockSequence != null) {
      params.set("feed_epoch", state.liveBlockFeedEpoch);
      params.set("block_after", String(state.liveBlockSequence));
    }
    const source = new EventSource(`${API_ROOT}/stream?${params.toString()}`);
    state.stream = source;
    setStreamState("SYNCING", "is-stale");

    const parseEvent = (event, healthName = "tape") => {
      if (generation !== state.streamGeneration || source !== state.stream) return null;
      try {
        return JSON.parse(event.data);
      } catch (_) {
        healthFailure(healthName, new Error("stream sent invalid JSON"));
        return null;
      }
    };
    const receiveActivity = (event) => {
      const payload = parseEvent(event);
      if (payload) applyStreamFrame(payload);
    };
    const receiveBlock = (event) => {
      const payload = parseEvent(event);
      if (!payload) return;
      if (payload.reset) applyStreamFrame(payload);
      else applyBlockFrame(payload, true);
      healthSuccess("tape");
    };
    const receiveOwners = (event) => {
      const payload = parseEvent(event, "owners");
      if (payload) applyOwnersEnvelope(payload);
    };
    source.addEventListener("activity", receiveActivity);
    source.addEventListener("block", receiveBlock);
    source.addEventListener("owners", receiveOwners);
    source.onopen = () => {
      if (generation !== state.streamGeneration || source !== state.stream) return;
      clearTimeout(state.streamRetryTimer);
      state.streamRetryTimer = null;
      state.streamRetryMs = 1_000;
      setStreamState("STREAM LIVE");
    };
    source.onerror = () => {
      if (generation !== state.streamGeneration || source !== state.stream) return;
      setStreamState("RECONNECTING", "is-stale");
      healthFailure("tape", new Error("live stream disconnected"));
      if (ownersEnabled) healthFailure("owners", new Error("wallet stream disconnected"));
      if (source.readyState !== EventSource.CLOSED) return;
      closeStream();
      const delay = state.streamRetryMs;
      state.streamRetryMs = Math.min(15_000, Math.round(state.streamRetryMs * 1.8));
      state.streamRetryTimer = setTimeout(() => {
        if (!state.hidden) reloadTape("reconnect");
      }, delay);
    };
  }

  function setWindow(next) {
    if (!Object.values(WINDOW_KEYS).includes(next) || state.window === next) return;
    state.window = next;
    document.querySelectorAll("[data-window]").forEach((button) => button.classList.toggle("is-active", button.dataset.window === next));
    byId("mobile-window").value = next;
    byId("mobile-more-window").textContent = next;
    if (!elements.modal.hidden) {
      const url = new URL(window.location.href);
      url.searchParams.set("window", state.window);
      window.history.replaceState(null, "", url);
    }
    renderOverview();
    refreshNow("window");
    if (state.ownerAddress && !elements.modal.hidden) loadOwner(state.ownerAddress);
  }

  function setFollow(next) {
    state.follow = Boolean(next);
    elements.followControl.classList.toggle("is-active", state.follow);
    elements.followState.textContent = state.follow ? "on" : "off";
    elements.followControl.setAttribute("aria-pressed", String(state.follow));
    if (state.follow) {
      releaseHeldTapeRows();
      elements.tapeScroll.scrollTop = 0;
    }
  }

  function updatePoolSortControls() {
    document.querySelectorAll("[data-pool-sort]").forEach((button) => {
      const active = button.dataset.poolSort === state.poolSort;
      button.classList.toggle("is-active", active);
      button.dataset.direction = active ? state.poolOrder : "";
      button.setAttribute("aria-pressed", String(active));
      button.closest("th").setAttribute("aria-sort", active ? (state.poolOrder === "asc" ? "ascending" : "descending") : "none");
      button.title = active ? `Sorted ${state.poolOrder}; activate to reverse` : `Sort by ${button.textContent.trim()}`;
    });
    byId("mobile-pool-sort").value = state.poolSort;
    byId("mobile-pool-order").textContent = state.poolOrder === "desc" ? "↓" : "↑";
    byId("mobile-pool-order").setAttribute("aria-label", `Reverse pool sort order; currently ${state.poolOrder === "desc" ? "descending" : "ascending"}`);
  }

  function setPoolSort(key, toggle = true) {
    if (!Object.hasOwn(POOL_SORT_FIELDS, key)) return;
    state.poolOrder = toggle && state.poolSort === key && state.poolOrder === "desc" ? "asc" : "desc";
    state.poolSort = key;
    byId("pools-table").parentElement.scrollTop = 0;
    activatePoolView();
    updatePoolSortControls();
    refreshPools();
  }

  function setTab(next, focus = false, load = true) {
    if (!Object.prototype.hasOwnProperty.call(POOL_SORT, next)) return;
    const changed = state.tab !== next;
    state.tab = next;
    if (changed) [state.poolSort, state.poolOrder] = POOL_SORT[next];
    const tabs = Array.from(elements.tabs.querySelectorAll("[role=tab]"));
    tabs.forEach((tab) => {
      const selected = tab.dataset.tab === next;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
      if (selected) {
        elements.poolPanel.setAttribute("aria-labelledby", tab.id);
        if (focus) tab.focus();
      }
    });
    updatePoolSortControls();
    if (changed) {
      byId("pools-table").parentElement.scrollTop = 0;
      activatePoolView();
      if (load) refreshPools();
    }
  }

  function refreshNow(reason) {
    if (state.hidden) return;
    clearTimeout(state.refreshTimer);
    reloadTape(reason);
    refreshAggregates(reason);
  }

  function abortForFilter() {
    clearTimeout(state.refreshTimer);
    if (state.aggregateController) {
      state.aggregateGeneration += 1;
      state.aggregateController.abort();
      state.aggregateController = null;
    }
    abortPoolRequests();
    if (state.tapeController) {
      state.tapeGeneration += 1;
      state.tapeController.abort();
      state.tapeController = null;
      state.tapeLoading = false;
      byId("tape-table").setAttribute("aria-busy", "false");
    }
    state.pendingStreamFrames.length = 0;
    state.pendingStreamReset = false;
  }

  function queueFilterRefresh() {
    clearTimeout(state.filterTimer);
    abortForFilter();
    renderAllTables();
    state.filterTimer = setTimeout(() => refreshNow("filter"), FILTER_DELAY_MS);
  }
  function setOwnerLiveState(mode, message = "") {
    if (mode === "LIVE" && state.ownerLastSuccessAt && Date.now() - state.ownerLastSuccessAt > REFRESH_MS * 2.5) mode = "STALE";
    let label = mode;
    let className = "";
    if (!state.ownerFollow) {
      const age = state.ownerLastSuccessAt ? formatAge((Date.now() - state.ownerLastSuccessAt) / 1000) : "NO SNAPSHOT";
      label = `FROZEN · ${age}`;
      className = "is-frozen";
    } else if (mode === "LIVE") {
      const age = state.ownerLastSuccessAt ? formatAge((Date.now() - state.ownerLastSuccessAt) / 1000) : "0s";
      label = `FOLLOW · SNAPSHOT ${age}`;
    } else if (mode === "STALE") {
      const age = state.ownerLastSuccessAt ? ` · ${formatAge((Date.now() - state.ownerLastSuccessAt) / 1000)} OLD` : "";
      label = `STALE${age}`;
      className = "is-stale";
    } else if (mode === "ERROR") {
      label = "ERROR · NO SNAPSHOT";
      className = "is-error";
    } else {
      className = "is-stale";
    }
    elements.ownerLiveState.textContent = label;
    elements.ownerLiveState.className = `owner-live-state${className ? ` ${className}` : ""}`;
    elements.ownerLiveState.title = message || "";
  }

  function setOwnerFollow(next) {
    state.ownerFollow = Boolean(next);
    elements.ownerFollow.classList.toggle("is-active", state.ownerFollow);
    elements.ownerFollow.setAttribute("aria-pressed", String(state.ownerFollow));
    elements.ownerFollow.textContent = state.ownerFollow ? "FOLLOW ON" : "FOLLOW OFF";
    if (!state.ownerFollow && state.ownerController) {
      state.ownerController.abort();
      state.ownerController = null;
      state.ownerRequest += 1;
      modalSetBusy(false);
    }
    setOwnerLiveState(state.ownerLastError ? "STALE" : state.ownerLastSuccessAt ? "LIVE" : "SYNCING", state.ownerLastError);
    if (state.ownerFollow && state.ownerAddress && !elements.modal.hidden) loadOwner(state.ownerAddress);
  }
  function renderOwnerIdentity(payload) {
    const metadata = selectedIdentity(state.ownerAddress);
    const positions = payload && Array.isArray(payload.positions) ? payload.positions : [];
    const beneficialOwners = [...new Set([
      payload && payload.owner,
      metadata && metadata.owner,
      ...positions.map((position) => position && position.owner)
    ].filter(Boolean).map(String))];
    const reportedCustody = payload && payload.custody || metadata && metadata.custody || null;
    const custodies = [...new Set([
      reportedCustody,
      ...positions.map((position) => position && position.custody)
    ].filter(Boolean).map(String))];
    const compact = (addresses, missing) => addresses.length
      ? `${addresses.slice(0, 2).map((address) => shortIdentifier(address, 9, 7)).join(",")}${addresses.length > 2 ? ` +${addresses.length - 2}` : ""}`
      : missing;
    elements.modalIdentity.textContent = [
      `OWNER ${compact(beneficialOwners, "UNRESOLVED")}`,
      `CUSTODY ${compact(custodies, "—")}`,
    ].join(" · ");
    elements.modalIdentity.title = [
      beneficialOwners.length ? `Owner ${beneficialOwners.join(", ")}` : "Owner unavailable",
      custodies.length ? `On-chain custody ${custodies.join(", ")}` : "Custody address unavailable",
    ].join(". ");
  }


  function modalSetBusy(busy) {
    elements.dialog.setAttribute("aria-busy", String(busy));
  }

  function summaryEntry(label, value, className = "") {
    const wrapper = document.createElement("div");
    const term = el("dt", null, label);
    const description = el("dd", className, value);
    wrapper.append(term, description);
    return wrapper;
  }

  function renderOwnerSummary(summary) {
    const data = summary || {};
    elements.ownerSummary.replaceChildren(
      summaryEntry("positions", formatCount(data.positions)),
      summaryEntry("open", formatCount(data.open_positions)),
      summaryEntry("closed runs", formatCount(data.closed_episodes)),
      summaryEntry("fees", formatUsd(data.fees_usd), data.fees_usd == null ? "neutral" : "positive"),
      summaryEntry("gross P/L", formatSignedUsd(data.gross_pnl_usd), valueClass(data.gross_pnl_usd)),
      summaryEntry("gas", data.gas_usd == null ? "—" : `-${formatUsd(Math.abs(numeric(data.gas_usd)))}`, data.gas_usd == null ? "neutral" : "negative"),
      summaryEntry("net P/L", formatSignedUsd(data.net_pnl_usd), valueClass(data.net_pnl_usd)),
      summaryEntry("win rate", formatRate(data.win_rate))
    );
  }

  function drawOwnerCurve(series, coverage = null) {
    if (state.hidden || elements.modal.hidden) return;
    const chart = elements.ownerCurve;
    const rows = Array.isArray(series) ? series.map((point) => ({
      timestamp: toDate(point.timestamp),
      gross: finite(point.gross_pnl_usd),
      net: finite(point.net_pnl_usd)
    })).filter((point) => point.timestamp).sort((a, b) => a.timestamp - b.timestamp) : [];
    let minimum = 0;
    let maximum = 0;
    let observations = 0;
    for (const point of rows) {
      for (const value of [point.gross, point.net]) {
        if (value == null) continue;
        minimum = Math.min(minimum, value);
        maximum = Math.max(maximum, value);
        observations += 1;
      }
    }
    chart.classList.toggle("is-empty", !observations);
    const width = chart.clientWidth;
    const height = chart.clientHeight;
    if (!width || !height) return;
    const ratio = window.devicePixelRatio || 1;
    const pixelWidth = Math.round(width * ratio);
    const pixelHeight = Math.round(height * ratio);
    if (chart.width !== pixelWidth || chart.height !== pixelHeight) {
      chart.width = pixelWidth;
      chart.height = pixelHeight;
    }
    const context = chart.getContext("2d");
    if (!context) return;
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, width, height);
    context.font = `11px ${getComputedStyle(chart).fontFamily}`;
    context.textBaseline = "middle";
    elements.ownerCurveNote.title = coverage ? describeCoverage(coverage) : "";
    if (!observations) {
      context.fillStyle = "#666";
      context.textAlign = "center";
      context.fillText("NO REALIZED P/L", width / 2, height / 2);
      elements.ownerCurveNote.textContent = "";
      chart.setAttribute("aria-label", "No realized LP profit and loss observations.");
      return;
    }
    if (minimum === maximum) {
      minimum -= 1;
      maximum += 1;
    }
    const rawStep = (maximum - minimum) / 4;
    const magnitude = 10 ** Math.floor(Math.log10(rawStep));
    const step = [1, 2, 5, 10].find((factor) => factor * magnitude >= rawStep) * magnitude;
    minimum = Math.floor(minimum / step) * step;
    maximum = Math.ceil(maximum / step) * step;
    const axisLabel = (value) => value === 0 ? "$0" : `${value < 0 ? "-" : ""}$${formatCount(Math.abs(value))}`;
    const left = Math.max(58, context.measureText(axisLabel(minimum)).width + 12, context.measureText(axisLabel(maximum)).width + 12);
    const right = width - 14;
    const top = 12;
    const bottom = height - 28;
    const start = rows[0].timestamp.getTime();
    const end = rows[rows.length - 1].timestamp.getTime();
    const xFor = (timestamp) => start === end ? (left + right) / 2 : left + ((timestamp - start) / (end - start)) * (right - left);
    const yFor = (value) => bottom - ((value - minimum) / (maximum - minimum)) * (bottom - top);
    context.lineWidth = 1;
    context.textAlign = "right";
    for (let tick = 0; tick <= Math.round((maximum - minimum) / step); tick += 1) {
      const value = minimum + tick * step;
      const y = yFor(value);
      context.strokeStyle = value === 0 ? "#3b4b43" : "#17231e";
      context.beginPath();
      context.moveTo(left, y);
      context.lineTo(right, y);
      context.stroke();
      context.fillStyle = "#829088";
      context.fillText(axisLabel(value), left - 8, y);
    }
    const timeTicks = start === end ? 1 : width < 480 ? 2 : 5;
    for (let tick = 0; tick < timeTicks; tick += 1) {
      const timestamp = timeTicks === 1 ? start : start + ((end - start) * tick) / (timeTicks - 1);
      const stamp = new Date(timestamp).toISOString();
      const label = end - start < 86400000 ? stamp.slice(11, 19) : stamp.slice(5, 16).replace("T", " ");
      context.textAlign = timeTicks === 1 ? "center" : tick === 0 ? "left" : tick === timeTicks - 1 ? "right" : "center";
      context.fillText(label, xFor(timestamp), height - 10);
    }
    const plot = (field, color, dashed) => {
      context.strokeStyle = color;
      context.lineWidth = dashed ? 1.25 : 1.8;
      context.setLineDash(dashed ? [4, 3] : []);
      context.beginPath();
      let previous = null;
      let last = null;
      for (const point of rows) {
        if (point[field] == null) {
          previous = null;
          continue;
        }
        const x = xFor(point.timestamp.getTime());
        const y = yFor(point[field]);
        if (previous) {
          // Realized returns change at observations, not between them.
          context.lineTo(x, previous.y);
          context.lineTo(x, y);
        } else {
          context.moveTo(x, y);
        }
        previous = { x, y };
        last = previous;
      }
      context.stroke();
      context.setLineDash([]);
      if (last) {
        context.fillStyle = color;
        context.beginPath();
        context.arc(last.x, last.y, 2.5, 0, Math.PI * 2);
        context.fill();
      }
    };
    plot("gross", "#82b58d", true);
    plot("net", "#78d8e5", false);
    const latest = rows[rows.length - 1];
    elements.ownerCurveNote.textContent = `GROSS ${formatSignedUsd(latest.gross)} · NET ${formatSignedUsd(latest.net)} · UTC`;
    chart.setAttribute("aria-label", `Realized LP profit and loss. Gross ${formatSignedUsd(latest.gross)}; net ${formatSignedUsd(latest.net)}. From ${rows[0].timestamp.toISOString()} to ${latest.timestamp.toISOString()}.`);
  }

  function renderOwnerDetail(payload) {
    state.ownerDetail = payload;
    state.ownerDetailAddress = state.ownerAddress;
    state.ownerLastSuccessAt = Date.now();
    state.ownerLastError = "";
    const summary = payload && payload.summary || {};
    renderOwnerSummary(summary);
    renderOwnerIdentity(payload);
    const positions = payload && Array.isArray(payload.positions) ? payload.positions.filter((position) => !["closed", "complete", "historical"].includes(String(position.status || "").toLowerCase())) : [];
    const closed = payload && Array.isArray(payload.closed) ? payload.closed : [];
    ownerPositionsTable.reconcile(positions, "NO OPEN / AWAITING-CLAIM POSITIONS");
    ownerClosedTable.reconcile(closed, "NO CLOSED POSITIONS");
    setOwnerLiveState("LIVE");
    drawOwnerCurve(payload && payload.series, payload && payload.coverage);
  }

  async function loadOwner(address) {
    if (!state.ownerFollow || elements.modal.hidden) return;
    const request = ++state.ownerRequest;
    if (state.ownerController) state.ownerController.abort();
    const controller = new AbortController();
    state.ownerController = controller;
    state.ownerAddress = address;
    modalSetBusy(true);
    elements.modalError.hidden = true;
    elements.modalError.textContent = "";
    if (!state.ownerLastSuccessAt || state.ownerDetailAddress !== address) {
      setOwnerLiveState("SYNCING");
    }
    try {
      const payload = await api("/owner", { owner: address, window: state.window }, controller.signal);
      if (request !== state.ownerRequest || controller.signal.aborted) return;
      renderOwnerDetail(payload || {});
    } catch (error) {
      if (error.name === "AbortError" || request !== state.ownerRequest) return;
      const retained = state.ownerDetailAddress === address;
      state.ownerLastError = errorMessage(error);
      elements.modalError.textContent = `OWNER ${retained ? "STALE" : "ERROR"} · ${state.ownerLastError}`;
      elements.modalError.title = retained
        ? "The last confirmed owner detail remains visible."
        : "No accounting values are shown for this address.";
      elements.modalError.hidden = false;
      setOwnerLiveState(retained ? "STALE" : "ERROR", state.ownerLastError);
      if (!retained) {
        ownerPositionsTable.reconcile([], "OWNER POSITIONS UNAVAILABLE");
        ownerClosedTable.reconcile([], "OWNER CASHFLOWS UNAVAILABLE");
      }
    } finally {
      if (request === state.ownerRequest) {
        state.ownerController = null;
        modalSetBusy(false);
      }
    }
  }

  function openOwner(address, trigger, historyMode = "push") {
    const normalized = String(address || "").trim().toLowerCase();
    if (!ADDRESS_RE.test(normalized)) return;
    const changingOwner = state.ownerDetailAddress !== normalized;
    state.modalReturnFocus = trigger instanceof HTMLElement ? trigger : document.activeElement;
    state.ownerAddress = normalized;
    state.ownerPaused = false;
    elements.modal.hidden = false;
    syncOwnerProjection("Owner detail opened");
    abortPoolRequests();
    elements.modalTitle.textContent = "LP DETAIL";
    elements.context.textContent = `LP ${shortIdentifier(normalized, 7, 5)}`;
    elements.context.title = normalized;
    elements.modalAddress.replaceChildren(copyButton(normalized, normalized));
    elements.lpSearchInput.value = normalized;
    elements.lpSearchResults.replaceChildren();
    elements.lpSearchStatus.className = "lp-search-status";
    elements.lpSearchStatus.textContent = "OWNER OPEN";
    renderOwnerIdentity(state.ownerDetailAddress === normalized ? state.ownerDetail : null);
    if (changingOwner) {
      state.ownerDetail = null;
      state.ownerLastSuccessAt = 0;
      state.ownerLastError = "";
      renderOwnerSummary({});
      ownerPositionsTable.reconcile([], "LOADING POSITIONS");
      ownerClosedTable.reconcile([], "LOADING CASHFLOWS");
      drawOwnerCurve([]);
    }
    if (historyMode !== "none") {
      const url = new URL(window.location.href);
      url.pathname = "/lp";
      url.searchParams.set("owner", normalized);
      url.searchParams.set("window", state.window);
      window.history[historyMode === "replace" ? "replaceState" : "pushState"](null, "", url);
    }
    elements.dialog.focus({ preventScroll: true });
    elements.modal.scrollIntoView({ behavior: "auto", block: "start" });
    loadOwner(normalized);
  }

  function closeOwner(historyMode = "push") {
    if (elements.modal.hidden) return;
    if (state.ownerController) state.ownerController.abort();
    state.ownerController = null;
    state.ownerRequest += 1;
    state.ownerPaused = false;
    elements.modal.hidden = true;
    syncOwnerProjection("Owner detail closed");
    refreshAggregates("owner detail closed");
    elements.context.textContent = "LP WALLETS";
    elements.context.title = "";
    if (historyMode !== "none") {
      const url = new URL(window.location.href);
      url.searchParams.delete("owner");
      window.history[historyMode === "replace" ? "replaceState" : "pushState"](null, "", url);
    }
    const target = state.modalReturnFocus;
    state.modalReturnFocus = null;
    if (target && target.isConnected && typeof target.focus === "function") target.focus();
  }

  function trapModalFocus(event, dialog = elements.dialog, close = closeOwner) {
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      close();
      return;
    }
    if (event.key !== "Tab") return;
    const focusable = Array.from(dialog.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    )).filter((node) => node.getClientRects().length > 0);
    const first = focusable[0], last = focusable[focusable.length - 1];
    const active = document.activeElement;
    if (!first) {
      event.preventDefault();
      dialog.focus();
    } else if (event.shiftKey && (active === first || active === dialog)) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (active === last || active === dialog)) {
      event.preventDefault();
      first.focus();
    }
  }

  function poolInspectorUrl(href) {
    try {
      const url = new URL(href, window.location.href);
      if (url.origin !== window.location.origin || url.pathname !== "/pool") return null;
      const externalUrl = new URL(url);
      externalUrl.searchParams.delete("embedded");
      url.searchParams.set("embedded", "1");
      return { embedded: url, external: externalUrl };
    } catch (_) {
      return null;
    }
  }

  function postInspectorVisibility(open) {
    const target = elements.poolInspectorFrame.contentWindow;
    if (target && elements.poolInspectorFrame.src !== "about:blank") {
      target.postMessage({ type: "lp-workbench:visibility", open }, window.location.origin);
    }
  }

  function openPoolInspector(href, trigger) {
    const urls = poolInspectorUrl(href);
    if (!urls) return false;
    state.inspectorReturnFocus = trigger instanceof HTMLElement ? trigger : document.activeElement;
    state.inspectorScrollTop = elements.terminalMain.scrollTop;
    state.inspectorUrl = urls.embedded.href;
    elements.poolInspectorContext.textContent = urls.external.searchParams.get("id") || "POOL";
    elements.poolInspectorContext.title = urls.external.href;
    elements.poolInspectorNewTab.href = urls.external.href;
    elements.poolInspector.hidden = false;
    syncOwnerProjection("Pool inspector opened");
    abortPoolRequests();
    if (state.aggregateController) state.aggregateController.abort();
    if (elements.poolInspectorFrame.src !== urls.embedded.href) {
      elements.poolInspectorFrame.src = urls.embedded.href;
    } else {
      postInspectorVisibility(true);
    }
    elements.poolInspectorShell.focus({ preventScroll: true });
    return true;
  }

  function closePoolInspector({ restoreFocus = true } = {}) {
    if (elements.poolInspector.hidden) return;
    const target = state.inspectorReturnFocus;
    const scrollTop = state.inspectorScrollTop;
    state.inspectorReturnFocus = null;
    state.inspectorUrl = "";
    postInspectorVisibility(false);
    elements.poolInspector.hidden = true;
    syncOwnerProjection("Pool inspector closed");
    refreshAggregates("pool inspector closed");
    setTimeout(() => {
      if (elements.poolInspector.hidden && !state.inspectorUrl) elements.poolInspectorFrame.src = "about:blank";
    }, 50);
    requestAnimationFrame(() => {
      elements.terminalMain.scrollTop = scrollTop;
      if (restoreFocus && target && target.isConnected && typeof target.focus === "function") {
        target.focus({ preventScroll: true });
      }
    });
  }

  function refreshVisibleTapeAges() {
    for (const [key, item] of tapeTable.items) {
      const row = tapeTable.rowFor(key);
      if (!row) continue;
      const eventTime = eventTimeLabel(item);
      setTextCell(row.cells[0], eventTime.text, "dim", eventTime.title);
    }
  }

  function refreshVisibleOwnerAges() {
    for (const [key, item] of ownersTable.items) {
      const row = ownersTable.rowFor(key);
      const activity = item && item.activity;
      if (!row || !activity) continue;
      const age = row.cells[3].querySelector(".owner-activity-age");
      if (!age) continue;
      const ageView = ownerActivityAgeView(activity);
      if (age.textContent !== ageView.text) age.textContent = ageView.text;
      if (age.className !== ageView.className) age.className = ageView.className;
      if (age.title !== ageView.title) age.title = ageView.title;
    }
  }


  async function copyText(value) {
    let copied = false;
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(value);
        copied = true;
      }
    } catch (_) {
      copied = false;
    }
    if (!copied) {
      const input = document.createElement("textarea");
      input.value = value;
      input.setAttribute("readonly", "");
      input.style.position = "fixed";
      input.style.opacity = "0";
      document.body.append(input);
      input.select();
      copied = document.execCommand("copy");
      input.remove();
    }
    elements.copyStatus.textContent = copied ? `Copied ${value}` : `Could not copy ${value}`;
  }

  function updateClock() {
    const now = new Date();
    elements.clock.textContent = formatTime(now, true);
    elements.clock.dateTime = now.toISOString();
    renderLiveBlockAge();
    renderOwnerFreshness();
    const ageTick = Math.floor(now.getTime() / 15_000);
    if (ageTick !== state.lastTapeAgeTick) {
      state.lastTapeAgeTick = ageTick;
      refreshVisibleTapeAges();
      refreshVisibleOwnerAges();
    }
    const statusHealth = state.health.get("status");
    if (state.hidden) {
      elements.freshness.textContent = "PAUSED";
    } else if (statusHealth && statusHealth.at) {
      const age = (Date.now() - statusHealth.at) / 1000;
      elements.freshness.textContent = `STATUS ${formatAge(age)}`;
    } else {
      elements.freshness.textContent = "STATUS —";
    }
    renderHealth();
    if (!elements.modal.hidden) {
      if (state.hidden) {
        elements.ownerLiveState.textContent = "FROZEN · TAB HIDDEN";
        elements.ownerLiveState.className = "owner-live-state is-frozen";
      } else {
        setOwnerLiveState(state.ownerLastError ? "STALE" : state.ownerLastSuccessAt ? "LIVE" : "SYNCING", state.ownerLastError);
      }
    }
  }

  function handleVisibility() {
    state.hidden = document.hidden;
    if (state.hidden) {
      clearTimeout(state.refreshTimer);
      clearTimeout(state.historyTimer);
      state.historyTimer = null;
      clearTimeout(state.filterTimer);
      if (state.aggregateController) state.aggregateController.abort();
      abortPoolRequests();
      if (state.tapeController) state.tapeController.abort();
      if (state.ownerController) {
        state.ownerController.abort();
        state.ownerController = null;
        state.ownerRequest += 1;
        state.ownerPaused = true;
        modalSetBusy(false);
      }
      closeStream();
      setStreamState("PAUSED", "is-stale");
      return;
    }
    if (state.renderQueue.size && !state.renderFrame) state.renderFrame = requestAnimationFrame(flushRenders);
    reloadTape("resume");
    renderAllTables();
    refreshAggregates("resume");
    if (!elements.modal.hidden && state.ownerPaused && state.ownerAddress) {
      state.ownerPaused = false;
      loadOwner(state.ownerAddress);
    } else if (state.ownerDetail && !elements.modal.hidden) {
      drawOwnerCurve(state.ownerDetail.series, state.ownerDetail.coverage);
    }
  }

  function readPanePreferences() {
    try {
      const value = JSON.parse(localStorage.getItem(PANE_STORAGE_KEY) || "{}");
      return value && typeof value === "object" && !Array.isArray(value) ? value : {};
    } catch (_) {
      return {};
    }
  }

  function validPaneSizes(layout, sizes) {
    if (!Array.isArray(sizes) || sizes.length !== 3 || !sizes.every(Number.isFinite)) return false;
    return layout === "mobile"
      ? sizes.every((size) => size >= 25 && size <= 160)
      : sizes.every((size) => size >= .05 && size <= .9);
  }

  function setPaneProperties(layout, sizes) {
    const unit = layout === "mobile" ? "dvh" : "fr";
    sizes.forEach((size, index) => {
      document.documentElement.style.setProperty(PANE_PROPERTIES[index], `${size}${unit}`);
    });
  }

  function updatePaneSeparatorValues() {
    elements.paneSeparators.forEach((separator, index) => {
      const upper = elements.panes[index].getBoundingClientRect().height;
      const lower = elements.panes[index + 1].getBoundingClientRect().height;
      const total = upper + lower;
      if (!total) return;
      const minimums = PANE_MINIMUMS[state.paneLayout];
      separator.setAttribute("aria-valuemin", String(Math.round(100 * minimums[index] / total)));
      separator.setAttribute("aria-valuemax", String(Math.round(100 * (total - minimums[index + 1]) / total)));
      separator.setAttribute("aria-valuenow", String(Math.round(100 * upper / total)));
      separator.setAttribute("aria-valuetext", `${Math.round(upper)} pixels above, ${Math.round(lower)} pixels below`);
    });
  }

  function storePaneSizes() {
    const heights = elements.panes.map((pane) => pane.getBoundingClientRect().height);
    const total = heights.reduce((sum, height) => sum + height, 0);
    if (!total) return;
    const sizes = state.paneLayout === "mobile"
      ? heights.map((height) => Number((100 * height / window.innerHeight).toFixed(3)))
      : heights.map((height) => Number((height / total).toFixed(6)));
    const preferences = readPanePreferences();
    preferences[state.paneLayout] = sizes;
    try {
      localStorage.setItem(PANE_STORAGE_KEY, JSON.stringify(preferences));
    } catch (_) {
      // Resizing remains functional when storage is blocked or full.
    }
    setPaneProperties(state.paneLayout, sizes);
    updatePaneSeparatorValues();
  }

  function setPanePairPixels(index, requestedUpper) {
    const heights = elements.panes.map((pane) => pane.getBoundingClientRect().height);
    const total = heights[index] + heights[index + 1];
    const minimums = PANE_MINIMUMS[state.paneLayout];
    const minimumUpper = minimums[index];
    const maximumUpper = total - minimums[index + 1];
    if (maximumUpper < minimumUpper) return;
    const upper = Math.min(maximumUpper, Math.max(minimumUpper, requestedUpper));
    heights[index] = upper;
    heights[index + 1] = total - upper;
    // Do not mix normalized sub-1fr tracks with pixels: CSS leaves unused
    // space in that case, unexpectedly shrinking the uninvolved pane.
    heights.forEach((height, paneIndex) => {
      document.documentElement.style.setProperty(PANE_PROPERTIES[paneIndex], `${height}px`);
    });
    updatePaneSeparatorValues();
  }

  function finishPaneDrag(event) {
    const drag = state.paneDrag;
    if (!drag || (event.pointerId != null && event.pointerId !== drag.pointerId)) return;
    drag.separator.classList.remove("is-dragging");
    document.body.classList.remove("is-resizing-panes");
    state.paneDrag = null;
    storePaneSizes();
  }

  function startPaneDrag(event, separator, index) {
    if (!event.isPrimary || event.button !== 0) return;
    event.preventDefault();
    const upper = elements.panes[index].getBoundingClientRect().height;
    state.paneDrag = {
      separator,
      index,
      pointerId: event.pointerId,
      startY: event.clientY,
      startUpper: upper
    };
    separator.setPointerCapture(event.pointerId);
    separator.classList.add("is-dragging");
    document.body.classList.add("is-resizing-panes");
  }

  function movePaneDrag(event) {
    const drag = state.paneDrag;
    if (!drag || event.pointerId !== drag.pointerId) return;
    event.preventDefault();
    setPanePairPixels(drag.index, drag.startUpper + event.clientY - drag.startY);
  }

  function resizePaneWithKeyboard(event, index) {
    const keys = ["ArrowUp", "ArrowDown", "PageUp", "PageDown", "Home", "End"];
    if (!keys.includes(event.key)) return;
    event.preventDefault();
    const upper = elements.panes[index].getBoundingClientRect().height;
    const lower = elements.panes[index + 1].getBoundingClientRect().height;
    const total = upper + lower;
    const minimums = PANE_MINIMUMS[state.paneLayout];
    let next = upper;
    if (event.key === "ArrowUp") next -= 16;
    else if (event.key === "ArrowDown") next += 16;
    else if (event.key === "PageUp") next -= 48;
    else if (event.key === "PageDown") next += 48;
    else if (event.key === "Home") next = minimums[index];
    else next = total - minimums[index + 1];
    setPanePairPixels(index, next);
    storePaneSizes();
  }

  function applyPaneLayout() {
    state.paneLayout = PANE_LAYOUT_MEDIA.matches ? "mobile" : "desktop";
    const controls = byId("key-strip");
    const destination = state.paneLayout === "mobile"
      ? byId("mobile-controls-slot") : document.querySelector(".status-strip");
    if (controls.parentElement !== destination) destination.append(controls);
    const filterButton = byId("mobile-filter-toggle");
    const filterDestination = state.paneLayout === "mobile"
      ? byId("lp-search-form") : byId("tape-section").querySelector(".section-head");
    if (filterButton.parentElement !== filterDestination) filterDestination.append(filterButton);
    const arrivalsDestination = state.paneLayout === "mobile"
      ? byId("tape-section") : byId("tape-section").querySelector(".section-head");
    if (elements.tapeArrivals.parentElement !== arrivalsDestination) arrivalsDestination.append(elements.tapeArrivals);
    PANE_PROPERTIES.forEach((property) => document.documentElement.style.removeProperty(property));
    const sizes = readPanePreferences()[state.paneLayout];
    if (validPaneSizes(state.paneLayout, sizes)) setPaneProperties(state.paneLayout, sizes);
    requestAnimationFrame(updatePaneSeparatorValues);
  }

  function resetPaneLayouts() {
    try {
      localStorage.removeItem(PANE_STORAGE_KEY);
    } catch (_) {
      // CSS defaults can still be restored when storage access is unavailable.
    }
    PANE_PROPERTIES.forEach((property) => document.documentElement.style.removeProperty(property));
    elements.copyStatus.textContent = "Pane heights reset";
    requestAnimationFrame(updatePaneSeparatorValues);
  }

  function isTypingTarget(target) {
    return target instanceof HTMLElement && (target.matches("input, select, textarea, button") || target.isContentEditable);
  }

  document.addEventListener("keydown", (event) => {
    if (!elements.indexStatus.hidden) return;
    if (!elements.poolInspector.hidden && event.key === "Escape") {
      event.preventDefault();
      closePoolInspector();
      return;
    }
    if (!elements.modal.hidden && event.key === "Escape" && !isTypingTarget(event.target)) {
      event.preventDefault();
      closeOwner();
      return;
    }
    if (event.metaKey || event.ctrlKey || event.altKey) return;
    if (isTypingTarget(event.target)) {
      if (event.key === "Escape" && event.target === elements.marketFilter) elements.marketFilter.blur();
      if (event.key === "Escape" && event.target === elements.lpSearchInput) {
        elements.lpSearchInput.value = "";
        searchUniversal("");
      }
      return;
    }
    if (WINDOW_KEYS[event.key]) {
      event.preventDefault();
      setWindow(WINDOW_KEYS[event.key]);
    } else if (event.key === "/") {
      event.preventDefault();
      elements.marketFilter.focus();
      elements.marketFilter.select();
    } else if (event.key.toLowerCase() === "f") {
      event.preventDefault();
      setFollow(!state.follow);
    }
  });

  document.addEventListener("click", (event) => {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    const copy = event.target.closest && event.target.closest("[data-copy]");
    if (copy && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) {
      event.preventDefault();
      if (copy.dataset.owner && (event.pointerType === "touch" || TOUCH_NAVIGATION.matches)) {
        openOwner(copy.dataset.owner, copy);
      } else {
        copyText(copy.dataset.copy);
      }
      return;
    }
    const poolLink = event.target.closest && event.target.closest("a[href]");
    if (poolLink
        && event.button === 0
        && !event.defaultPrevented
        && !event.metaKey
        && !event.ctrlKey
        && !event.shiftKey
        && !event.altKey
        && poolLink.target !== "_blank") {
      const urls = poolInspectorUrl(poolLink.href);
      if (urls) {
        event.preventDefault();
        openPoolInspector(urls.external.href, poolLink);
        return;
      }
    }
    const owner = event.target.closest && event.target.closest("[data-owner]");
    if (owner) {
      event.preventDefault();
      openOwner(owner.dataset.owner, owner);
      return;
    }
  });

  document.addEventListener("dblclick", (event) => {
    const link = event.target.closest && event.target.closest("a[data-copy]");
    if (!link) return;
    event.preventDefault();
    const urls = poolInspectorUrl(link.href);
    if (urls) openPoolInspector(urls.external.href, link);
    else if (link.dataset.owner) openOwner(link.dataset.owner, link);
  });

  elements.lpSearchForm.addEventListener("submit", (event) => {
    event.preventDefault();
    clearTimeout(state.searchTimer);
    const query = elements.lpSearchInput.value.trim();
    if (!query) {
      searchUniversal("");
      elements.lpSearchInput.focus();
      return;
    }
    searchUniversal(query);
  });
  elements.lpSearchInput.addEventListener("input", () => {
    clearTimeout(state.searchTimer);
    const query = elements.lpSearchInput.value.trim();
    if (!query) {
      searchUniversal("");
      return;
    }
    state.searchTimer = setTimeout(() => searchUniversal(query), FILTER_DELAY_MS);
  });

  document.querySelectorAll("[data-window]").forEach((button) => button.addEventListener("click", () => setWindow(button.dataset.window)));
  byId("mobile-window").addEventListener("change", (event) => setWindow(event.target.value));
  byId("mobile-pool-sort").addEventListener("change", (event) => {
    const key = event.target.value;
    setTab(key === "flow" ? "flow" : key === "created" ? "fresh" : "pools", false, false);
    setPoolSort(key, false);
  });
  byId("mobile-pool-order").addEventListener("click", () => setPoolSort(state.poolSort));
  const mobileMore = document.querySelector(".mobile-more");
  document.addEventListener("click", (event) => {
    if (mobileMore.open && !mobileMore.contains(event.target)) mobileMore.open = false;
  });
  mobileMore.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      mobileMore.open = false;
      mobileMore.querySelector("summary").focus();
    }
  });
  byId("mobile-filter-toggle").addEventListener("click", (event) => {
    const expanded = event.currentTarget.getAttribute("aria-expanded") !== "true";
    event.currentTarget.setAttribute("aria-expanded", String(expanded));
    byId("tape-tools").classList.toggle("is-open", expanded);
  });
  elements.followControl.addEventListener("click", () => setFollow(!state.follow));
  elements.tapeArrivals.addEventListener("click", releaseHeldTapeRows);
  elements.paneSeparators.forEach((separator, index) => {
    separator.addEventListener("pointerdown", (event) => startPaneDrag(event, separator, index));
    separator.addEventListener("pointermove", movePaneDrag);
    separator.addEventListener("pointerup", finishPaneDrag);
    separator.addEventListener("pointercancel", finishPaneDrag);
    separator.addEventListener("keydown", (event) => resizePaneWithKeyboard(event, index));
    separator.addEventListener("dblclick", resetPaneLayouts);
  });
  elements.paneReset.addEventListener("click", resetPaneLayouts);
  PANE_LAYOUT_MEDIA.addEventListener("change", applyPaneLayout);
  byId("pools-table").addEventListener("click", (event) => {
    const button = event.target.closest("[data-pool-sort]");
    if (button) setPoolSort(button.dataset.poolSort);
  });
  elements.filterControl.addEventListener("click", () => {
    elements.marketFilter.focus();
    elements.marketFilter.select();
  });
  elements.marketFilter.addEventListener("input", () => {
    state.q = elements.marketFilter.value.trim().toLowerCase();
    queueFilterRefresh();
  });
  elements.protocolFilter.addEventListener("change", () => {
    state.protocol = elements.protocolFilter.value;
    refreshNow("protocol");
  });
  elements.tapeKind.addEventListener("change", () => {
    state.tapeKind = elements.tapeKind.value;
    reloadTape("kind");
    scheduleRender("tape", renderTape);
  });
  elements.ownerSort.addEventListener("change", () => {
    state.ownerSort = elements.ownerSort.value;
    invalidateOwners("Wallet sort changed; waiting for a fresh summary");
    openStream(true);
  });
  elements.ownerScope.addEventListener("change", () => {
    state.ownerScope = elements.ownerScope.value;
    byId("owners-title").textContent = state.ownerScope === "wallets" ? "LP WALLETS" : "LP CUSTODY";
    byId("owners-table").tHead.rows[0].cells[0].textContent = state.ownerScope === "wallets" ? "LP wallet" : "Custody contract";
    document.querySelector(".wallet-description").textContent = state.ownerScope === "wallets"
      ? "Who is providing liquidity, and where? Select a wallet for positions and accounting."
      : "Contracts holding positions for multiple wallets. Custody activity is not one trader’s portfolio.";
    invalidateOwners("Identity scope changed");
    openStream(true);
  });
  elements.tabs.addEventListener("click", (event) => {
    const tab = event.target.closest("[role=tab]");
    if (tab) setTab(tab.dataset.tab);
  });
  elements.tabs.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const tabs = Array.from(elements.tabs.querySelectorAll("[role=tab]"));
    const current = tabs.indexOf(document.activeElement);
    let next = current;
    if (event.key === "Home") next = 0;
    else if (event.key === "End") next = tabs.length - 1;
    else if (event.key === "ArrowLeft") next = (current - 1 + tabs.length) % tabs.length;
    else next = (current + 1) % tabs.length;
    setTab(tabs[next].dataset.tab, true);
  });
  elements.ownerFollow.addEventListener("click", () => setOwnerFollow(!state.ownerFollow));
  elements.status.addEventListener("click", () => {
    elements.indexStatus.hidden = false;
    renderStatus();
    elements.indexStatusShell.focus({ preventScroll: true });
  });
  elements.indexStatusClose.addEventListener("click", closeIndexStatus);
  elements.indexStatusShell.addEventListener("keydown", (event) => {
    trapModalFocus(event, elements.indexStatusShell, closeIndexStatus);
  });
  elements.indexStatus.addEventListener("click", (event) => {
    if (event.target === elements.indexStatus) closeIndexStatus();
  });
  elements.modalClose.addEventListener("click", () => closeOwner());
  elements.dialog.addEventListener("keydown", trapModalFocus);
  elements.poolInspectorClose.addEventListener("click", () => closePoolInspector());
  elements.poolInspector.addEventListener("click", (event) => {
    if (event.target === elements.poolInspector) closePoolInspector();
  });
  elements.poolInspectorShell.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      closePoolInspector();
    }
  });
  elements.poolInspectorFrame.addEventListener("load", () => {
    if (!elements.poolInspector.hidden && state.inspectorUrl && elements.poolInspectorFrame.src !== "about:blank") {
      postInspectorVisibility(true);
    }
  });
  window.addEventListener("message", (event) => {
    if (event.origin !== window.location.origin || event.source !== elements.poolInspectorFrame.contentWindow) return;
    const message = event.data;
    if (!message || typeof message !== "object") return;
    if (message.type === "lp-workbench:close") {
      closePoolInspector();
      return;
    }
    if (message.type === "lp-workbench:open-owner" && ADDRESS_RE.test(String(message.owner || ""))) {
      const trigger = state.inspectorReturnFocus;
      closePoolInspector({ restoreFocus: false });
      openOwner(String(message.owner).toLowerCase(), trigger);
    }
  });
  document.addEventListener("visibilitychange", handleVisibility);
  window.addEventListener("popstate", () => {
    const url = new URL(window.location.href);
    const owner = String(url.searchParams.get("owner") || "").toLowerCase();
    if (ADDRESS_RE.test(owner)) openOwner(owner, null, "none");
    else closeOwner("none");
  });
  window.addEventListener("resize", () => {
    cancelAnimationFrame(state.resizeFrame);
    state.resizeFrame = requestAnimationFrame(() => {
      if (state.ownerDetail && !elements.modal.hidden) drawOwnerCurve(state.ownerDetail.series, state.ownerDetail.coverage);
      updatePaneSeparatorValues();
    });
  });

  const initialUrl = new URL(window.location.href);
  const initialWindow = initialUrl.searchParams.get("window");
  if (Object.values(WINDOW_KEYS).includes(initialWindow)) state.window = initialWindow;
  const initialQuery = String(initialUrl.searchParams.get("q") || "").trim();
  if (initialQuery) {
    state.q = initialQuery.toLowerCase();
    elements.marketFilter.value = initialQuery;
  }
  const initialProtocol = String(initialUrl.searchParams.get("protocol") || "").toLowerCase();
  if (["v2", "v3", "v4"].includes(initialProtocol)) {
    state.protocol = initialProtocol;
    elements.protocolFilter.value = initialProtocol;
  }
  document.querySelectorAll("[data-window]").forEach((button) => button.classList.toggle("is-active", button.dataset.window === state.window));
  byId("mobile-window").value = state.window;
  byId("mobile-more-window").textContent = state.window;
  setFollow(true);
  setOwnerFollow(true);
  setTab("pools", false, false);
  applyPaneLayout();
  renderStatus();
  renderOverview();
  renderAllTables();
  updateClock();
  setInterval(updateClock, 1_000);
  reloadTape("initial");
  const initialOwner = String(initialUrl.searchParams.get("owner") || "").toLowerCase();
  if (ADDRESS_RE.test(initialOwner)) openOwner(initialOwner, null, "replace");
  requestAnimationFrame(() => {
    setTimeout(() => {
      startSummaryObservers();
      refreshAggregates("initial");
    }, 0);
  });
})();
