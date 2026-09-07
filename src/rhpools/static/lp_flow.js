(() => {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const elements = {
    form: byId("flow-form"),
    source: byId("source-select"),
    chain: byId("chain-select"),
    verified: byId("verified-select"),
    limit: byId("limit-select"),
    requestState: byId("request-state"),
    requestDetail: byId("request-detail"),
    clock: byId("clock"),
    errorPanel: byId("error-panel"),
    errorMessage: byId("error-message"),
    retry: byId("retry-button"),
    warning: byId("coverage-warning"),
    results: byId("results"),
    evidenceGrid: byId("evidence-grid"),
    evidenceNote: byId("evidence-note"),
    body: byId("flow-body"),
    newer: byId("newer-button"),
    older: byId("older-button"),
    pageNumber: byId("page-number"),
    limitations: byId("limitations-list"),
    footer: byId("footer-state"),
    sourceEyebrow: byId("source-eyebrow"),
    sourceSummary: byId("source-summary"),
    sourceName: byId("source-name"),
    sourceAccess: byId("source-access"),
    sourceChains: byId("source-chains"),
    verifiedLegend: byId("verified-legend"),
    actionLegend: byId("action-legend"),
  };
  const state = { controller: null, data: null, cursor: null, history: [] };
  const timeFormat = new Intl.DateTimeFormat(undefined, {
    month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
    timeZoneName: "short",
  });

  function node(tag, text, className) {
    const item = document.createElement(tag);
    if (text != null) item.textContent = String(text);
    if (className) item.className = className;
    return item;
  }

  function compact(value, start = 7, end = 6) {
    const text = String(value || "");
    return text.length > start + end + 2 ? `${text.slice(0, start)}…${text.slice(-end)}` : text || "—";
  }

  function exactDecimal(value, places = 4) {
    if (value == null) return "—";
    const text = String(value);
    if (!/^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/.test(text)) return "—";
    const [whole, fraction = ""] = text.split(".");
    const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    if (!fraction) return grouped;
    if (fraction.length <= places) return `${grouped}.${fraction}`;
    return `${grouped}.${fraction.slice(0, places)}…`;
  }

  function money(value, places) {
    return value == null ? "—" : `$${exactDecimal(value, places)}`;
  }

  function formatTime(value) {
    const parsed = Date.parse(String(value || ""));
    return Number.isFinite(parsed) ? timeFormat.format(new Date(parsed)) : "—";
  }

  function age(value) {
    const parsed = Date.parse(String(value || ""));
    if (!Number.isFinite(parsed)) return "UNKNOWN";
    const seconds = Math.max(0, Math.round((Date.now() - parsed) / 1000));
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`;
    return `${Math.floor(seconds / 86400)}d ${Math.floor(seconds % 86400 / 3600)}h`;
  }

  function observedDelay(evidence) {
    const occurred = Date.parse(String(evidence?.occurred_at || ""));
    const observed = Date.parse(String(evidence?.observed_at || ""));
    if (!Number.isFinite(occurred) || !Number.isFinite(observed)) return "LAG UNKNOWN";
    const milliseconds = Math.max(0, observed - occurred);
    return milliseconds < 1000 ? `${milliseconds}ms TO OBSERVE` : `${(milliseconds / 1000).toFixed(2)}s TO OBSERVE`;
  }

  function setRequest(label, detail, className = "") {
    elements.requestState.textContent = label;
    elements.requestState.className = `request-state${className ? ` ${className}` : ""}`;
    elements.requestDetail.textContent = detail;
  }

  function cell(row, content, className = "") {
    const td = node("td", null, className);
    if (content instanceof Node) td.append(content);
    else td.textContent = String(content ?? "—");
    row.append(td);
    return td;
  }

  function metric(label, value, className = "") {
    const wrapper = node("div");
    wrapper.append(node("dt", label), node("dd", value, className));
    return wrapper;
  }

  function evidenceUrl(chain, hash) {
    const encoded = encodeURIComponent(String(hash || ""));
    if (!hash) return null;
    if (chain === "solana") return `https://solscan.io/tx/${encoded}`;
    if (chain === "base") return `https://basescan.org/tx/${encoded}`;
    if (chain === "robinhood") return `https://robinhoodchain.blockscout.com/tx/${encoded}`;
    return null;
  }

  function renderEvidence(data) {
    const coverage = data.coverage || {};
    const counts = { solana: 0, base: 0, robinhood: 0 };
    const identities = { "verified-fomo": 0, "observed-wallet": 0 };
    for (const item of data.items) {
      if (Object.hasOwn(counts, item.asset?.chain)) counts[item.asset.chain] += 1;
      if (Object.hasOwn(identities, item.identity?.kind)) identities[item.identity.kind] += 1;
    }
    const chainSummary = `SOL ${counts.solana} · BASE ${counts.base} · RH ${counts.robinhood}`;
    elements.evidenceGrid.replaceChildren(
      metric("returned observations", `${coverage.returned_items ?? data.items.length} / ${coverage.upstream_page_items ?? "—"}`),
      metric("newest evidence", formatTime(data.evidence_through), data.evidence_through ? "good" : "warn"),
      metric("evidence age now", data.evidence_through ? age(data.evidence_through) : "NO MATCHING EVENT", data.evidence_through ? "" : "warn"),
      metric("page chain mix", chainSummary),
      metric("identity mix", `${identities["verified-fomo"]} VERIFIED · ${identities["observed-wallet"]} OBSERVED`),
    );
    const provenance = data.provenance || {};
    const publisher = provenance.publisher || "Selected publisher";
    const status = coverage.publisher_status;
    const sourceHealth = status
      ? ` Publisher status: ${status.tracked_wallets ?? "unknown"} tracked wallets; index lag ${status.lag_seconds_reported ?? "unknown"}s; last block ${status.last_block ?? "unknown"}; block-to-tape median ${status.latency_seconds_reported?.median ?? "unknown"}s / p90 ${status.latency_seconds_reported?.p90 ?? "unknown"}s.`
      : "";
    elements.evidenceNote.textContent = [
      `${publisher} read/status ${formatTime(provenance.upstream_read_at || data.read_at)}.`,
      `Consumer retrieved ${formatTime(provenance.retrieved_at)}.`,
      `Scope: ${data.query?.chain_scope === "supported-set" ? "the Apollo supported three-chain set" : String(data.query?.chain || "unknown chain")}.`,
      `Evidence basis: ${provenance.evidence_time_basis || "publisher contract"}.${sourceHealth}`,
    ].join(" ");

    const warnings = [
      data.query?.source === "rhtrenches"
        ? "RH TRENCHES · ROBINHOOD TRACKED-WALLET SNAPSHOT ONLY"
        : "APOLLO SUPPORTED SET ONLY · NOT ALL-CHAIN COVERAGE",
    ];
    if (coverage.items_filtered_by_chain > 0) {
      warnings.push(`${coverage.items_filtered_by_chain} UPSTREAM PAGE EVENTS OUTSIDE THE SELECTED CHAIN WERE NOT RETURNED`);
    }
    const omitted = Object.entries(coverage.rows_omitted || {});
    if (omitted.length) {
      warnings.push(`${omitted.reduce((sum, entry) => sum + Number(entry[1] || 0), 0)} SOURCE-QUALIFIED ROWS OMITTED`);
    }
    if (!data.items.length) warnings.push("NO QUALIFIED MATCHING EVENT ON THIS BOUNDED UPSTREAM PAGE");
    warnings.push("NO LP OR POOL ROUTE IS ATTRIBUTED");
    elements.warning.hidden = false;
    elements.warning.textContent = warnings.join(" · ");
  }

  function renderIdentity(item) {
    const wrapper = node("div", null, "identity-cell");
    const line = node("div", null, "identity-line-cell");
    const identity = item.identity || {};
    line.append(
      node("span", String(identity.kind || "unknown").toUpperCase(), `identity-tag ${identity.kind || ""}`),
      node("span", identity.handle || "UNLABELED"),
    );
    const wallet = node("small", compact(identity.wallet, 8, 7));
    wallet.title = identity.wallet || "";
    wrapper.append(line, wallet);
    return wrapper;
  }

  function renderAsset(item) {
    const wrapper = node("div", null, "asset-cell");
    const asset = item.asset || {};
    const title = asset.symbol || asset.name || compact(asset.address);
    wrapper.append(node("strong", title));
    const detail = node("small", [asset.name && asset.name !== title ? asset.name : null, compact(asset.address, 7, 6)].filter(Boolean).join(" · "));
    detail.title = asset.address || "";
    wrapper.append(detail);
    return wrapper;
  }

  function renderSource(item) {
    const source = item.source || {};
    const wrapper = node("div", null, "evidence-cell");
    if (source.kind === "chain-observed") {
      const href = evidenceUrl(item.asset?.chain, source.transaction_hash);
      if (href) {
        const link = node("a", "TX");
        link.href = href;
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.title = source.transaction_hash || "";
        wrapper.append(link);
      }
      wrapper.append(node(
        "span",
        source.confirmation_status ? String(source.confirmation_status).toUpperCase() : "BLOCK OBSERVED",
      ));
      const locator = source.instruction_or_log_index == null
        ? `#${source.block_number_or_slot ?? "—"}`
        : `#${source.block_number_or_slot ?? "—"}:${source.instruction_or_log_index}`;
      const block = node("span", locator, "exact");
      block.title = source.instruction_or_log_index == null
        ? `Publisher-reported block ${source.block_number_or_slot ?? "unknown"}; confirmation and log index not published`
        : `Block/slot ${source.block_number_or_slot ?? "unknown"}, instruction/log ${source.instruction_or_log_index}`;
      wrapper.append(block);
    } else {
      wrapper.append(node("span", "FOMO OFFICIAL"), node("span", `SEQ ${source.sequence || "—"}`, "exact"));
    }
    return wrapper;
  }

  function renderRows(data) {
    const rows = [];
    for (const item of data.items) {
      const row = node("tr");
      const occurred = cell(row, formatTime(item.evidence?.occurred_at));
      occurred.title = `${item.evidence?.occurred_at || "unknown"} · ${observedDelay(item.evidence)}`;
      cell(row, node("span", String(item.asset?.chain || "unknown").toUpperCase(), `chain-tag ${item.asset?.chain || ""}`));
      cell(row, node("span", String(item.action || "unknown").toUpperCase(), `action-tag ${item.action || ""}`));
      cell(row, renderAsset(item));
      cell(row, renderIdentity(item));
      const size = cell(row, money(item.economics?.trade_size_usd, 4), "numeric");
      size.title = item.economics?.trade_size_usd || "not published";
      const price = cell(row, money(item.economics?.leader_fill_price_usd, 8), "numeric");
      price.title = item.economics?.leader_fill_price_usd || "not published";
      const beforeValue = item.economics?.leader_position_before_usd;
      const afterValue = item.economics?.leader_position_after_usd;
      const positionText = beforeValue == null || afterValue == null
        ? "NOT PUBLISHED"
        : `${money(beforeValue, 2)} → ${money(afterValue, 2)}`;
      const position = cell(row, positionText, "numeric");
      position.title = beforeValue == null || afterValue == null
        ? "This source does not publish before/after position value in the selected raw projection"
        : `${beforeValue} → ${afterValue} USD`;
      cell(row, renderSource(item));
      rows.push(row);
    }
    if (!rows.length) {
      const row = node("tr", null, "empty-row");
      const td = cell(row, "NO MATCHING OBSERVATION ON THIS BOUNDED PAGE");
      td.colSpan = 9;
      rows.push(row);
    }
    elements.body.replaceChildren(...rows);
  }

  function renderSourcePresentation(source) {
    const rhtrenches = source === "rhtrenches";
    elements.sourceEyebrow.textContent = rhtrenches
      ? "RH TRENCHES / UNOFFICIAL ANONYMOUS PUBLIC TAPE"
      : "APOLLO / ANONYMOUS PUBLISHED FLOWPAGE";
    elements.sourceName.textContent = rhtrenches ? "RH TRENCHES TAPE" : "APOLLO FLOWPAGE";
    elements.sourceAccess.textContent = "ANONYMOUS PUBLIC READ";
    elements.sourceChains.textContent = rhtrenches ? "ROBINHOOD · CHAIN 4663" : "SOLANA · BASE · ROBINHOOD";
    elements.sourceSummary.textContent = rhtrenches
      ? "A bounded projection of RH Trenches' public Robinhood Chain tape. Warning-qualified and estimated-value rows are omitted; P/L, reputation, followers, market enrichment, and lead/follower inference never cross this boundary."
      : "A bounded view of Apollo's public FlowPage on its supported Solana, Base, and Robinhood set. Observed-wallet and verified-fomo identity remain distinct, with source event and durable-observation clocks preserved.";
    elements.verifiedLegend.hidden = rhtrenches;
    elements.actionLegend.textContent = rhtrenches
      ? "BUY / SELL ARE RH TRENCHES’ PUBLISHED SIDE — NOT INTENT OR RECOMMENDATIONS"
      : "OPEN / INCREASE / DECREASE / CLOSE ARE APOLLO ACTIONS — NOT RECOMMENDATIONS";
    return rhtrenches;
  }

  function render(data) {
    const rhtrenches = renderSourcePresentation(data.query?.source);
    renderEvidence(data);
    renderRows(data);
    elements.limitations.replaceChildren(...(data.limitations || []).map((text) => node("li", text)));
    elements.pageNumber.textContent = rhtrenches ? "CURRENT" : `PAGE ${state.history.length + 1}`;
    elements.newer.disabled = rhtrenches || state.history.length === 0;
    elements.older.disabled = rhtrenches || !data.next_cursor;
    elements.results.hidden = false;
    const scope = data.query?.chain ? String(data.query.chain).toUpperCase() : "SUPPORTED 3-CHAIN SET";
    let identity = "OBSERVED/PUBLISHED IDENTITIES";
    if (rhtrenches) identity = "OBSERVED-WALLET ONLY";
    else if (data.query?.verified_only) identity = "VERIFIED FOMO";
    elements.footer.textContent = `${String(data.query?.source || "source").toUpperCase()} · ${scope} · ${identity} · ${data.items.length} RETURNED · EVIDENCE ${data.evidence_through ? age(data.evidence_through) + " AGO" : "ABSENT ON PAGE"}`;
  }

  function currentFilters() {
    return {
      source: elements.source.value,
      chain: elements.chain.value,
      verified: elements.verified.value,
      limit: elements.limit.value,
    };
  }

  function updateLocation() {
    const filters = currentFilters();
    const query = new URLSearchParams({
      source: filters.source, verified: filters.verified, limit: filters.limit,
    });
    if (filters.chain) query.set("chain", filters.chain);
    window.history.replaceState(null, "", `${window.location.pathname}?${query}`);
  }

  async function load(cursor, history) {
    if (state.controller) state.controller.abort();
    const controller = new AbortController();
    state.controller = controller;
    elements.errorPanel.hidden = true;
    elements.form.querySelector("button").disabled = true;
    elements.newer.disabled = true;
    elements.older.disabled = true;
    const filters = currentFilters();
    const sourceLabel = filters.source === "rhtrenches" ? "RH Trenches tape + status" : "Apollo FlowPage";
    setRequest("LOADING", `Reading one bounded ${sourceLabel}…`, "loading");
    const query = new URLSearchParams({
      source: filters.source, verified: filters.verified, limit: filters.limit,
    });
    if (filters.chain) query.set("chain", filters.chain);
    if (cursor && filters.source === "apollo") query.set("cursor", cursor);
    try {
      const response = await fetch(`/api/v1/fomo/flow?${query}`, {
        method: "GET",
        headers: { Accept: "application/json" },
        cache: "no-store",
        signal: controller.signal,
      });
      if (!response.ok) {
        let detail = `HTTP ${response.status}`;
        try {
          const problem = await response.json();
          if (typeof problem.error === "string") detail = problem.error;
          else if (problem.error?.message) detail = String(problem.error.message);
          else if (problem.detail) detail = String(problem.detail);
        } catch (_) { /* Keep the status-only failure. */ }
        throw new Error(detail);
      }
      const data = await response.json();
      if (
        !data || data.schema_version !== "fomo-flow.v1" || !Array.isArray(data.items)
        || !data.coverage || !data.provenance || !data.query || !data.sources
        || data.query.source !== filters.source
      ) throw new Error("public flow response is malformed");
      state.data = data;
      state.cursor = cursor;
      state.history = history;
      render(data);
      updateLocation();
      setRequest("SNAPSHOT", `${data.items.length} observations · refresh 15s`);
    } catch (error) {
      if (error.name === "AbortError") return;
      elements.errorMessage.textContent = String(error.message || error);
      elements.errorPanel.hidden = false;
      setRequest("ERROR", state.data ? "Last confirmed snapshot remains visible." : "No flow page is available.", "error");
      elements.footer.textContent = state.data ? "STALE SNAPSHOT · REFRESH FAILED" : "UPSTREAM FLOW UNAVAILABLE";
      if (state.data) {
        elements.newer.disabled = state.history.length === 0;
        elements.older.disabled = !state.data.next_cursor;
      }
    } finally {
      if (state.controller === controller) state.controller = null;
      elements.form.querySelector("button").disabled = false;
    }
  }

  function resetAndLoad() {
    load(null, []);
  }

  function syncSourceControls() {
    const rhtrenches = elements.source.value === "rhtrenches";
    if (rhtrenches) {
      elements.chain.value = "robinhood";
      elements.verified.value = "false";
    }
    elements.chain.disabled = rhtrenches;
    elements.verified.disabled = rhtrenches;
  }

  elements.form.addEventListener("submit", (event) => {
    event.preventDefault();
    resetAndLoad();
  });
  elements.source.addEventListener("change", () => {
    syncSourceControls();
    renderSourcePresentation(elements.source.value);
    if (state.data && state.data.query?.source !== elements.source.value) {
      state.data = null;
      state.cursor = null;
      state.history = [];
      elements.results.hidden = true;
      elements.warning.hidden = true;
      elements.footer.textContent = "SOURCE CHANGED · NO PAGE LOADED";
    }
    resetAndLoad();
  });
  elements.chain.addEventListener("change", resetAndLoad);
  elements.verified.addEventListener("change", resetAndLoad);
  elements.limit.addEventListener("change", resetAndLoad);
  elements.retry.addEventListener("click", () => load(state.cursor, state.history));
  elements.older.addEventListener("click", () => {
    if (!state.data?.next_cursor) return;
    load(state.data.next_cursor, [...state.history, state.cursor]);
  });
  elements.newer.addEventListener("click", () => {
    if (!state.history.length) return;
    load(state.history[state.history.length - 1], state.history.slice(0, -1));
  });

  function updateClock() {
    const now = new Date();
    elements.clock.textContent = `${now.toISOString().slice(11, 19)} UTC`;
    elements.clock.dateTime = now.toISOString();
  }
  updateClock();
  window.setInterval(updateClock, 1000);
  window.setInterval(() => {
    if (!document.hidden && state.cursor === null && state.controller === null) load(null, []);
  }, 15_000);

  const initial = new URLSearchParams(window.location.search);
  const initialSource = initial.get("source");
  const initialChain = initial.get("chain");
  const initialVerified = initial.get("verified");
  const initialLimit = initial.get("limit");
  if (["apollo", "rhtrenches"].includes(initialSource)) elements.source.value = initialSource;
  if (["solana", "base", "robinhood"].includes(initialChain)) elements.chain.value = initialChain;
  if (["true", "false"].includes(initialVerified)) elements.verified.value = initialVerified;
  if (["25", "50", "100"].includes(initialLimit)) elements.limit.value = initialLimit;
  syncSourceControls();
  renderSourcePresentation(elements.source.value);
  resetAndLoad();
})();
