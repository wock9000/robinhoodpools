(() => {
  "use strict";

  const ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;
  const ROBINSCAN = "https://robinscan.io";
  const byId = (id) => document.getElementById(id);
  const elements = {
    form: byId("owner-form"), owner: byId("owner-input"), window: byId("window-select"),
    formMessage: byId("form-message"), requestState: byId("request-state"), clock: byId("clock"),
    notice: byId("notice"), errorPanel: byId("error-panel"), errorMessage: byId("error-message"),
    retry: byId("retry-button"), results: byId("results"), ownerAddress: byId("owner-address"),
    badges: byId("identity-badges"), coverageGrid: byId("coverage-grid"),
    coverageNote: byId("coverage-note"), allocationGrid: byId("allocation-grid"),
    allocationNote: byId("allocation-note"), allocationBody: byId("allocation-body"),
    mixGrid: byId("mix-grid"), configurationBody: byId("configuration-body"),
    latest: byId("latest-activity"), activityBody: byId("activity-body"),
    limitations: byId("limitations-list"), footer: byId("footer-state"),
    apiLinks: [byId("api-link"), byId("api-link-bottom")],
  };
  const state = { controller: null, loadedOwner: null, data: null };
  const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
  const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
  const money = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  function node(tag, text, className) {
    const item = document.createElement(tag);
    if (text != null) item.textContent = String(text);
    if (className) item.className = className;
    return item;
  }

  function setRequest(label, className = "") {
    elements.requestState.textContent = label;
    elements.requestState.className = `request-state${className ? ` ${className}` : ""}`;
  }

  function formatCount(value) {
    if (value == null || value === "") return "—";
    const number = Number(value);
    return Number.isFinite(number) ? integer.format(number) : "—";
  }

  function formatUsdg(value) {
    if (value == null || value === "") return "UNKNOWN";
    const number = Number(value);
    return Number.isFinite(number) ? `${money.format(number)} USDG` : "UNKNOWN";
  }

  function formatPct(value) {
    if (value == null || value === "") return "—";
    const number = Number(value);
    return Number.isFinite(number) ? `${decimal.format(number)}%` : "—";
  }

  function formatTime(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number <= 0) return "—";
    return new Date(number * 1000).toLocaleString(undefined, {
      year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit",
      timeZoneName: "short",
    });
  }

  function formatAge(seconds) {
    const number = Number(seconds);
    if (!Number.isFinite(number) || number < 0) return "—";
    if (number < 60) return `${Math.round(number)}s`;
    if (number < 3600) return `${Math.round(number / 60)}m`;
    if (number < 86400) return `${decimal.format(number / 3600)}h`;
    return `${decimal.format(number / 86400)}d`;
  }

  function compact(value, start = 8, end = 6) {
    const text = String(value || "");
    return text.length > start + end + 2 ? `${text.slice(0, start)}…${text.slice(-end)}` : text || "—";
  }

  function cell(row, value, className = "") {
    const td = node("td", value, className);
    row.append(td);
    return td;
  }

  function linkCell(row, href, label) {
    const td = node("td");
    const link = node("a", label);
    link.href = href;
    td.append(link);
    row.append(td);
  }

  function emptyRow(body, columns, message) {
    const row = node("tr", null, "empty-row");
    const td = cell(row, message);
    td.colSpan = columns;
    body.replaceChildren(row);
  }

  function metric(label, value, className = "") {
    const wrapper = node("div");
    wrapper.append(node("dt", label), node("dd", value, className));
    return wrapper;
  }

  function badge(label, className = "") {
    return node("span", label, `badge${className ? ` ${className}` : ""}`);
  }

  function renderIdentity(data) {
    elements.ownerAddress.textContent = data.owner;
    const identity = data.coverage.identity || {};
    const badges = [];
    if (identity.beneficial_owner_observed) badges.push(badge("beneficial owner observed", "good"));
    if (identity.custody_observed) badges.push(badge("custody observed", "warn"));
    if (!identity.beneficial_owner_observed && !identity.custody_observed) {
      if (identity.current_activity_match) {
        badges.push(badge(`provisional ${String(identity.current_activity_match).replaceAll("_", " ")}`, "warn"));
      } else {
        badges.push(badge("no indexed identity", "bad"));
      }
    }
    badges.push(badge(String(identity.basis || "unobserved").replaceAll("_", " ")));
    elements.badges.replaceChildren(...badges);
  }

  function renderCoverage(data) {
    const coverage = data.coverage || {};
    const complete = coverage.window_complete === true;
    const returned = Number(coverage.returned_positions || 0);
    const truncated = coverage.possible_position_truncation === true;
    const valuation = coverage.valuation || {};
    elements.coverageGrid.replaceChildren(
      metric("index state", String(coverage.state || "unknown").toUpperCase(), coverage.state === "live" ? "good" : "warn"),
      metric("window", complete ? `${String(data.window).toUpperCase()} COMPLETE` : `${String(data.window).toUpperCase()} PARTIAL`, complete ? "good" : "warn"),
      metric("positions returned", `${formatCount(returned)} · PAGE LIMIT ${formatCount(coverage.source_position_limit)}`, truncated ? "warn" : ""),
      metric("USDG valuation", `${formatCount(valuation.fully_valued_positions)} FULL · ${formatCount(valuation.partially_valued_positions)} PARTIAL · ${formatCount(valuation.unvalued_positions)} UNKNOWN`, valuation.partially_valued_positions || valuation.unvalued_positions ? "warn" : "good"),
    );
    const start = formatTime(coverage.history_from);
    const end = formatTime(coverage.history_to);
    elements.coverageNote.textContent = [
      `Indexed history ${start} → ${end}.`,
      `Valuation basis: ${valuation.basis || "unknown"}.`,
      truncated ? "The source return boundary was reached; every grouping below is returned-position scope, not a full-wallet claim." : "Groupings cover every position returned by the owner model.",
    ].join(" ");
  }

  function renderAllocation(data) {
    const allocation = data.allocation || {};
    elements.allocationGrid.replaceChildren(
      metric("current beneficial positions", formatCount(allocation.current_beneficial_positions)),
      metric("known principal", formatUsdg(allocation.known_principal_usdg), allocation.known_principal_usdg == null ? "warn" : "good"),
      metric("full / partial / unknown", `${formatCount(allocation.valued_positions)} / ${formatCount(allocation.partially_valued_positions)} / ${formatCount(allocation.unvalued_positions)}`, allocation.partially_valued_positions || allocation.unvalued_positions ? "warn" : "good"),
      metric("custody positions", formatCount(allocation.custody_positions_observed), allocation.custody_positions_observed ? "warn" : ""),
    );
    elements.allocationNote.textContent = allocation.custody_positions_observed
      ? "Custody-observed positions are excluded from owner allocation and value totals. Known-only shares do not allocate unknown principal."
      : "Known-only shares use block-pinned USDG-quote principal for returned current beneficial-owner positions. Unknown principal receives no implied zero value.";
    const rows = [];
    for (const pool of allocation.pools || []) {
      const row = node("tr");
      cell(row, pool.pair || compact(pool.pool_id));
      cell(row, String(pool.protocol || "—").toUpperCase());
      cell(row, formatCount(pool.position_count), "numeric");
      cell(row, formatUsdg(pool.known_principal_usdg), `numeric ${pool.known_principal_usdg == null ? "warn" : "good"}`);
      cell(row, formatPct(pool.share_of_known_principal_pct), "numeric");
      const incomplete = Number(pool.partially_valued_positions || 0) + Number(pool.unvalued_positions || 0);
      cell(row, incomplete ? `${formatCount(pool.partially_valued_positions)} PARTIAL · ${formatCount(pool.unvalued_positions)} UNKNOWN` : "COMPLETE", incomplete ? "warn" : "good");
      linkCell(row, pool.inspector || `/pool?id=${encodeURIComponent(pool.pool_id || "")}`, "OPEN");
      rows.push(row);
    }
    if (rows.length) elements.allocationBody.replaceChildren(...rows);
    else emptyRow(elements.allocationBody, 7, "NO CURRENT BENEFICIAL-OWNER ALLOCATION IN RETURNED INDEXED HISTORY");
  }

  function mixCard(title, lines) {
    const card = node("article", null, "mix-card");
    card.append(node("h3", title));
    for (const line of lines) {
      const paragraph = node("p");
      paragraph.append(node("strong", line.value), document.createTextNode(` ${line.label}`));
      card.append(paragraph);
    }
    return card;
  }

  function feeLabel(fee) {
    const mode = String(fee?.mode || "unknown").toUpperCase();
    if (mode === "DYNAMIC") {
      return fee.current_ppm != null ? `DYNAMIC · NOW ${fee.current_ppm} ppm` : "DYNAMIC · CURRENT UNKNOWN";
    }
    return fee?.configured_ppm != null ? `${fee.configured_ppm} ppm` : mode;
  }

  function renderConfigurations(data) {
    const config = data.configurations || {};
    const widths = config.range_width_ticks || {};
    const protocolLines = (config.protocol_mix || []).map((row) => ({
      value: formatCount(row.returned_positions),
      label: String(row.protocol || "unknown").toUpperCase(),
    }));
    const feeLines = (config.fee_mode_mix || []).map((row) => ({
      value: formatCount(row.returned_positions),
      label: `${String(row.mode || "unknown").toUpperCase()} · ${formatCount(row.pool_count)} POOLS`,
    }));
    elements.mixGrid.replaceChildren(
      mixCard("PROTOCOL MIX", protocolLines.length ? protocolLines : [
        { value: "0", label: "RETURNED POSITIONS" },
      ]),
      mixCard("FEE MODE MIX", feeLines.length ? feeLines : [
        { value: "0", label: "RETURNED POSITIONS" },
      ]),
      mixCard("RANGE WIDTH · TICKS", [
        { value: formatCount(widths.minimum), label: "MIN" },
        { value: formatCount(widths.median), label: "MEDIAN" },
        { value: formatCount(widths.maximum), label: "MAX" },
      ]),
    );

    const rows = [];
    for (const position of config.positions || []) {
      const row = node("tr");
      cell(row, position.pair || compact(position.pool_id));
      const match = String(position.identity_match || "unresolved").replaceAll("_", " ").toUpperCase();
      cell(row, match, position.identity_match === "custody" ? "identity-custody" : "");
      const status = `${position.current ? "CURRENT" : "HISTORICAL"} · ${String(position.status || "unknown").toUpperCase()}`;
      cell(row, status, position.current ? "good" : "dim");
      const range = position.range || {};
      cell(row, range.tick_lower == null || range.tick_upper == null ? "FULL / UNKNOWN" : `${range.tick_lower} → ${range.tick_upper}`);
      cell(row, formatCount(range.width_ticks), "numeric");
      cell(row, feeLabel(position.fee));
      cell(row, formatCount(position.lifecycle?.episodes_observed), "numeric");
      cell(row, formatTime(position.lifecycle?.first_opened_at));
      cell(row, formatTime(position.lifecycle?.last_event_at));
      linkCell(row, position.inspector || `/pool?id=${encodeURIComponent(position.pool_id || "")}`, "OPEN");
      rows.push(row);
    }
    if (rows.length) elements.configurationBody.replaceChildren(...rows);
    else emptyRow(elements.configurationBody, 10, "NO RETURNED POSITION CONFIGURATIONS FOR THIS IDENTITY AND WINDOW");
  }

  function renderActivity(data) {
    const activity = data.activity || {};
    const latest = activity.latest;
    if (latest) {
      elements.latest.replaceChildren(
        node("strong", `${String(latest.kind || "activity").toUpperCase()} · ${latest.pair || compact(latest.pool_id)}`),
        document.createTextNode(` · ${formatTime(latest.timestamp)} · ${String(latest.qualification || activity.qualification || "unknown").replaceAll("_", " ").toUpperCase()}`),
      );
      if (latest.tx_hash) {
        elements.latest.append(document.createTextNode(" · "));
        const tx = node("a", "TX");
        tx.href = `${ROBINSCAN}/tx/${encodeURIComponent(latest.tx_hash)}`;
        tx.target = "_blank";
        tx.rel = "noopener noreferrer";
        elements.latest.append(tx);
      }
    } else {
      elements.latest.textContent = "NO QUALIFIED CURRENT OR DURABLE ACTIVITY OBSERVATION";
    }
    const rows = [];
    for (const item of activity.recent_lifecycle || []) {
      const row = node("tr");
      cell(row, formatTime(item.timestamp));
      cell(row, String(item.kind || "—").toUpperCase(), item.kind === "opened" ? "good" : "");
      cell(row, item.pair || compact(item.pool_id));
      cell(row, String(item.identity_match || "unresolved").replaceAll("_", " ").toUpperCase(), item.identity_match === "custody" ? "identity-custody" : "");
      cell(row, item.duration_s == null ? "—" : formatAge(item.duration_s), "numeric");
      const id = cell(row, compact(item.position_key, 10, 8), "dim");
      id.title = item.position_key || "";
      rows.push(row);
    }
    if (rows.length) elements.activityBody.replaceChildren(...rows);
    else emptyRow(elements.activityBody, 6, "NO POSITION LIFECYCLE EVENTS IN RETURNED INDEXED HISTORY");
  }

  function renderLimitations(data) {
    elements.limitations.replaceChildren(...(data.limitations || []).map((text) => node("li", text)));
  }

  function render(data) {
    renderIdentity(data);
    renderCoverage(data);
    renderAllocation(data);
    renderConfigurations(data);
    renderActivity(data);
    renderLimitations(data);
    elements.results.hidden = false;
    const coverage = data.coverage || {};
    const indexGap = coverage.current_activity?.head != null && coverage.indexed_head != null
      ? Math.max(0, Number(coverage.current_activity.head) - Number(coverage.indexed_head)) : null;
    const warnings = [];
    if (!coverage.found) warnings.push("NO INDEXED OWNER OR CUSTODY HISTORY WAS FOUND FOR THIS ADDRESS AND WINDOW");
    if (coverage.state === "catching_up" || indexGap > 0) warnings.push(`DURABLE ACCOUNTING IS CATCHING UP${indexGap > 0 ? ` · ${formatCount(indexGap)} BLOCK GAP` : ""}`);
    if (!coverage.window_complete) warnings.push("REQUESTED HISTORY WINDOW IS NOT PROVEN COMPLETE");
    if (coverage.possible_position_truncation) warnings.push("POSITION RETURN BOUNDARY REACHED · GROUPS ARE PARTIAL");
    elements.notice.hidden = warnings.length === 0;
    elements.notice.textContent = warnings.join(" · ");
    const apiQuery = new URLSearchParams({ owner: data.owner, window: data.window });
    for (const link of elements.apiLinks) link.href = `/api/v1/research/owner?${apiQuery}`;
    elements.footer.textContent = `${String(data.window).toUpperCase()} · ${formatCount(coverage.returned_positions)} RETURNED POSITIONS · ${coverage.window_complete ? "WINDOW COVERED" : "PARTIAL COVERAGE"}`;
  }

  async function load(owner, selectedWindow, pushHistory = true) {
    const normalized = String(owner || "").trim().toLowerCase();
    if (!ADDRESS_RE.test(normalized)) {
      elements.formMessage.textContent = "INVALID OWNER · EXPECTED 0x + 40 HEX CHARACTERS";
      elements.formMessage.className = "form-message is-error";
      setRequest("INVALID", "is-error");
      elements.footer.textContent = state.data
        ? "LAST CONFIRMED RESPONSE UNCHANGED · INVALID ADDRESS"
        : "NO OWNER LOADED · INVALID ADDRESS";
      elements.owner.focus();
      return;
    }
    if (state.controller) state.controller.abort();
    const controller = new AbortController();
    state.controller = controller;
    const changingOwner = state.loadedOwner !== normalized;
    if (changingOwner) elements.results.hidden = true;
    elements.errorPanel.hidden = true;
    elements.formMessage.textContent = "LOADING CANONICAL OWNER MODEL";
    elements.formMessage.className = "form-message";
    elements.results.setAttribute("aria-busy", "true");
    setRequest("LOADING", "is-loading");
    const query = new URLSearchParams({ owner: normalized, window: selectedWindow });
    try {
      const response = await fetch(`/api/v1/research/owner?${query}`, {
        method: "GET", headers: { Accept: "application/json" }, cache: "no-store", signal: controller.signal,
      });
      if (!response.ok) {
        let detail = `HTTP ${response.status}`;
        try {
          const body = await response.json();
          if (typeof body.error === "string") detail = body.error;
          else if (body.error?.message) detail = body.error.message;
        } catch (_) { /* retain the status */ }
        throw new Error(detail);
      }
      const data = await response.json();
      if (!data || data.owner !== normalized || !data.coverage || !data.allocation || !data.configurations) {
        throw new Error("research response is malformed");
      }
      state.data = data;
      state.loadedOwner = normalized;
      elements.owner.value = normalized;
      elements.window.value = data.window;
      render(data);
      elements.formMessage.textContent = data.coverage.found ? "INDEXED OBSERVATIONS LOADED" : "NO INDEXED HISTORY IN THIS WINDOW";
      const live = data.coverage.state === "live" && data.coverage.window_complete;
      const label = !data.coverage.found ? "EMPTY" : live ? "LIVE" : "PARTIAL";
      setRequest(label, live ? "" : "is-loading");
      if (pushHistory) {
        const url = new URL(window.location.href);
        url.search = query.toString();
        window.history.pushState({ owner: normalized, window: data.window }, "", url);
      }
    } catch (error) {
      if (error.name === "AbortError") return;
      elements.errorMessage.textContent = String(error.message || error);
      elements.errorPanel.hidden = false;
      elements.formMessage.textContent = changingOwner ? "NO DATA LOADED" : "LAST CONFIRMED RESPONSE REMAINS VISIBLE";
      elements.formMessage.className = "form-message is-error";
      setRequest("ERROR", "is-error");
      elements.footer.textContent = changingOwner ? "REQUEST FAILED" : "STALE RESPONSE · REQUEST FAILED";
    } finally {
      if (state.controller === controller) state.controller = null;
      elements.results.removeAttribute("aria-busy");
    }
  }

  elements.form.addEventListener("submit", (event) => {
    event.preventDefault();
    load(elements.owner.value, elements.window.value);
  });
  elements.retry.addEventListener("click", () => load(elements.owner.value, elements.window.value, false));
  elements.window.addEventListener("change", () => {
    if (ADDRESS_RE.test(elements.owner.value.trim())) load(elements.owner.value, elements.window.value);
  });
  window.addEventListener("popstate", () => {
    const query = new URLSearchParams(window.location.search);
    const owner = query.get("owner") || "";
    const selectedWindow = query.get("window") || "30d";
    elements.owner.value = owner;
    if (["1h", "24h", "7d", "30d", "all"].includes(selectedWindow)) elements.window.value = selectedWindow;
    if (ADDRESS_RE.test(owner)) load(owner, elements.window.value, false);
    else {
      if (state.controller) state.controller.abort();
      state.loadedOwner = null;
      elements.results.hidden = true;
      elements.notice.hidden = true;
      elements.errorPanel.hidden = true;
      elements.formMessage.textContent = "ENTER A 20-BYTE HEX ADDRESS";
      elements.formMessage.className = "form-message";
      elements.footer.textContent = "NO OWNER LOADED";
      setRequest("READY");
    }
  });

  function updateClock() {
    const now = new Date();
    elements.clock.textContent = now.toISOString().slice(11, 19) + " UTC";
    elements.clock.dateTime = now.toISOString();
  }
  updateClock();
  window.setInterval(updateClock, 1000);

  const initial = new URLSearchParams(window.location.search);
  const initialOwner = initial.get("owner") || "";
  const initialWindow = initial.get("window") || "30d";
  elements.owner.value = initialOwner;
  if (["1h", "24h", "7d", "30d", "all"].includes(initialWindow)) elements.window.value = initialWindow;
  if (ADDRESS_RE.test(initialOwner)) load(initialOwner, elements.window.value, false);
  else if (initialOwner) {
    elements.formMessage.textContent = "INVALID OWNER · EXPECTED 0x + 40 HEX CHARACTERS";
    elements.formMessage.className = "form-message is-error";
    setRequest("INVALID", "is-error");
  }
})();
