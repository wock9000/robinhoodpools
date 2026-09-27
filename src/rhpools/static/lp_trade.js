(function () {
  "use strict";
  const CHAIN_ID = 4663;
  const NATIVE = "0x0000000000000000000000000000000000000000";
  const WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";
  const USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168";
  const CURRENCIES = [[NATIVE, "ETH"], [WETH, "WETH"], [USDG, "USDG"]];
  const SLIPPAGE_PRESETS = [50, 100, 300];
  const ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;
  const EXPLORER = "https://robinhoodchain.blockscout.com/tx/";
  const REQUOTE_MS = 10000;
  const REFUSALS = {
    no_route: "no route on supported pools for this pair",
    insufficient_balance: "balance too low for this amount",
    impact_over_limit: "price impact above the 15% limit; lower the amount",
    unmodeled_fee: "this token takes a transfer fee the ticket cannot price; refused",
    allowlist_mismatch: "trading paused: a pinned contract's code changed",
    trading_disabled: "trading is not enabled",
  };

  const dialog = document.getElementById("trade-dialog");
  const body = document.getElementById("trade-body");
  if (!dialog || !body) return;
  const walletOut = document.getElementById("trade-dialog-wallet");
  const state = {
    token: null, side: "buy", currency: NATIVE, amount: "", slippage: 100,
    meta: {}, results: [], quote: null, phase: "idle", note: null, keepNote: false, hash: null, fill: null, txStatus: null, seq: 0,
  };
  const refs = {};
  let quoteTimer = null;
  let searchTimer = null;
  let requoteTimer = null;

  function el(tag, attrs, children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value == null || value === false) continue;
      if (key === "class") node.className = value;
      else if (key === "text") node.textContent = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value === true ? "" : value);
    }
    for (const child of children || []) if (child != null) node.append(child);
    return node;
  }

  function gate() {
    return window.rhpGate ? window.rhpGate.snapshot() : { wallet: null, chain: null, me: null };
  }

  function short(address) {
    return address ? address.slice(0, 6) + "…" + address.slice(-4) : "";
  }

  function toRaw(text, decimals) {
    const match = /^\s*(\d*)(?:\.(\d*))?\s*$/.exec(String(text));
    if (!match || (!match[1] && !match[2])) return null;
    const frac = match[2] || "";
    if (frac.length > decimals) return null;
    return BigInt((match[1] || "0") + frac.padEnd(decimals, "0"));
  }

  function fromRaw(raw, decimals, places) {
    const value = BigInt(raw);
    const scale = 10n ** BigInt(decimals);
    const whole = value / scale;
    const frac = (value % scale).toString().padStart(decimals, "0");
    const shown = places == null ? (whole >= 1000n ? 2 : whole >= 1n ? 4 : 6) : places;
    const trimmed = frac.slice(0, shown).replace(/0+$/, "");
    const grouped = whole.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    if (whole === 0n && value > 0n && !/[1-9]/.test(trimmed)) return "<0." + "0".repeat(Math.max(0, shown - 1)) + "1";
    return trimmed ? grouped + "." + trimmed : grouped;
  }

  function symbolOf(address) {
    const key = String(address || "").toLowerCase();
    const known = CURRENCIES.find(([addr]) => addr === key);
    if (known) return known[1];
    return (state.meta[key] && state.meta[key].symbol) || short(key);
  }

  function decimalsOf(address) {
    const key = String(address || "").toLowerCase();
    if (key === NATIVE || key === WETH) return 18;
    if (key === USDG) return 6;
    return state.meta[key] ? state.meta[key].decimals : null;
  }

  function currencyIn() { return state.side === "buy" ? state.currency : state.token; }
  function currencyOut() { return state.side === "buy" ? state.token : state.currency; }

  function amountRaw() {
    const decimals = decimalsOf(currencyIn());
    if (decimals == null || !state.amount) return null;
    const raw = toRaw(state.amount, decimals);
    return raw && raw > 0n ? raw : null;
  }

  async function api(path, payload) {
    const response = await fetch(path, payload === undefined
      ? { credentials: "same-origin", headers: { Accept: "application/json" } }
      : { method: "POST", credentials: "same-origin", headers: { Accept: "application/json", "Content-Type": "application/json" }, body: JSON.stringify(payload) });
    let data = null;
    try { data = await response.json(); } catch (_) { data = null; }
    if (!response.ok) throw { status: response.status, data: data || {} };
    return data;
  }

  function entitled() {
    const me = gate().me;
    return Boolean(me && me.signed_in && (me.features || []).includes("trade"));
  }

  function walletError(error) {
    const code = error && Number(error.code);
    const text = String((error && (error.message || error.reason)) || "");
    if (code === 4001 || /reject|denied|cancel/i.test(text)) return "request rejected in wallet";
    if (error && error.data && (error.data.error || error.data.refusal)) return REFUSALS[error.data.refusal] || error.data.error || error.data.refusal;
    return "wallet error";
  }

  async function loadBalances() {
    if (!entitled()) return;
    const currencies = [...new Set([state.token, NATIVE, WETH, USDG].filter(Boolean))].join(",");
    try {
      const data = await api("/api/tx/balances?feature=trade&currencies=" + currencies);
      for (const [address, meta] of Object.entries(data.balances || {})) state.meta[address] = meta;
    } catch (_) {
      return;
    }
    const meta = state.token && state.meta[state.token];
    if (meta && meta.symbol && document.activeElement !== refs.token) refs.token.value = meta.symbol + "  " + short(state.token);
    renderForm();
    if (!settled()) scheduleQuote(0);
  }

  function clearQuote() {
    state.quote = null;
    clearInterval(requoteTimer);
    requoteTimer = null;
  }

  function scheduleQuote(delay) {
    clearTimeout(quoteTimer);
    quoteTimer = setTimeout(requestQuote, delay == null ? 350 : delay);
  }

  function busy() {
    return ["approving", "permit", "preparing", "confirming", "pending"].includes(state.phase);
  }

  function settled() {
    return state.phase === "confirmed" || state.phase === "failed";
  }

  async function requestQuote() {
    if (busy() || settled() || !dialog.open) return;
    const raw = amountRaw();
    if (!entitled() || !state.token || raw == null || (state.txStatus && !state.txStatus.enabled)) {
      clearQuote();
      if (!busy()) state.phase = "idle";
      return render();
    }
    const seq = ++state.seq;
    if (!state.quote) state.phase = "quoting";
    renderCta();
    try {
      const quote = await api("/api/tx/quote", {
        kind: "swap", side: state.side, token: state.token, quote_currency: state.currency,
        amount_in: raw.toString(), slippage_bps: state.slippage,
      });
      if (seq !== state.seq || busy() || settled()) return;
      state.quote = quote;
      state.phase = "quoted";
      if (!state.keepNote) state.note = null;
      state.keepNote = false;
      if (!requoteTimer) requoteTimer = setInterval(() => { if (state.phase === "quoted" && !document.hidden) scheduleQuote(0); }, REQUOTE_MS);
    } catch (error) {
      if (seq !== state.seq) return;
      clearQuote();
      if (error.status === 422) {
        state.phase = "refused";
        state.note = REFUSALS[error.data.refusal] || error.data.detail || error.data.refusal;
      } else if (error.status === 401 || error.status === 403) {
        state.phase = "idle";
        if (window.rhpGate) window.rhpGate.refresh();
      } else {
        state.phase = "idle";
        state.note = (error.data && error.data.error) || "quote unavailable; retrying";
        scheduleQuote(3000);
      }
    }
    render();
  }

  async function waitWalletReceipt(hash) {
    const until = Date.now() + 180000;
    while (Date.now() < until) {
      const receipt = await window.ethereum.request({ method: "eth_getTransactionReceipt", params: [hash] });
      if (receipt) {
        if (receipt.status !== "0x1") throw { message: "approval failed on chain" };
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 800));
    }
    throw { message: "approval not confirmed after 3 minutes" };
  }

  async function execute() {
    let quote = state.quote;
    if (!quote) return;
    const wallet = gate().wallet;
    state.note = null;
    try {
      for (const step of quote.steps.filter((item) => item.kind === "approve")) {
        state.phase = "approving";
        render();
        const hash = await window.ethereum.request({ method: "eth_sendTransaction", params: [step.tx] });
        await waitWalletReceipt(hash);
      }
      if (quote.expires_at - Date.now() / 1000 < 20 || quote.steps.some((item) => item.kind === "approve")) {
        state.phase = "idle";
        state.quote = null;
        await requestQuote();
        quote = state.quote;
        if (!quote) return;
      }
      let permit = null;
      const permitStep = quote.steps.find((item) => item.kind === "permit");
      if (permitStep) {
        state.phase = "permit";
        render();
        permit = await window.ethereum.request({ method: "eth_signTypedData_v4", params: [wallet, JSON.stringify(permitStep.typed_data)] });
      }
      state.phase = "preparing";
      render();
      const prepared = await api("/api/tx/prepare", { quote_id: quote.quote_id, permit_signature: permit });
      state.phase = "confirming";
      render();
      const tx = prepared.transaction;
      state.hash = await window.ethereum.request({ method: "eth_sendTransaction", params: [{ from: tx.from, to: tx.to, data: tx.data, value: tx.value, gas: tx.gas }] });
      state.phase = "pending";
      clearInterval(requoteTimer);
      requoteTimer = null;
      render();
      await trackFill(state.hash);
    } catch (error) {
      state.note = error && error.status ? walletError({ data: error.data }) : walletError(error);
      state.phase = state.hash ? "failed" : "idle";
      state.keepNote = !state.hash;
      if (!state.hash) {
        state.quote = null;
        scheduleQuote(0);
      }
      render();
    }
  }

  async function trackFill(hash) {
    const until = Date.now() + 180000;
    while (Date.now() < until) {
      try {
        const fill = await api("/api/tx/receipt?feature=trade&hash=" + hash);
        if (fill.status === "confirmed" || fill.status === "failed") {
          state.fill = fill;
          state.phase = fill.status;
          if (fill.status === "failed") state.note = "reverted on chain: price moved past min received, or the quote expired";
          render();
          loadBalances();
          return;
        }
      } catch (_) {
        // node briefly unaware of the transaction; keep polling
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    state.phase = "failed";
    state.note = "no receipt after 3 minutes; check the explorer";
    render();
  }

  function reset() {
    state.phase = "idle";
    state.hash = null;
    state.fill = null;
    state.note = null;
    state.amount = "";
    clearQuote();
    render();
  }

  function segment(options, current, onpick) {
    return el("div", { class: "gate-row", role: "group" }, options.map(([value, label]) =>
      el("button", { type: "button", class: "seg", "aria-pressed": String(value === current), text: label, onclick: () => onpick(value) })));
  }

  function pickToken(address, label) {
    state.token = address.toLowerCase();
    state.results = [];
    if (label) state.meta[state.token] = { ...(state.meta[state.token] || {}), symbol: label };
    clearQuote();
    refs.token.value = label ? label + "  " + short(state.token) : state.token;
    renderForm();
    loadBalances();
  }

  function onTokenInput() {
    const text = refs.token.value.trim();
    clearTimeout(searchTimer);
    if (ADDRESS_RE.test(text)) return pickToken(text, null);
    if (text.length < 2) {
      state.results = [];
      return renderResults();
    }
    searchTimer = setTimeout(async () => {
      try {
        const data = await api("/api/lp/search?q=" + encodeURIComponent(text));
        const tokens = (data.rows || []).filter((row) => row.kind === "token").slice(0, 6);
        state.results = tokens.length ? tokens : null;
      } catch (_) {
        state.results = [];
      }
      renderResults();
    }, 250);
  }

  function renderResults() {
    if (state.results === null) return refs.results.replaceChildren(el("li", { class: "note", text: "no matching token; paste its 0x address" }));
    refs.results.replaceChildren(...(state.results.length ? state.results.map((row) =>
      el("li", {}, [el("button", { type: "button", onclick: () => pickToken(row.id, row.label) }, [
        el("span", { text: row.label + "  " }), el("span", { class: "note", text: short(row.id) }),
      ])])) : []));
  }

  function balanceLine(address) {
    const meta = state.meta[address];
    if (!meta) return el("span", { class: "note", text: entitled() ? "" : "sign in as a holder to see balances" });
    const decimals = meta.decimals;
    let max = BigInt(meta.balance);
    if (address === NATIVE) max = max > 2n * 10n ** 15n ? max - 2n * 10n ** 15n : 0n;
    return el("span", { class: "note" }, [
      el("span", { text: "bal " + fromRaw(meta.balance, decimals) + " " + symbolOf(address) + " · " }),
      el("button", { type: "button", class: "link", text: "max", onclick: () => {
        state.amount = fromRaw(max, decimals, decimals).replace(/,/g, "");
        refs.amount.value = state.amount;
        clearQuote();
        scheduleQuote(0);
      } }),
    ]);
  }

  function renderForm() {
    const payLabel = state.side === "buy" ? "pay with" : "receive";
    refs.side.replaceWith(refs.side = segment([["buy", "BUY"], ["sell", "SELL"]], state.side, (value) => {
      state.side = value;
      state.amount = "";
      refs.amount.value = "";
      clearQuote();
      renderForm();
      render();
    }));
    refs.currencyLabel.textContent = payLabel;
    refs.currency.replaceWith(refs.currency = segment(CURRENCIES, state.currency, (value) => {
      state.currency = value;
      if (state.side === "buy") { state.amount = ""; refs.amount.value = ""; }
      clearQuote();
      renderForm();
      scheduleQuote(0);
    }));
    refs.amountUnit.textContent = currencyIn() ? symbolOf(currencyIn()) : "";
    refs.balance.replaceChildren(currencyIn() ? balanceLine(currencyIn()) : "");
    const custom = !SLIPPAGE_PRESETS.includes(state.slippage);
    refs.slippage.replaceWith(refs.slippage = el("div", { class: "gate-row" }, [
      ...SLIPPAGE_PRESETS.map((bps) => el("button", { type: "button", class: "seg", "aria-pressed": String(bps === state.slippage), text: bps / 100 + "%", onclick: () => {
        state.slippage = bps;
        clearQuote();
        renderForm();
        scheduleQuote(0);
      } })),
      el("input", { type: "text", inputmode: "decimal", "aria-label": "custom slippage percent", placeholder: "custom %", value: custom ? String(state.slippage / 100) : null, style: "min-width:8ch;width:8ch", onchange: (event) => {
        const bps = Math.round(Number(event.target.value) * 100);
        if (bps >= 1 && bps <= 5000) {
          state.slippage = bps;
          clearQuote();
          renderForm();
          scheduleQuote(0);
        }
      } }),
    ]));
    renderResults();
  }

  function buildForm() {
    refs.token = el("input", { type: "text", class: "trade-token", placeholder: "symbol or 0x token address", autocomplete: "off", spellcheck: "false", "aria-label": "token", oninput: onTokenInput });
    refs.results = el("ul", { class: "trade-results" });
    refs.side = el("div");
    refs.currencyLabel = el("label", { text: "pay with" });
    refs.currency = el("div");
    refs.amount = el("input", { type: "text", class: "trade-amount", inputmode: "decimal", placeholder: "0.0", autocomplete: "off", "aria-label": "amount", oninput: (event) => {
      state.amount = event.target.value;
      clearQuote();
      state.phase = "idle";
      renderCta();
      scheduleQuote();
    } });
    refs.amountUnit = el("span", { class: "note" });
    refs.balance = el("div");
    refs.slippage = el("div");
    refs.quote = el("section", { class: "gate-section trade-quote" });
    refs.cta = el("button", { type: "submit", class: "trade-cta" });
    refs.status = el("p", { class: "trade-status note", "aria-live": "polite" });
    body.replaceChildren(
      el("section", { class: "gate-section" }, [
        el("div", { class: "trade-form" }, [
          el("label", { text: "token" }), el("div", {}, [el("div", { class: "gate-row" }, [refs.token]), refs.results]),
          el("label", { text: "side" }), refs.side,
          refs.currencyLabel, refs.currency,
          el("label", { text: "amount" }), el("div", {}, [el("div", { class: "gate-row" }, [refs.amount, refs.amountUnit]), refs.balance]),
          el("label", { text: "slippage" }), refs.slippage,
        ]),
      ]),
      refs.quote,
      el("section", { class: "gate-section" }, [refs.cta, refs.status]),
    );
    renderForm();
  }

  function routeText(quote) {
    const parts = [symbolOf(quote.hops[0].currency_in)];
    for (const hop of quote.hops) {
      const pons = hop.hook_fee_bps || hop.creator_tax_bps;
      parts.push(hop.venue + (pons ? " pons" : " " + (hop.fee_ppm / 10000) + "%"));
      parts.push(symbolOf(hop.currency_out));
    }
    return parts.join(" → ");
  }

  function feeRow(label, rate, fee) {
    if (!fee) return null;
    const decimals = decimalsOf(fee.currency);
    return el("tr", {}, [
      el("th", { text: label }), el("td", { class: "dim", text: rate }),
      el("td", { class: "numeric", text: decimals == null ? fee.amount : fromRaw(fee.amount, decimals) + " " + symbolOf(fee.currency) }),
    ]);
  }

  function renderQuote() {
    const quote = state.quote;
    if (!quote || state.phase === "confirmed") {
      refs.quote.replaceChildren();
      refs.quote.hidden = true;
      return;
    }
    refs.quote.hidden = false;
    const amounts = quote.amounts;
    const inDec = decimalsOf(currencyIn());
    const outDec = decimalsOf(currencyOut());
    const pons = quote.hops.find((hop) => hop.hook_fee_bps || hop.creator_tax_bps);
    const impact = amounts.impact_bps;
    const impactClass = impact == null ? "dim" : impact >= 1000 ? "impact-bad" : impact >= 300 ? "impact-warn" : "";
    refs.quote.replaceChildren(el("table", { class: "gate-table" }, [el("tbody", {}, [
      el("tr", {}, [el("th", { text: "route" }), el("td", { class: "trade-route", colspan: "2", text: routeText(quote) })]),
      el("tr", {}, [el("th", { text: "you pay" }), el("td", {}), el("td", { class: "numeric", text: fromRaw(amounts.amount_in, inDec) + " " + symbolOf(currencyIn()) })]),
      el("tr", {}, [el("th", { text: "you get" }), el("td", {}), el("td", { class: "numeric", text: fromRaw(amounts.net_out, outDec) + " " + symbolOf(currencyOut()) })]),
      el("tr", {}, [el("th", { text: "min received" }), el("td", { class: "dim", text: state.slippage / 100 + "% slippage" }), el("td", { class: "numeric", text: fromRaw(amounts.min_out, outDec) + " " + symbolOf(currencyOut()) })]),
      pons ? feeRow("pons fee", pons.hook_fee_bps / 100 + "%", amounts.hook_fee) : null,
      pons ? feeRow("creator tax", pons.creator_tax_bps / 100 + "%", amounts.creator_tax) : null,
      feeRow("rhpools fee", "0.75%", amounts.rhpools_fee),
      el("tr", {}, [el("th", { text: "price impact" }), el("td", {}), el("td", { class: "numeric " + impactClass, text: impact == null ? "—" : (impact / 100).toFixed(2) + "%" })]),
      el("tr", {}, [el("th", { text: "quote" }), el("td", { class: "dim", text: "block " + quote.block.number }), el("td", { class: "numeric dim" }, [refs.expiry = el("span")])]),
    ])]));
    tick();
  }

  function tick() {
    if (!refs.expiry || !state.quote) return;
    const left = Math.max(0, Math.round(state.quote.expires_at - Date.now() / 1000));
    refs.expiry.textContent = "expires in " + left + " s";
    if (left <= 3 && state.phase === "quoted") renderCta();
  }

  function cta() {
    const g = gate();
    const me = g.me || {};
    if (!window.ethereum) return { label: "no browser wallet detected", disabled: true };
    if (!g.wallet) return { label: "connect wallet", action: () => window.rhpGate.connect() };
    if (Number(g.chain) !== CHAIN_ID) return { label: "switch to chain 4663", action: () => window.rhpGate.switchChain() };
    if (!me.signed_in) return { label: "sign in", action: () => window.rhpGate.signIn() };
    if (me.state === "unset") return { label: "token not launched yet", disabled: true };
    if (!(me.features || []).includes("trade")) {
      const policy = me.policy || {};
      const need = policy.threshold ? fromRaw(policy.threshold.trade, policy.decimals || 18, 2) : "the threshold";
      return { label: "hold ≥ " + need + " tokens to trade", action: () => window.rhpGate.open() };
    }
    if (state.txStatus && !state.txStatus.enabled) return { label: state.txStatus.reason || "trading is not enabled", disabled: true };
    const busyLabels = {
      approving: "approve " + symbolOf(currencyIn()) + " in wallet (once per token)…",
      permit: "sign permit in wallet…", preparing: "checking the exact transaction…",
      confirming: "confirm " + state.side + " in wallet…", pending: "pending on chain…",
    };
    if (busyLabels[state.phase]) return { label: busyLabels[state.phase], disabled: true };
    if (state.phase === "confirmed" || state.phase === "failed") return { label: "new trade", action: reset };
    if (!state.token) return { label: "pick a token", disabled: true };
    if (amountRaw() == null) return { label: "enter an amount", disabled: true };
    if (state.phase === "quoting") return { label: "quoting…", disabled: true };
    if (state.phase === "refused") return { label: "not tradable", disabled: true };
    if (!state.quote) return { label: "quoting…", disabled: true };
    if (state.quote.expires_at - Date.now() / 1000 <= 3) return { label: "refreshing quote…", disabled: true };
    return { label: state.side + " " + symbolOf(state.token), action: execute };
  }

  function renderCta() {
    const next = cta();
    refs.cta.textContent = next.label;
    refs.cta.disabled = Boolean(next.disabled);
    refs.cta.onclick = next.action ? (event) => { event.preventDefault(); next.action(); } : (event) => event.preventDefault();
    const parts = [];
    if (state.phase === "refused" && state.note) parts.push(el("span", { class: "trade-refusal", text: state.note }));
    else if (state.phase === "confirmed" && state.fill && state.fill.amounts) {
      const amounts = state.fill.amounts;
      const fee = amounts.rhpools_fee;
      parts.push(el("span", { class: "good", text: "received " + fromRaw(amounts.net_out, decimalsOf(currencyOut())) + " " + symbolOf(currencyOut()) +
        (fee ? " · fee " + fromRaw(fee.amount, decimalsOf(fee.currency)) + " " + symbolOf(fee.currency) : "") + " · " }));
    } else if (state.note) parts.push(el("span", { class: state.phase === "failed" ? "bad" : "note", text: state.note + (state.hash ? " · " : "") }));
    if (state.hash) parts.push(el("a", { href: EXPLORER + state.hash, target: "_blank", rel: "noopener noreferrer", text: "tx " + short(state.hash) }));
    refs.status.replaceChildren(...parts);
  }

  function render() {
    const g = gate();
    walletOut.textContent = g.wallet ? short(g.wallet) : "";
    renderQuote();
    renderCta();
  }

  async function open(options) {
    if (!dialog.open) dialog.showModal();
    if (options && options.token && ADDRESS_RE.test(options.token)) pickToken(options.token, options.label || null);
    if (!state.txStatus) {
      try { state.txStatus = await api("/api/tx/status"); } catch (_) { state.txStatus = null; }
    }
    render();
    if (!options || !options.token) refs.token.focus();
    loadBalances();
  }

  function close() {
    clearTimeout(quoteTimer);
    if (!busy()) clearQuote();
    if (state.phase === "confirmed" || state.phase === "failed") reset();
  }

  buildForm();
  render();
  body.addEventListener("submit", (event) => {
    event.preventDefault();
    if (!refs.cta.disabled && refs.cta.onclick) refs.cta.onclick(event);
  });
  document.getElementById("trade-dialog-close").addEventListener("click", () => dialog.close());
  dialog.addEventListener("close", close);
  document.addEventListener("rhp:gate", () => {
    if (!dialog.open) return;
    render();
    if (entitled() && state.token && !state.meta[state.token]) loadBalances();
  });
  const nav = document.getElementById("trade-nav");
  if (nav) nav.addEventListener("click", (event) => { event.preventDefault(); open(); });
  document.addEventListener("keydown", (event) => {
    const target = event.target;
    const typing = target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName));
    if (event.key === "t" && !typing && !event.metaKey && !event.ctrlKey && !event.altKey && !dialog.open && !document.querySelector("dialog[open]")) {
      event.preventDefault();
      open();
    }
  });
  setInterval(() => { if (dialog.open) tick(); }, 1000);
  window.rhpTrade = { open: open };
})();
