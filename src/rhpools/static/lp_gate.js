(function () {
  "use strict";
  const CHAIN_ID = 4663;
  const CHAIN_HEX = "0x1237";
  const FEATURES = ["trade", "lp", "api", "flags"];
  const ADDRESS_RE = /^0x[0-9a-fA-F]{40}$/;
  const chip = document.getElementById("gate-chip");
  const dialog = document.getElementById("gate-dialog");
  if (!chip || !dialog) return;
  const chipText = chip.querySelector(".gate-chip-text");
  const sections = {
    status: document.getElementById("gate-status"),
    keys: document.getElementById("gate-keys"),
    policy: document.getElementById("gate-policy"),
  };
  const addressOut = document.getElementById("gate-dialog-address");
  const state = { wallet: null, chain: null, me: null, phase: "idle", error: null, secret: null, keys: null, policy: null };
  let ticker = null;

  function el(tag, attrs, children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value == null) continue;
      if (key === "class") node.className = value;
      else if (key === "text") node.textContent = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value);
    }
    for (const child of children || []) node.append(child);
    return node;
  }

  function short(address) {
    return address ? address.slice(0, 6) + "…" + address.slice(-4) : "";
  }

  function units(raw, decimals) {
    if (raw == null) return "?";
    const value = BigInt(raw);
    const base = 10n ** BigInt(decimals || 0);
    const whole = (value / base).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    if (!decimals) return whole;
    const frac = (value % base).toString().padStart(decimals, "0").slice(0, 2);
    return frac === "00" ? whole : whole + "." + frac;
  }

  function clock(seconds) {
    const total = Math.max(0, Math.floor(seconds));
    const m = Math.floor(total / 60);
    const s = total % 60;
    return (m < 10 ? "0" : "") + m + ":" + (s < 10 ? "0" : "") + s;
  }

  async function api(path, body, method) {
    const response = await fetch(path, {
      method: method || (body ? "POST" : "GET"),
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const gate = payload.gate || {};
      const error = new Error(payload.error || ("HTTP " + response.status));
      error.gate = gate;
      error.status = response.status;
      throw error;
    }
    return payload;
  }

  function graceLeft() {
    const me = state.me;
    if (!me || !me.signed_in) return null;
    const until = Object.values(me.grace_until || {});
    if (!until.length) return null;
    return Math.max(...until) - Date.now() / 1000;
  }

  function view() {
    const me = state.me;
    if (!window.ethereum) return { key: "no-wallet", text: "none", dim: true };
    if (state.phase === "signing") return { key: "signing", text: short(state.wallet) + " confirm in wallet…" };
    if (state.phase === "connecting") return { key: "connecting", text: "connecting…" };
    if (!state.wallet) return { key: "disconnected", text: "connect" };
    if (state.chain !== CHAIN_ID) return { key: "wrong-chain", text: "wrong chain " + (state.chain == null ? "?" : state.chain) + " · switch", cls: "is-below" };
    if (!me || !me.signed_in) return { key: "connected", text: short(state.wallet) + " sign in" };
    if (me.wallet.toLowerCase() !== state.wallet.toLowerCase()) return { key: "mismatch", text: short(state.wallet) + " ≠ session " + short(me.wallet) + " · sign in" };
    const owner = me.owner ? " owner" : "";
    if (me.state === "unset") return { key: "unset", text: short(me.wallet) + " gate not live" + owner, dim: !me.owner };
    if (me.state === "holder") return { key: "holder", text: short(me.wallet) + " holder" + owner, cls: "is-holder", features: me.features };
    if (me.state === "grace") return { key: "grace", text: short(me.wallet) + " grace " + clock(graceLeft() || 0) + owner, cls: "is-grace", features: me.features };
    const need = FEATURES.map((f) => BigInt(me.policy.threshold[f])).filter((n) => n > 0n).sort((a, b) => (a < b ? -1 : 1))[0];
    const have = me.holding ? me.holding.balance_raw : null;
    return {
      key: "below", cls: "is-below",
      text: short(me.wallet) + " need " + units(need == null ? "0" : need.toString(), me.policy.decimals) + " (have " + units(have, me.policy.decimals) + ")" + owner,
    };
  }

  function renderChip() {
    const current = view();
    chip.className = "gate-chip" + (current.dim ? " is-dim" : "") + (current.cls ? " " + current.cls : "");
    chip.dataset.state = current.key;
    chipText.replaceChildren(document.createTextNode(current.text + (current.features ? " " : "")));
    if (current.features) {
      for (const feature of FEATURES) {
        chipText.append(el("span", { class: "badge" + (current.features.includes(feature) ? "" : " off"), text: feature }));
      }
    }
    chip.title = state.error || "";
    if (current.key === "grace" && !ticker) ticker = setInterval(renderChip, 1000);
    if (current.key !== "grace" && ticker) { clearInterval(ticker); ticker = null; }
  }

  function statusSection() {
    const me = state.me;
    const out = [el("h3", { text: "WALLET" })];
    if (state.error) out.push(el("p", { class: "bad", text: state.error }));
    if (!window.ethereum) {
      out.push(el("p", { class: "note", text: "no browser wallet detected. sign-in needs an EIP-1193 wallet on chain 4663." }));
      return out;
    }
    if (!state.wallet) {
      out.push(el("p", { class: "note", text: "not connected." }));
      out.push(el("div", { class: "gate-row" }, [el("button", { type: "button", text: "connect", onclick: connect })]));
      return out;
    }
    if (state.chain !== CHAIN_ID) {
      out.push(el("p", { class: "bad", text: "wallet is on chain " + state.chain + "; chain 4663 required." }));
      out.push(el("div", { class: "gate-row" }, [el("button", { type: "button", text: "switch chain", onclick: switchChain })]));
      return out;
    }
    if (!me || !me.signed_in || me.wallet.toLowerCase() !== state.wallet.toLowerCase()) {
      out.push(el("p", { class: "note", text: (me && me.signed_in ? "session belongs to " + short(me.wallet) + ". " : "") + "sign a message to prove the wallet. no transaction, no fee." }));
      out.push(el("div", { class: "gate-row" }, [el("button", { type: "button", text: "sign in", onclick: signIn })]));
      return out;
    }
    const policy = me.policy;
    if (me.state === "unset") {
      out.push(el("p", { class: "note", text: "gate not live: no token set in the policy yet." }));
    } else {
      const have = me.holding ? units(me.holding.balance_raw, policy.decimals) : "unknown (oracle unavailable)";
      out.push(el("p", {}, [
        el("span", { class: "note", text: "state " }), el("span", { class: me.state === "holder" ? "good" : me.state === "grace" ? "warn" : "bad", text: me.state }),
        el("span", { class: "note", text: " · balance " }), document.createTextNode(have),
        el("span", { class: "note", text: me.holding ? " · block " + me.holding.block : "" }),
      ]));
      const rows = FEATURES.map((feature) => {
        const on = me.features.includes(feature);
        const until = me.grace_until && me.grace_until[feature];
        return el("tr", {}, [
          el("td", { text: feature }),
          el("td", { text: policy.threshold[feature] === "0" ? "any signed-in wallet" : "≥ " + units(policy.threshold[feature], policy.decimals) }),
          el("td", { class: on ? (until ? "warn" : "good") : "bad", text: on ? (until ? "grace " + clock(until - Date.now() / 1000) : "on") : "off" }),
        ]);
      });
      out.push(el("table", { class: "gate-table" }, [
        el("thead", {}, [el("tr", {}, [el("th", { text: "feature" }), el("th", { text: "threshold" }), el("th", { text: "state" })])]),
        el("tbody", {}, rows),
      ]));
      out.push(el("p", { class: "note", text: "grace " + policy.grace_s + " s after the last qualifying balance · policy v" + policy.version + " · token " + policy.token }));
    }
    out.push(el("div", { class: "gate-row" }, [
      el("span", { class: "note", text: "session " + me.key_id + " · expires " + new Date(me.expires_at * 1000).toISOString().slice(0, 16).replace("T", " ") }),
      el("button", { type: "button", text: "sign out", onclick: signOut }),
    ]));
    return out;
  }

  function keysSection() {
    const me = state.me;
    if (!me || !me.signed_in || !state.wallet || me.wallet.toLowerCase() !== state.wallet.toLowerCase()) return [];
    const out = [el("h3", { text: "API KEYS" })];
    if (!me.features.includes("api")) out.push(el("p", { class: "note", text: "api feature required to mint keys." }));
    if (state.secret) {
      out.push(el("p", { class: "warn", text: "new key " + state.secret.key_id + " (" + state.secret.label + "). shown once:" }));
      out.push(el("code", { class: "gate-secret", text: state.secret.secret }));
      out.push(el("div", { class: "gate-row" }, [
        el("button", { type: "button", text: "copy", onclick: () => navigator.clipboard && navigator.clipboard.writeText(state.secret.secret) }),
        el("button", { type: "button", text: "dismiss", onclick: () => { state.secret = null; renderDialog(); } }),
      ]));
    }
    const rows = (state.keys || []).filter((key) => !key.revoked_at).map((key) => el("tr", {}, [
      el("td", { text: key.key_id }),
      el("td", { class: "grow", text: key.label + (key.kind === "session" ? " (session)" : "") }),
      el("td", { text: new Date(key.expires_at * 1000).toISOString().slice(0, 10) }),
      el("td", { text: key.last_used_at ? new Date(key.last_used_at * 1000).toISOString().slice(0, 16).replace("T", " ") : "never" }),
      el("td", {}, [el("button", { type: "button", class: "danger", text: "revoke", onclick: () => revoke(key.key_id) })]),
    ]));
    out.push(el("table", { class: "gate-table" }, [
      el("thead", {}, [el("tr", {}, [el("th", { text: "id" }), el("th", { text: "label" }), el("th", { text: "expires" }), el("th", { text: "last used" }), el("th", { text: "" })])]),
      el("tbody", {}, rows.length ? rows : [el("tr", {}, [el("td", { class: "note", colspan: "5", text: "no live keys" })])]),
    ]));
    const label = el("input", { type: "text", placeholder: "label", maxlength: "64", value: "bot" });
    const ttl = el("input", { type: "number", min: "1", max: "365", value: "90", title: "days" });
    out.push(el("div", { class: "gate-row" }, [
      el("label", { text: "mint" }), label, el("label", { text: "days" }), ttl,
      el("button", { type: "button", text: "mint key", disabled: me.features.includes("api") ? null : "", onclick: () => mint(label.value, Number(ttl.value)) }),
    ]));
    out.push(el("p", { class: "note", text: "use: Authorization: Bearer <secret> on /api/v1/stream, /api/lp/*, /api/v1/*." }));
    return out;
  }

  function policySection() {
    const me = state.me;
    if (!me || !me.signed_in || !me.owner || !state.wallet || me.wallet.toLowerCase() !== state.wallet.toLowerCase()) return [];
    const policy = me.policy;
    const fields = {};
    const form = el("div", { class: "gate-form" });
    const add = (name, value, attrs) => {
      fields[name] = el("input", Object.assign({ type: "text", value: value, class: "wide" }, attrs || {}));
      form.append(el("label", { text: name }), fields[name]);
    };
    add("token", policy.token || "", { placeholder: "0x… (empty = unset)" });
    add("decimals", String(policy.decimals));
    for (const feature of FEATURES) add(feature, policy.threshold[feature], { title: "raw units" });
    add("grace_s", String(policy.grace_s));
    const out = [
      el("h3", { text: "POLICY (owner)" }),
      el("p", { class: "note", text: "current v" + policy.version + ". signing produces v" + (policy.version + 1) + " with EIP-712 (eth_signTypedData_v4); the server verifies the owner address and audits the change." }),
      form,
      el("div", { class: "gate-row" }, [el("button", { type: "button", text: "sign & apply", onclick: () => applyPolicy(fields) })]),
    ];
    return out;
  }

  function renderDialog() {
    addressOut.textContent = state.wallet ? state.wallet : "";
    sections.status.replaceChildren(...statusSection());
    sections.keys.replaceChildren(...keysSection());
    sections.policy.replaceChildren(...policySection());
    sections.keys.hidden = !sections.keys.childElementCount;
    sections.policy.hidden = !sections.policy.childElementCount;
  }

  function render() {
    renderChip();
    if (dialog.open) renderDialog();
  }

  async function refreshMe() {
    try {
      state.me = await api("/api/gate/me");
      state.error = null;
    } catch (error) {
      state.error = error.message;
    }
    render();
  }

  async function refreshKeys() {
    try {
      state.keys = (await api("/api/gate/keys")).keys;
    } catch (error) {
      state.keys = [];
    }
  }

  function normalizeChain(value) {
    return typeof value === "string" ? parseInt(value, 16) : Number(value);
  }

  async function readWallet(prompt) {
    const accounts = await window.ethereum.request({ method: prompt ? "eth_requestAccounts" : "eth_accounts" });
    state.wallet = Array.isArray(accounts) && ADDRESS_RE.test(accounts[0] || "") ? accounts[0] : null;
    state.chain = normalizeChain(await window.ethereum.request({ method: "eth_chainId" }));
  }

  async function connect() {
    state.phase = "connecting";
    state.error = null;
    render();
    try {
      await readWallet(true);
    } catch (error) {
      state.error = error && error.message ? error.message : "wallet refused";
    }
    state.phase = "idle";
    render();
  }

  async function switchChain() {
    try {
      await window.ethereum.request({ method: "wallet_switchEthereumChain", params: [{ chainId: CHAIN_HEX }] });
      state.chain = normalizeChain(await window.ethereum.request({ method: "eth_chainId" }));
      state.error = null;
    } catch (error) {
      state.error = error && Number(error.code) === 4902 ? "chain 4663 is not configured in this wallet" : "switch to chain 4663 was not approved";
    }
    render();
  }

  function siweMessage(nonce, address) {
    const domain = location.host;
    return domain + " wants you to sign in with your Ethereum account:\n" + address + "\n\n" + nonce.statement +
      "\n\nURI: " + location.origin + "/\nVersion: 1\nChain ID: " + CHAIN_ID + "\nNonce: " + nonce.nonce +
      "\nIssued At: " + nonce.issued_at + "\nExpiration Time: " + nonce.expires_at;
  }

  function hex(text) {
    return "0x" + Array.from(new TextEncoder().encode(text), (b) => b.toString(16).padStart(2, "0")).join("");
  }

  async function signIn() {
    if (!state.wallet || state.chain !== CHAIN_ID) return;
    state.phase = "signing";
    state.error = null;
    render();
    try {
      const nonce = await api("/api/gate/nonce?wallet=" + state.wallet);
      const message = siweMessage(nonce, nonce.address);
      const signature = await window.ethereum.request({ method: "personal_sign", params: [hex(message), state.wallet] });
      state.me = await api("/api/gate/session", { message: message, signature: signature, label: "browser" });
      state.secret = null;
      await refreshKeys();
    } catch (error) {
      state.error = error.gate && error.gate.reason ? "sign-in refused: " + error.gate.reason : (error && error.message) || "sign-in failed";
    }
    state.phase = "idle";
    render();
  }

  async function signOut() {
    try {
      await api("/api/gate/logout", {});
    } catch (error) {
      state.error = error.message;
    }
    state.me = { signed_in: false };
    state.keys = null;
    state.secret = null;
    render();
  }

  async function mint(label, days) {
    try {
      state.secret = await api("/api/gate/keys", { op: "mint", label: label || "key", ttl_s: Math.max(1, Math.min(365, days || 90)) * 86400 });
      state.error = null;
    } catch (error) {
      state.error = error.message;
    }
    await refreshKeys();
    render();
  }

  async function revoke(keyId) {
    try {
      await api("/api/gate/keys", { op: "revoke", key_id: keyId });
    } catch (error) {
      state.error = error.message;
    }
    if (state.me && state.me.key_id === keyId) return signOut();
    await refreshKeys();
    render();
  }

  async function applyPolicy(fields) {
    const me = state.me;
    const token = fields.token.value.trim();
    const policy = {
      version: me.policy.version + 1,
      token: token && token !== "0x0000000000000000000000000000000000000000" ? token : null,
      decimals: Number(fields.decimals.value),
      threshold: {},
      grace_s: Number(fields.grace_s.value),
      issued_at: Math.floor(Date.now() / 1000),
    };
    for (const feature of FEATURES) policy.threshold[feature] = fields[feature].value.trim() || "0";
    const status = await api("/api/gate/policy").catch(() => null);
    if (!status) { state.error = "policy template unavailable"; return render(); }
    const typed = {
      types: status.typed_data.types,
      primaryType: "GatePolicy",
      domain: status.typed_data.domain,
      message: {
        version: String(policy.version), token: policy.token || "0x0000000000000000000000000000000000000000",
        decimals: String(policy.decimals), trade: policy.threshold.trade, lp: policy.threshold.lp,
        api: policy.threshold.api, flags: policy.threshold.flags, graceSeconds: String(policy.grace_s), issuedAt: String(policy.issued_at),
      },
    };
    state.phase = "signing";
    render();
    try {
      const signature = await window.ethereum.request({ method: "eth_signTypedData_v4", params: [state.wallet, JSON.stringify(typed)] });
      await api("/api/gate/policy", { policy: policy, signature: signature });
      state.error = null;
      await refreshMe();
    } catch (error) {
      state.error = error.gate && error.gate.reason ? "policy refused: " + error.gate.reason : (error && error.message) || "policy signing failed";
    }
    state.phase = "idle";
    render();
  }

  async function onChip() {
    const current = view();
    if (current.key === "disconnected") return connect();
    if (current.key === "wrong-chain") return switchChain();
    if (current.key === "connected" || current.key === "mismatch") return signIn();
    if (current.key === "signing" || current.key === "connecting") return;
    if (state.me && state.me.signed_in) await refreshKeys();
    renderDialog();
    dialog.showModal();
  }

  chip.addEventListener("click", onChip);
  document.getElementById("gate-dialog-close").addEventListener("click", () => dialog.close());
  dialog.addEventListener("click", (event) => { if (event.target === dialog) dialog.close(); });

  async function boot() {
    chip.hidden = false;
    if (window.ethereum) {
      try { await readWallet(false); } catch (error) { state.error = error && error.message; }
      window.ethereum.on && window.ethereum.on("accountsChanged", () => { readWallet(false).then(render, render); });
      window.ethereum.on && window.ethereum.on("chainChanged", () => { readWallet(false).then(render, render); });
    }
    await refreshMe();
    setInterval(() => { if (state.me && state.me.signed_in) refreshMe(); }, 30000);
  }

  boot();
})();
