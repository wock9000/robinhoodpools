const PRIMARY_ROOT = "https://rhpools.lol/";
const PRIMARY_STATUS = "https://rhpools.lol/api/lp/status";
const SECONDARY_ROOT = "https://robinhoodpools.lol/";

const TIMEOUT_MS = 5_000;
const ROOT_BODY_LIMIT = 128 * 1024;
const STATUS_BODY_LIMIT = 64 * 1024;
const MAX_RECORD_AGE_SECONDS = 5 * 60;
const TERMINAL_MARKER = "<title>Robinhood Pools / Chain 4663</title>";
const EXPECTED_CHAIN_ID = 4663;
const LATEST_KEY = "latest";

class ProbeFailure extends Error {
  constructor(category) {
    super(category);
    this.category = category;
  }
}

function elapsedMilliseconds(startedAt) {
  return Math.max(0, Date.now() - startedAt);
}

function checkResult(path, ok, status, startedAt, error) {
  const result = {
    path,
    ok,
    status,
    latency_ms: elapsedMilliseconds(startedAt),
  };
  if (error !== undefined) {
    result.error = error;
  }
  return result;
}

function safeError(error, signal) {
  if (signal.aborted) {
    return "timeout";
  }
  if (error instanceof ProbeFailure) {
    return error.category;
  }
  return "network";
}

async function discardBody(response) {
  if (response.body === null) {
    return;
  }
  try {
    await response.body.cancel();
  } catch {
    // Body cancellation does not change the probe result.
  }
}

async function readBoundedText(response, limit) {
  const contentLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > limit) {
    await discardBody(response);
    throw new ProbeFailure("body_too_large");
  }
  if (response.body === null) {
    return "";
  }

  const reader = response.body.getReader();
  const chunks = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      length += value.byteLength;
      if (length > limit) {
        await reader.cancel();
        throw new ProbeFailure("body_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

async function probeRoot() {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let status = null;

  try {
    const response = await fetch(PRIMARY_ROOT, {
      headers: { Accept: "text/html" },
      redirect: "manual",
      signal: controller.signal,
    });
    status = response.status;
    if (status !== 200) {
      await discardBody(response);
      return checkResult("/", false, status, startedAt, "http_status");
    }

    const body = await readBoundedText(response, ROOT_BODY_LIMIT);
    if (!body.includes(TERMINAL_MARKER)) {
      return checkResult("/", false, status, startedAt, "content_mismatch");
    }
    return checkResult("/", true, status, startedAt);
  } catch (error) {
    return checkResult(
      "/",
      false,
      status,
      startedAt,
      safeError(error, controller.signal),
    );
  } finally {
    clearTimeout(timeout);
  }
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

async function probeStatus() {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let status = null;

  try {
    const response = await fetch(PRIMARY_STATUS, {
      headers: { Accept: "application/json" },
      redirect: "manual",
      signal: controller.signal,
    });
    status = response.status;
    if (status !== 200) {
      await discardBody(response);
      return {
        check: checkResult(
          "/api/lp/status",
          false,
          status,
          startedAt,
          "http_status",
        ),
      };
    }

    const body = await readBoundedText(response, STATUS_BODY_LIMIT);
    let payload;
    try {
      payload = JSON.parse(body);
    } catch {
      throw new ProbeFailure("invalid_json");
    }
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
      throw new ProbeFailure("invalid_payload");
    }
    if (!isFiniteNumber(payload.chain_id) || payload.chain_id !== EXPECTED_CHAIN_ID) {
      throw new ProbeFailure("invalid_chain");
    }
    if (!isFiniteNumber(payload.head)) {
      throw new ProbeFailure("invalid_head");
    }
    if (!isFiniteNumber(payload.indexed_head)) {
      throw new ProbeFailure("invalid_indexed_head");
    }

    const lagBlocks = isFiniteNumber(payload.lag_blocks)
      ? payload.lag_blocks
      : Math.max(0, payload.head - payload.indexed_head);
    return {
      check: checkResult("/api/lp/status", true, status, startedAt),
      head: payload.head,
      indexed_head: payload.indexed_head,
      lag_blocks: lagBlocks,
    };
  } catch (error) {
    return {
      check: checkResult(
        "/api/lp/status",
        false,
        status,
        startedAt,
        safeError(error, controller.signal),
      ),
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function probeSecondary() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
  let status = null;

  try {
    const response = await fetch(SECONDARY_ROOT, {
      headers: { Accept: "text/html" },
      redirect: "manual",
      signal: controller.signal,
    });
    status = response.status;
    await discardBody(response);
    if (response.ok) {
      return { host: "robinhoodpools.lol", ok: true, status };
    }
    return {
      host: "robinhoodpools.lol",
      ok: false,
      status,
      error: "http_status",
    };
  } catch (error) {
    return {
      host: "robinhoodpools.lol",
      ok: false,
      status,
      error: safeError(error, controller.signal),
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function recordHealth(env) {
  const [root, api, secondary] = await Promise.all([
    probeRoot(),
    probeStatus(),
    probeSecondary(),
  ]);
  const record = {
    checked_at: Math.floor(Date.now() / 1000),
    primary: {
      ok: root.ok && api.check.ok,
      checks: [root, api.check],
      head: api.head ?? null,
      indexed_head: api.indexed_head ?? null,
      lag_blocks: api.lag_blocks ?? null,
    },
    secondary,
  };
  await env.HEALTH.put(LATEST_KEY, JSON.stringify(record));
}

const PUBLIC_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Cache-Control": "no-store",
  "Content-Type": "application/json; charset=utf-8",
};

function jsonResponse(body, status, headOnly, extraHeaders = {}) {
  return new Response(headOnly ? null : JSON.stringify(body), {
    status,
    headers: { ...PUBLIC_HEADERS, ...extraHeaders },
  });
}

function isUsableRecord(record) {
  return (
    record !== null &&
    typeof record === "object" &&
    Number.isFinite(record.checked_at) &&
    record.primary !== null &&
    typeof record.primary === "object" &&
    typeof record.primary.ok === "boolean"
  );
}

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(recordHealth(env));
  },

  async fetch(request, env) {
    const headOnly = request.method === "HEAD";
    if (request.method !== "GET" && !headOnly) {
      return jsonResponse(
        { error: "method_not_allowed" },
        405,
        false,
        { Allow: "GET, HEAD" },
      );
    }

    let stored;
    try {
      stored = await env.HEALTH.get(LATEST_KEY);
    } catch {
      return jsonResponse({ error: "health_record_unavailable" }, 503, headOnly);
    }
    if (stored === null) {
      return jsonResponse({ error: "health_record_unavailable" }, 503, headOnly);
    }

    let record;
    try {
      record = JSON.parse(stored);
    } catch {
      return jsonResponse({ error: "health_record_unavailable" }, 503, headOnly);
    }
    if (!isUsableRecord(record)) {
      return jsonResponse({ error: "health_record_unavailable" }, 503, headOnly);
    }

    const now = Math.floor(Date.now() / 1000);
    const stale = record.checked_at > now || now - record.checked_at > MAX_RECORD_AGE_SECONDS;
    const status = stale || !record.primary.ok ? 503 : 200;
    return jsonResponse(record, status, headOnly);
  },
};
