"use strict";
/*
 * StrangeQ management SPA.
 *
 * Vanilla JavaScript only: no framework, no build step, no CDN, no external
 * origin. It talks to the management API over same-origin /api/* at runtime
 * and never imports the API package. All untrusted strings are rendered with
 * textContent; the innerHTML setter is never used with non-literal data.
 */
(function () {
  /* Runtime configuration. The server injects a CSP-safe
   * <meta name="sq-config" content="{...}"> into the shell (an inline
   * <script> is forbidden by script-src 'self'); it takes precedence over the
   * window.__SQ_CONFIG__ seam the headless-browser tests use. */
  var CONFIG = {};
  var winCfg = window.__SQ_CONFIG__;
  if (winCfg && typeof winCfg === 'object') {
    for (var wk in winCfg) {
      if (Object.prototype.hasOwnProperty.call(winCfg, wk)) { CONFIG[wk] = winCfg[wk]; }
    }
  }
  var meta = document.querySelector('meta[name="sq-config"]');
  if (meta) {
    try {
      var injected = JSON.parse(meta.getAttribute('content') || '{}');
      if (injected && typeof injected === 'object') {
        for (var mk in injected) {
          if (Object.prototype.hasOwnProperty.call(injected, mk)) { CONFIG[mk] = injected[mk]; }
        }
      }
    } catch (e) { /* a malformed meta must never break the SPA */ }
  }
  var API = CONFIG.apiBase || '/api';
  /* Only an absolute http(s) URL may become an href. A hostile or malformed
   * config value (javascript:, data:, vbscript:, an off-origin
   * scheme-relative URL) is rejected so it can never be promoted into a link. */
  var METRICS_URL = /^https?:[/][/]/i.test(CONFIG.metricsUrl || '') ? CONFIG.metricsUrl : '';
  /* DURABILITY scopes the WAL full-flush alarm to the crash model the metric's
   * contract is written for. Under durability: flush every confirm is a full
   * drive-cache flush, so amqp_wal_full_flush_total{site="group_commit"} is
   * expected to climb and must not raise the alarm. barrier (the default, and
   * the fallback for an absent or unknown value) keeps the strict alarm, so a
   * stale shell can only over-alarm, never silently suppress. */
  var DURABILITY = CONFIG.durability === "flush" ? "flush" : "barrier";
  /* METRICS_API is the same-origin, auth-gated data source for the Metrics
   * screen: the management listener's JSON {text} wrapper around the process
   * registry scrape. connect-src 'self' covers it, so the CSP is untouched.
   * METRICS_URL above stays the RAW Prometheus URL (a different listener),
   * injected CSP-safely as <meta name="sq-config">; it is rendered only as a
   * plain new-tab link and is NEVER fetched. */
  var METRICS_API = API + "/metrics";
  /* nowMs is the deterministic clock seam for the metrics rate derivation: a
   * headless test installs CONFIG.nowMs as a counter so rates are exact with no
   * sleeps. Only state.metrics uses it; the refresh chrome keeps Date.now(). */
  function nowMs() { return typeof CONFIG.nowMs === "function" ? CONFIG.nowMs() : Date.now(); }
  var DEFAULT_REFRESH_MS = CONFIG.refreshInterval || 5000;
  var S1_REFRESH_MS = CONFIG.s1RefreshInterval || 2000;
  var MAX_BACKOFF_MS = 30000;
  /* Browsers defer a credentials-carrying fetch/XHR whose 401 carries
   * WWW-Authenticate: Basic, waiting for the native auth prompt. Bounding the
   * wait keeps the SPA usable: on timeout the probe is treated as
   * unauthenticated and the login form renders. */
  var REQUEST_TIMEOUT_MS = CONFIG.requestTimeoutMs || 20000;
  var AUTH_TIMEOUT_MS = CONFIG.authTimeoutMs || 4000;

  var state = {
    principal: null,
    csrf: null,
    authed: false,
    version: null,
    capabilities: {},
    capabilitiesLoaded: false,
    route: null,
    currentLoad: null,
    etags: {},
    cache: {},
    displayed: null,
    degraded: null,
    health: null,
    samples: {},
    metrics: { filter: "", samples: {}, model: null, tiles: null, text: "", at: 0 },
    refresh: {
      timer: 0,
      base: DEFAULT_REFRESH_MS,
      delay: DEFAULT_REFRESH_MS,
      errCount: 0,
      paused: false,
      lastUpdated: 0,
      stale: false
    }
  };

  /* ---------------------------------------------------------------- DOM ---- */

  function appendChildren(parent, children) {
    if (children === null || children === undefined || children === false) return;
    if (Array.isArray(children)) {
      children.forEach(function (c) { appendChildren(parent, c); });
      return;
    }
    if (children instanceof Node) { parent.appendChild(children); return; }
    parent.appendChild(document.createTextNode(String(children)));
  }

  function h(tag, attrs, children) {
    var el = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        var v = attrs[k];
        if (v === null || v === undefined || v === false) return;
        if (k === "class") { el.className = v; }
        else if (k === "text") { el.textContent = String(v); }
        else if (k === "dataset") { Object.keys(v).forEach(function (d) { el.dataset[d] = v[d]; }); }
        else if (k.slice(0, 2) === "on" && typeof v === "function") { el.addEventListener(k.slice(2), v); }
        else if (v === true) { el.setAttribute(k, ""); }
        else { el.setAttribute(k, String(v)); }
      });
    }
    appendChildren(el, children);
    return el;
  }

  /* Escapes a string for the rare static template. Untrusted values are never
   * passed through here; they go through textContent. */
  function esc(s) {
    return String(s === null || s === undefined ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  var DASH = "\u2014";

  function isNull(v) { return v === null || v === undefined; }
  function val(v) { return isNull(v) ? DASH : String(v); }

  function dash(title) {
    return h("span", { class: "null", text: DASH, title: title || "unknown" });
  }

  function stat(v, explain) {
    if (isNull(v)) return dash(explain || "unknown");
    return h("span", { text: String(v) });
  }

  function num(v) {
    if (isNull(v)) return dash("unknown");
    if (typeof v === "number") return h("span", { text: v.toLocaleString() });
    return h("span", { text: String(v) });
  }

  function fmtTime(v) {
    if (isNull(v) || v === "") return dash("unknown");
    var d = new Date(v);
    if (isNaN(d.getTime())) return dash("unknown");
    return h("span", { text: d.toLocaleString(), title: d.toISOString() });
  }

  function fmtDuration(seconds) {
    if (isNull(seconds)) return dash("unknown");
    var s = Number(seconds);
    if (isNaN(s)) return dash("unknown");
    var out;
    if (s < 60) out = s + "s";
    else if (s < 3600) out = Math.floor(s / 60) + "m" + (s % 60) + "s";
    else if (s < 86400) out = Math.floor(s / 3600) + "h" + Math.floor((s % 3600) / 60) + "m";
    else out = Math.floor(s / 86400) + "d" + Math.floor((s % 86400) / 3600) + "h";
    return h("span", { text: out, title: String(s) + " seconds" });
  }

  function stateCell(value) {
    var s = isNull(value) ? "unknown" : String(value);
    return h("span", { class: "state", dataset: { state: s }, text: s });
  }

  function badge(text, kind) {
    return h("span", { class: "badge" + (kind ? " " + kind : ""), text: text });
  }

  /* capabilityStatus derives a distinct status from the capability value: a
   * boolean, an empty list (honest-empty), or an explicit 501 marker (F7). */
  function capabilityStatus(value) {
    if (value === true) return { label: "supported", kind: "" };
    if (value === false) return { label: "blocked", kind: "warn" };
    if (Array.isArray(value)) return value.length ? { label: "supported", kind: "" } : { label: "empty ([])", kind: "warn" };
    if (value === 501 || value === "501") return { label: "not implemented (501)", kind: "warn" };
    if (isNull(value)) return { label: "unknown", kind: "warn" };
    return { label: "unavailable", kind: "warn" };
  }

  function boolCell(v, trueText, falseText) {
    if (isNull(v)) return dash();
    return h("span", { text: v ? (trueText || "true") : (falseText || "false") });
  }

  /* secureCell renders a connection's TLS transport state for the connections
   * list. The badge carries a text label ("SSL") so the state is never conveyed
   * by colour alone; a non-TLS connection shows the page's em-dash convention,
   * and an absent field is reported as unknown rather than assumed insecure. */
  function secureCell(r) {
    if (r.secure === true) {
      return h("span", { class: "badge ssl-badge", title: "TLS transport", text: "SSL" });
    }
    if (r.secure === false) return dash("not TLS");
    return dash("unknown");
  }

  /* authMechanismCell renders the SASL mechanism that authenticated the
   * connection. It is truthful only when a mechanism actually authenticated, so
   * an empty/absent value renders the em-dash convention — never a guess. */
  function authMechanismCell(v) {
    if (isNull(v) || v === "") return dash("no SASL mechanism");
    return h("span", { text: String(v) });
  }

  function link(text, href) {
    return h("a", { href: href, text: text });
  }

  function copyText(text, label) {
    var btn = h("button", { type: "button", class: "copy-btn", text: label || "Copy" });
    btn.addEventListener("click", function () {
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(
            function () { toast("Copied", "ok"); },
            function () { toast("Copy failed", "error"); });
        } else {
          toast("Clipboard unavailable", "error");
        }
      } catch (e) { toast("Copy failed", "error"); }
    });
    return btn;
  }

  function copyLinkButton() {
    var btn = copyText(location.href, "Copy link");
    btn.id = "copy-link-btn";
    btn.title = "Copy a deep link to this page";
    return btn;
  }

  /* --------------------------------------------------------------- API ----- */

  function api(path, opts) {
    opts = opts || {};
    var method = opts.method || "GET";
    var headers = { Accept: "application/json" };
    var hasRaw = opts.raw !== undefined && opts.raw !== null;
    var hasBody = hasRaw || (opts.body !== undefined && opts.body !== null);
    if (hasBody) headers["Content-Type"] = "application/json";
    /* Caller-supplied headers (for example the X-Reason on a graceful
     * connection.close). Applied first, then any case-insensitive
     * x-csrf-token is dropped before the canonical token is set, so a
     * caller-supplied value can never displace or fake it — even when there
     * is no session token yet. */
    if (opts.headers && typeof opts.headers === "object") {
      Object.keys(opts.headers).forEach(function (hk) {
        var hv = opts.headers[hk];
        if (hv === null || hv === undefined || hv === "") return;
        headers[hk] = String(hv);
      });
    }
    Object.keys(headers).forEach(function (hk) {
      if (hk.toLowerCase() === "x-csrf-token") delete headers[hk];
    });
    if (method !== "GET" && method !== "HEAD" && state.csrf) headers["X-CSRF-Token"] = state.csrf;
    if (opts.ifNoneMatch) headers["If-None-Match"] = opts.ifNoneMatch;
    var init = { method: method, headers: headers, credentials: "same-origin", cache: "no-store" };
    if (hasRaw) init.body = String(opts.raw);
    else if (hasBody) init.body = JSON.stringify(opts.body);
    var timeout = opts.timeoutMs !== undefined ? opts.timeoutMs : REQUEST_TIMEOUT_MS;
    var timer = 0;
    if (timeout > 0 && typeof AbortController === "function") {
      var controller = new AbortController();
      init.signal = controller.signal;
      timer = setTimeout(function () { controller.abort(); }, timeout);
    }
    function done() { if (timer) { clearTimeout(timer); timer = 0; } }
    return fetch(path, init).then(function (res) {
      done();
      var meta = {
        status: res.status,
        ok: res.ok,
        etag: res.headers.get("ETag"),
        location: res.headers.get("Location"),
        purged: res.headers.get("X-Sq-Messages-Purged"),
        deleted: res.headers.get("X-Sq-Messages-Deleted")
      };
      if (res.status === 401) handleUnauthorized();
      if (res.status === 304) { meta.data = null; return meta; }
      return res.text().then(function (t) {
        var data = null;
        if (t) { try { data = JSON.parse(t); } catch (e) { data = null; } }
        meta.data = data;
        return meta;
      });
    }, function (err) {
      done();
      throw err;
    });
  }

  function handleUnauthorized() {
    state.csrf = null;
    state.principal = null;
    state.authed = false;
    state.currentLoad = null;
    clearTimeout(state.refresh.timer);
    if (document.getElementById("login-form")) return;
    renderLogin();
  }

  /* The session response returns the CSRF token in memory, but a reload leaves
   * only cookies. Spec 01 §5.3 deliberately mirrors the token into the
   * non-HttpOnly SQ_CSRF cookie so a reloaded SPA can resume mutating without a
   * new login; SQ_SESSION stays HttpOnly and is never read here. */
  function readCSRFCookie() {
    var parts = String(document.cookie || "").split(";");
    for (var i = 0; i < parts.length; i++) {
      var kv = parts[i].trim();
      if (kv.indexOf("SQ_CSRF=") === 0) {
        try { return decodeURIComponent(kv.slice("SQ_CSRF=".length)); } catch (e) { return kv.slice("SQ_CSRF=".length); }
      }
    }
    return null;
  }

  function itemsOf(data) {
    if (Array.isArray(data)) return data;
    if (data && Array.isArray(data.items)) return data.items;
    return [];
  }

  function qs(params) {
    var sp = new URLSearchParams();
    Object.keys(params).forEach(function (k) {
      var v = params[k];
      if (v === null || v === undefined || v === "") return;
      sp.set(k, String(v));
    });
    var s = sp.toString();
    return s ? "?" + s : "";
  }

  function vhostPath(name) {
    return encodeURIComponent(name === undefined || name === null ? "/" : name);
  }

  function queuePath(vhost, name) {
    return API + "/queues/" + vhostPath(vhost) + "/" + encodeURIComponent(name);
  }

  function exchangePath(vhost, name) {
    return API + "/exchanges/" + vhostPath(vhost) + "/" + encodeURIComponent(name);
  }

  /* bindingsPath is the canonical binding resource: an exchange source, a
   * queue or exchange destination, and (for a delete) the synthesised
   * properties_key. dtype is "q" or "e" (FR-API-091/093). */
  function bindingsPath(vhost, source, dtype, destination, props) {
    var p = API + "/bindings/" + vhostPath(vhost) + "/e/" + encodeURIComponent(source) +
      "/" + dtype + "/" + encodeURIComponent(destination);
    if (props !== undefined && props !== null) p += "/" + encodeURIComponent(props);
    return p;
  }

  function bindingDtype(destinationType) {
    return destinationType === "exchange" ? "e" : "q";
  }

  function bindingRowKey(b) {
    return String(b.source) + "|" + String(b.destination_type) + "|" + String(b.destination) + "|" + String(b.properties_key);
  }

  /* ---------------------------------------------------------- feedback ----- */

  function toast(message, kind) {
    var root = document.getElementById("toast-root");
    if (!root) return;
    var node = h("div", { class: "toast" + (kind ? " " + kind : ""), text: message });
    root.appendChild(node);
    setTimeout(function () { if (node.parentNode) node.parentNode.removeChild(node); }, 6000);
  }

  /* asyncJobNotice renders the 202 "accepted as a background job" outcome. A
   * 202 means the work is still running: it must never show a count or the
   * word "complete". The Location header names the job endpoint that will
   * carry the result. */
  function asyncJobNotice(location) {
    var body = [
      h("h2", { text: "Accepted as a background job" }),
      h("p", { text: "This operation is running in the background; its result is not ready yet." })
    ];
    if (location) {
      body.push(h("p", null, [link("View job status", location)]));
    }
    return h("div", { class: "notice job-accepted", role: "status" }, body);
  }

  /* noticeToast shows a non-expiring notice in the toast root. A 202 carries
   * a link a plain text toast cannot, and it must stay reachable while the
   * background job runs. */
  function noticeToast(node) {
    var root = document.getElementById("toast-root");
    if (!root) return;
    root.appendChild(node);
  }

  function errorDetail(data) {
    if (!data) return "request failed";
    if (typeof data === "string") return data;
    var parts = [];
    if (data.error) parts.push(data.error);
    if (data.reason) parts.push(data.reason);
    return parts.length ? parts.join(": ") : "request failed";
  }

  function errorState(data) {
    return h("div", { class: "notice permission", role: "alert" }, [
      h("h2", { text: "Request failed" }),
      h("p", { class: "error", text: errorDetail(data) })
    ]);
  }

  function permissionDenied(data) {
    return h("div", { class: "notice permission", role: "alert" }, [
      h("h2", { text: "Permission denied" }),
      h("p", { text: "Your principal does not hold the capability required for this screen." }),
      h("p", { class: "reason", text: errorDetail(data) })
    ]);
  }

  function capabilityNotice(data, title) {
    return h("div", { class: "notice", role: "note" }, [
      h("h2", { text: title || "Not implemented by StrangeQ" }),
      h("p", { text: "This capability is not implemented; the API reported a typed not-implemented response." }),
      h("p", { class: "reason", text: errorDetail(data) })
    ]);
  }

  function notFound(backHref, backLabel) {
    return h("div", { class: "notice empty", role: "note" }, [
      h("h2", { text: "Not found" }),
      h("p", { text: "The requested resource does not exist." }),
      backHref ? h("p", null, [link(backLabel || "Back to list", backHref)]) : null
    ]);
  }

  function emptyState(message, actionLabel, onAction) {
    return h("div", { class: "notice empty" }, [
      h("p", { text: message }),
      actionLabel ? h("button", { type: "button", text: actionLabel, onclick: onAction }) : null
    ]);
  }

  /* ------------------------------------------------ shared list loader ----- */

  function loadList(container, key, path, opts) {
    opts = opts || {};
    return api(path, { ifNoneMatch: state.etags[key] }).then(function (r) {
      if (r.status === 403) { state.displayed = key; container.replaceChildren(permissionDenied(r.data)); return; }
      if (r.status === 501) { state.displayed = key; container.replaceChildren(capabilityNotice(r.data, opts.notImplementedTitle)); return; }
      if (r.status === 404) { state.displayed = key; container.replaceChildren(notFound(opts.backHref, opts.backLabel)); return; }
      if (r.status >= 400 && r.status !== 304) {
        if (state.displayed !== key) container.replaceChildren(errorState(r.data));
        var err = new Error("request failed: " + r.status);
        err.status = r.status;
        throw err;
      }
      if (r.status === 304) {
        /* A periodic refresh must not re-render (no flicker), but a fresh
         * navigation that cleared the region still has to paint from cache. */
        if (state.displayed === key) return;
        var cached = state.cache[key];
        if (cached === undefined) return renderListFromNetwork(container, key, path, opts);
        state.displayed = key;
        container.replaceChildren(opts.render({ data: cached, status: 200 }));
        return;
      }
      if (r.etag) state.etags[key] = r.etag;
      state.cache[key] = r.data;
      state.displayed = key;
      container.replaceChildren(opts.render(r));
    });
  }

  function renderListFromNetwork(container, key, path, opts) {
    return api(path).then(function (r) {
      if (r.status >= 400) { container.replaceChildren(errorState(r.data)); state.displayed = key; return; }
      if (r.etag) state.etags[key] = r.etag;
      state.cache[key] = r.data;
      state.displayed = key;
      container.replaceChildren(opts.render(r));
    });
  }

  /* invalidateAll drops every cached list/ETag so the next render re-fetches.
   * Called after a successful mutation so a refreshed view never shows
   * pre-mutation numbers. */
  function invalidateAll() {
    state.etags = {};
    state.cache = {};
  }

  /* ------------------------------------------------------------- tables ---- */

  function renderTable(columns, rows, opts) {
    opts = opts || {};
    var sortKeys = opts.sortKeys || null;
    var head = h("tr", null, columns.map(function (c) {
      var th = h("th", { role: "columnheader", scope: "col", text: c.label, dataset: { col: c.key }, title: c.title || null });
      var sortable = opts.route && (!sortKeys || sortKeys.indexOf(c.key) >= 0);
      if (sortable) {
        var effectiveSort = opts.route.query.get("sort") || opts.defaultSort || null;
        th.classList.add("sortable");
        th.dataset.sort = c.key;
        th.tabIndex = 0;
        if (effectiveSort === c.key) {
          var curReverse = opts.route.query.get("sort_reverse");
          var descending = curReverse === null ? opts.defaultSortReverse === true : curReverse === "true";
          th.setAttribute("aria-sort", descending ? "descending" : "ascending");
        }
        th.addEventListener("click", function () { onSort(opts.route, c.key, opts.defaultSort, opts.defaultSortReverse); });
        th.addEventListener("keydown", function (ev) { if (ev.key === "Enter") onSort(opts.route, c.key, opts.defaultSort, opts.defaultSortReverse); });
      }
      return th;
    }));
    var body = h("tbody", null, rows.map(function (row) {
      var tr = h("tr", { role: "row", dataset: { key: opts.rowKey ? String(opts.rowKey(row)) : "" } });
      if (opts.onRowClick) {
        tr.tabIndex = 0;
        tr.addEventListener("click", function (ev) {
          if (ev.target && ev.target.tagName === "A") return;
          opts.onRowClick(row);
        });
        tr.addEventListener("keydown", function (ev) { if (ev.key === "Enter") opts.onRowClick(row); });
      }
      columns.forEach(function (c) {
        var td = h("td", { role: "gridcell", dataset: { col: c.key } });
        appendChildren(td, c.render ? c.render(row) : val(row[c.key]));
        tr.appendChild(td);
      });
      return tr;
    }));
    var attrs = { class: "data-table" + (opts.className ? " " + opts.className : ""), role: "grid" };
    if (opts.id) attrs.id = opts.id;
    return h("table", attrs, [h("thead", null, [head]), body]);
  }

  function tableBlock(columns, rows, opts) {
    opts = opts || {};
    var table = renderTable(columns, rows, opts);
    return h("div", { class: "table-scroll" + (opts.scrollClass ? " " + opts.scrollClass : "") }, [table]);
  }

  /* --------------------------------------------- table toolbar + paging --- */

  function toInt(v, def) {
    if (v === null || v === undefined || v === "") return def;
    var n = Number(v);
    if (isNaN(n) || !isFinite(n)) return def;
    return Math.floor(n);
  }

  /* listHref clones the current route query and applies overrides, dropping
   * empties, so a sort/page/filter click never mutates the live query in place. */
  function listHref(route, overrides) {
    var p = new URLSearchParams(route.query.toString());
    Object.keys(overrides || {}).forEach(function (k) {
      var v = overrides[k];
      if (v === null || v === undefined || v === "") p.delete(k);
      else p.set(k, String(v));
    });
    var s = p.toString();
    return route.path + (s ? "?" + s : "");
  }

  /* onSort re-queries page 1 with the chosen column; a repeat click flips the
   * direction. The current key/direction is the route query when present, else
   * the screen's declared default, so the first click on the already-default
   * column actually toggles. The server does the sorting, so the DOM is never
   * reordered optimistically. */
  function onSort(route, key, defaultSort, defaultSortReverse) {
    var curKey = route.query.get("sort") || defaultSort || null;
    var curReverseRaw = route.query.get("sort_reverse");
    var curReverse = curReverseRaw === null ? defaultSortReverse === true : curReverseRaw === "true";
    var reverse = "true";
    if (curKey === key) reverse = curReverse ? "false" : "true";
    navigate(listHref(route, { sort: key, sort_reverse: reverse, page: 1 }));
  }

  function filterControl(name, spec, current) {
    if (spec.type === "select") {
      var sel = h("select", { "aria-label": spec.label || name, dataset: { filter: name } });
      (spec.options || []).forEach(function (o) { sel.appendChild(option(o[0], o[1])); });
      sel.value = current || "";
      return sel;
    }
    if (spec.type === "checkbox") {
      var cb = h("input", { type: "checkbox", id: "filter-" + name, dataset: { filter: name } });
      cb.checked = current === "true";
      return h("span", null, [h("label", { for: "filter-" + name, text: spec.label || name }), cb]);
    }
    return h("input", {
      type: "search", value: current || "", "aria-label": spec.label || name,
      placeholder: spec.placeholder || spec.label || name, dataset: { filter: name }
    });
  }

  /* tableToolbar renders the shared list controls: the spec's filters plus an
   * optional sort selector, wired to navigate (never to a local mutation). */
  function tableToolbar(route, opts) {
    opts = opts || {};
    var controls = (opts.filters || []).map(function (f) {
      return filterControl(f.name, f, route.query.get(f.name));
    });
    if (opts.sortKeys && opts.sortKeys.length) {
      var sortSel = h("select", { id: "sort-select", "aria-label": "Sort by", dataset: { role: "sort" } });
      if (!opts.defaultSort) sortSel.appendChild(option("", "sort: default"));
      opts.sortKeys.forEach(function (k) { sortSel.appendChild(option(k, "sort: " + k)); });
      sortSel.value = route.query.get("sort") || opts.defaultSort || "";
      var rev = h("input", { type: "checkbox", id: "sort-reverse" });
      var curReverse = route.query.get("sort_reverse");
      rev.checked = curReverse === null ? opts.defaultSortReverse === true : curReverse === "true";
      controls.push(sortSel, h("label", { for: "sort-reverse", text: "descending" }), rev);
    }
    var form = h("form", { class: "toolbar" }, controls.concat([
      h("button", { type: "submit", text: "Apply" }),
      opts.action || null
    ]));
    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var overrides = { page: 1 };
      Array.prototype.forEach.call(form.querySelectorAll("[data-filter]"), function (el) {
        if (el.type === "checkbox") overrides[el.dataset.filter] = el.checked ? "true" : "";
        else overrides[el.dataset.filter] = el.value;
      });
      if (opts.sortKeys && opts.sortKeys.length) {
        var chosen = form.querySelector('[data-role="sort"]').value;
        overrides.sort = chosen;
        overrides.sort_reverse = chosen ? (form.querySelector("#sort-reverse").checked ? "true" : "false") : "";
      }
      navigate(listHref(route, overrides));
    });
    return form;
  }

  /* pagination renders the shared page controls for a list ENVELOPE. A bare
   * array is not paged: it renders a read-only count with no page controls and
   * never invents a page number (round-2 #2). An envelope whose total_count is
   * missing renders an em-dash and "unknown", never a fabricated 0. */
  function pagination(data, route) {
    var items = itemsOf(data);
    var env = data && typeof data === "object" && !Array.isArray(data) ? data : null;
    var paged = !!env && (env.page != null || env.total_count != null || env.page_count != null);
    if (!paged) {
      return h("div", { class: "pagination", dataset: { paged: "false" } }, [
        h("span", { class: "pagination-total", dataset: { totalKnown: "false" }, text: items.length + " rows" })
      ]);
    }
    var page = toInt(env.page, 1);
    if (page < 1) page = 1;
    var pageSize = toInt(env.page_size, 50);
    var totalKnown = !isNull(env.total_count) && env.total_count !== "";
    var total = totalKnown ? Number(env.total_count) : null;
    var filteredKnown = !isNull(env.filtered_count) && env.filtered_count !== "";
    var filtered = filteredKnown ? Number(env.filtered_count) : null;
    var pageCount = toInt(env.page_count, totalKnown && pageSize > 0 ? Math.max(1, Math.ceil(total / pageSize)) : null);
    var offset = (page - 1) * pageSize;
    var end = totalKnown ? Math.min(offset + items.length, total) : null;
    var shown = "showing " + (totalKnown ? String(end) : DASH) + " of " + (totalKnown ? String(total) : "unknown");
    if (filteredKnown && (!totalKnown || filtered !== total)) shown += " (filtered " + filtered + " of " + (totalKnown ? total : "unknown") + ")";
    var indicator = h("span", {
      class: "pagination-total",
      dataset: { totalKnown: totalKnown ? "true" : "false" },
      text: shown
    });
    var sizes = [25, 50, 100, 250, 500];
    var sizeSel = h("select", { id: "page-size", "aria-label": "Page size" });
    sizes.forEach(function (n) { sizeSel.appendChild(option(String(n), String(n))); });
    sizeSel.value = sizes.indexOf(pageSize) >= 0 ? String(pageSize) : "50";
    sizeSel.addEventListener("change", function () {
      navigate(listHref(route, { page: 1, page_size: sizeSel.value }));
    });
    var prev = h("button", { type: "button", id: "page-prev", text: "Prev" });
    prev.disabled = page <= 1;
    prev.addEventListener("click", function () {
      if (prev.disabled) return;
      navigate(listHref(route, { page: page - 1, page_size: pageSize }));
    });
    var next = h("button", { type: "button", id: "page-next", text: "Next" });
    next.disabled = pageCount === null ? items.length < pageSize : page >= pageCount;
    next.addEventListener("click", function () {
      if (next.disabled) return;
      navigate(listHref(route, { page: page + 1, page_size: pageSize }));
    });
    return h("div", { class: "pagination" }, [
      h("span", { class: "pagination-page", text: "page " + page + (pageCount === null ? "" : " of " + pageCount) }),
      prev, next,
      h("label", { for: "page-size", text: "per page" }), sizeSel,
      indicator
    ]);
  }

  /* -------------------------------------------------------------- modal ---- */

  function trapTab(ev, container) {
    var focusables = container.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    if (!focusables.length) return;
    var first = focusables[0];
    var last = focusables[focusables.length - 1];
    if (ev.shiftKey && document.activeElement === first) { ev.preventDefault(); last.focus(); }
    else if (!ev.shiftKey && document.activeElement === last) { ev.preventDefault(); first.focus(); }
  }

  function openModal(opts) {
    opts = opts || {};
    var root = document.getElementById("modal-root");
    var previous = document.activeElement;
    var overlay = h("div", { class: "modal-overlay" });
    var confirmBtn = h("button", { type: "button", id: "modal-confirm", text: opts.confirmLabel || "Confirm" });
    if (opts.danger) confirmBtn.className = "danger";
    var cancelBtn = h("button", { type: "button", id: "modal-cancel", text: opts.cancelLabel || "Cancel" });
    var dialog = h("div", { class: "modal", role: "dialog", "aria-modal": "true", "aria-labelledby": "modal-title" }, [
      h("h2", { id: "modal-title", text: opts.title || "Confirm" }),
      h("div", { class: "modal-body" }, [opts.body || null]),
      opts.warning ? h("p", { class: "error", text: opts.warning }) : null,
      h("div", { class: "modal-actions" }, [cancelBtn, confirmBtn])
    ]);
    overlay.appendChild(dialog);
    root.appendChild(overlay);

    function updateGuard() {
      confirmBtn.disabled = !!(opts.guard && !opts.guard());
    }
    updateGuard();

    function close(restoreFocus) {
      document.removeEventListener("keydown", onKey, true);
      if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
      if (restoreFocus !== false && previous && previous.focus) {
        try { previous.focus(); } catch (e) { /* ignore */ }
      }
    }

    function onKey(ev) {
      if (ev.key === "Escape") { ev.preventDefault(); close(true); }
      else if (ev.key === "Tab") { trapTab(ev, dialog); }
    }

    document.addEventListener("keydown", onKey, true);
    dialog.addEventListener("input", updateGuard);
    dialog.addEventListener("change", updateGuard);
    cancelBtn.addEventListener("click", function () { close(true); });
    confirmBtn.addEventListener("click", function () {
      if (confirmBtn.disabled) return;
      confirmBtn.disabled = true;
      var result = opts.onConfirm ? opts.onConfirm() : null;
      if (result && typeof result.then === "function") {
        result.then(function (keepOpen) {
          if (keepOpen === true) { updateGuard(); } else { close(true); }
        }, function () { updateGuard(); });
      } else if (result !== false) {
        close(true);
      }
    });

    var initial = opts.initialFocus || cancelBtn;
    setTimeout(function () { try { initial.focus(); } catch (e) { /* ignore */ } }, 0);
    return { close: close, dialog: dialog, confirmButton: confirmBtn, overlay: overlay };
  }

  /* ----------------------------------------------------------- chrome ------ */

  function setAuthedChrome(authed) {
    var nav = document.getElementById("nav");
    var search = document.getElementById("global-search");
    var principal = document.getElementById("principal");
    var refresh = document.getElementById("refresh-status");
    if (nav) nav.hidden = !authed;
    if (search) search.hidden = !authed;
    if (principal) principal.hidden = !authed;
    if (refresh) refresh.hidden = !authed;
  }

  function applyHealth(h) {
    var el = document.getElementById("health");
    var txt = document.getElementById("health-text");
    var status = h && h.status ? h.status : "unknown";
    var mapped = status === "ok" ? "ok" : (status === "alarm" ? "alarm" : (status === "not_ready" ? "not_ready" : "unknown"));
    if (el) el.dataset.state = mapped;
    if (txt) txt.textContent = mapped === "not_ready" ? "not ready" : mapped;
    state.health = { mapped: mapped, reason: (h && h.reason) || "" };
    renderBanner();
  }

  /* renderBanner merges the two banner sources (health alarm and boot-degraded
   * recovery) so whichever function runs last cannot leave a stale banner.
   * Precedence: alarm > not-ready > degraded. */
  function renderBanner() {
    var banner = document.getElementById("alarm-banner");
    if (!banner) return;
    var h = state.health;
    if (h && h.mapped === "alarm") {
      banner.hidden = false;
      banner.dataset.kind = "warn";
      banner.textContent = "Alarm active: " + (h.reason || "yes");
      return;
    }
    if (h && h.mapped === "not_ready") {
      banner.hidden = false;
      banner.dataset.kind = "danger";
      banner.textContent = "Not ready: " + (h.reason || "starting up");
      return;
    }
    if (state.degraded) {
      banner.hidden = false;
      banner.dataset.kind = "danger";
      var discarded = isNull(state.degraded.discarded_artifacts) ? DASH : String(state.degraded.discarded_artifacts);
      banner.textContent = "broker booted degraded; " + discarded + " artifacts discarded";
      return;
    }
    banner.hidden = true;
  }

  function applyDegraded(overview) {
    if (overview && overview.unsafe_recovery && overview.unsafe_recovery.degraded) {
      state.degraded = overview.unsafe_recovery;
    } else {
      state.degraded = null;
    }
    renderBanner();
  }

  function refreshHealth() {
    return api(API + "/health").then(function (r) {
      if (r.status === 200 || r.status === 503) applyHealth(r.data);
    }).catch(function () { /* health is best-effort */ });
  }

  function updateRefreshStatus() {
    var el = document.getElementById("refresh-status");
    if (!el) return;
    el.dataset.delayMs = String(state.refresh.delay);
    el.dataset.stale = state.refresh.stale ? "true" : "false";
    var label = document.getElementById("refresh-label");
    if (!label) return;
    var parts = [state.refresh.paused ? "paused" : "live"];
    if (state.refresh.lastUpdated) parts.push("updated " + new Date(state.refresh.lastUpdated).toLocaleTimeString());
    if (state.refresh.stale) parts.push("stale");
    label.textContent = parts.join(" \u00b7 ");
  }

  /* The retry ladder is fixed at 2/4/8/30 s (FR-UI-112), independent of the
   * healthy refresh cadence, which is what sets the normal interval. */
  function backoffDelay(errCount) {
    var steps = [2000, 4000, 8000, MAX_BACKOFF_MS];
    var i = Math.min(errCount, steps.length) - 1;
    if (i < 0) i = 0;
    var d = steps[i];
    if (d > MAX_BACKOFF_MS) d = MAX_BACKOFF_MS;
    return d;
  }

  function scheduleRefresh() {
    clearTimeout(state.refresh.timer);
    state.refresh.timer = setTimeout(refreshTick, state.refresh.delay);
  }

  function onRefreshSuccess() {
    state.refresh.errCount = 0;
    state.refresh.stale = false;
    state.refresh.delay = state.refresh.base;
    state.refresh.lastUpdated = Date.now();
    updateRefreshStatus();
  }

  function onRefreshError() {
    state.refresh.errCount += 1;
    state.refresh.stale = true;
    state.refresh.delay = backoffDelay(state.refresh.errCount);
    updateRefreshStatus();
  }

  function refreshTick() {
    clearTimeout(state.refresh.timer);
    if (state.refresh.paused || document.hidden || !state.currentLoad) {
      scheduleRefresh();
      return Promise.resolve();
    }
    refreshHealth();
    return Promise.resolve()
      .then(function () { return state.currentLoad(); })
      .then(onRefreshSuccess, onRefreshError)
      .then(function () { scheduleRefresh(); });
  }

  function refreshNow() {
    clearTimeout(state.refresh.timer);
    return refreshTick();
  }

  /* ------------------------------------------------------------ routing ---- */

  function parsePath(pathname, search) {
    var segs = pathname.split("/").filter(Boolean).map(function (s) {
      try { return decodeURIComponent(s); } catch (e) { return s; }
    });
    var r = { screen: "overview", params: {}, query: new URLSearchParams(search || ""), path: pathname };
    if (!segs.length) return r;
    switch (segs[0]) {
      case "connections": r.screen = segs[1] ? "connection" : "connections"; r.params.name = segs[1]; break;
      case "channels": r.screen = segs[1] ? "channel" : "channels"; r.params.id = segs[1]; break;
      case "consumers": r.screen = "consumers"; break;
      case "exchanges": r.screen = segs[1] ? "exchange" : "exchanges"; r.params.name = segs[1]; break;
      case "queues": r.screen = segs[1] ? "queue" : "queues"; r.params.name = segs[1]; break;
      case "bindings": r.screen = "bindings"; break;
      case "nodes": r.screen = segs[1] ? "node" : "nodes"; r.params.name = segs[1]; break;
      case "users": r.screen = "users"; break;
      case "permissions": r.screen = "permissions"; break;
      case "definitions": r.screen = "definitions"; break;
      case "metrics": r.screen = "metrics"; break;
      case "about": r.screen = "about"; break;
      case "search": r.screen = "search"; break;
      default: r.screen = "notfound";
    }
    return r;
  }

  function navigate(href) {
    if (href === location.pathname + location.search) { render(); return; }
    history.pushState({}, "", href);
    render();
  }

  /* ------------------------------------------------------------- screens --- */

  function itemsLoading() {
    return h("p", { class: "null", text: "Loading\u2026" });
  }

  function loadOverview(container) {
    var listKey = "overview-top";
    var listPath = API + "/queues" + qs({ sort: "messages_ready", sort_reverse: "true", page_size: 10 });
    return Promise.all([
      api(API + "/overview", { ifNoneMatch: state.etags.overview }),
      api(listPath, { ifNoneMatch: state.etags[listKey] })
    ]).then(function (rs) {
      var o = rs[0], q = rs[1];
      if (o.status === 403) { state.displayed = "overview"; container.replaceChildren(permissionDenied(o.data)); return; }
      if (o.status >= 400 && o.status !== 304) {
        if (state.displayed !== "overview") container.replaceChildren(errorState(o.data));
        throw new Error("overview failed");
      }
      if (o.status !== 304) {
        if (o.etag) state.etags.overview = o.etag;
        state.cache.overview = o.data;
      }
      if (q.status !== 304) {
        if (q.etag) state.etags[listKey] = q.etag;
        state.cache.overviewTop = itemsOf(q.data);
      }
      if (o.status === 304 && q.status === 304 && state.displayed === "overview") return;
      state.displayed = "overview";
      applyDegraded(state.cache.overview);
      container.replaceChildren(buildOverview(state.cache.overview, state.cache.overviewTop || []));
    });
  }

  function buildOverview(overview, rows) {
    if (overview) applyDegraded(overview);
    var ot = (overview && overview.object_totals) || {};
    var qt = (overview && overview.queue_totals) || {};
    var totals = [
      ["connections", ot.connections],
      ["channels", ot.channels],
      ["queues", ot.queues],
      ["consumers", ot.consumers],
      ["exchanges", ot.exchanges]
    ];
    var cards = h("div", { class: "cards" }, totals.map(function (t) {
      return h("div", { class: "card", dataset: { total: t[0] } }, [
        h("div", { class: "card-value", text: isNull(t[1]) ? DASH : String(t[1]) }),
        h("div", { class: "card-label", text: t[0] })
      ]);
    }));
    cards.appendChild(h("div", { class: "card", id: "queues-with-messages-card" }, [
      h("div", { class: "card-value", id: "queues-with-messages", text: isNull(qt.queues_with_messages) ? DASH : String(qt.queues_with_messages) }),
      h("div", { class: "card-label", text: "queues with messages" })
    ]));
    var coherent = overview ? overview.snapshot_coherent : null;
    var ready = qt.messages_ready;
    var summary = h("div", { class: "summary-strip" }, [
      h("span", null, [h("strong", { text: "messages ready: " }), stat(ready, "queue totals unavailable")]),
      h("span", { class: "coherent", dataset: { coherent: coherent === false ? "false" : "true" } },
        [h("strong", { text: "snapshot: " }), h("span", { text: coherent === false ? "partial" : "coherent" })])
    ]);
    var topRows = rows || [];
    var table = tableBlock([
      { key: "name", label: "Queue", render: function (r) { return link(String(r.name), "/queues/" + encodeURIComponent(r.name)); } },
      { key: "messages_ready", label: "Ready", render: function (r) { return num(r.messages_ready); } },
      { key: "consumers", label: "Consumers", render: function (r) { return num(r.consumers); } },
      { key: "state", label: "State", render: function (r) { return stateCell(r.state); } }
    ], topRows, { id: "top-queues", className: "overview-table" });
    return h("div", { class: "screen screen-overview" }, [
      h("div", { class: "page-head" }, [
        h("h1", { text: "Overview" }),
        h("span", { class: "spacer" }),
        METRICS_URL ? h("a", { id: "metrics-link", href: METRICS_URL, target: "_blank", rel: "noopener", title: "Open the metrics endpoint" },
          [h("button", { type: "button", text: "Metrics" })]) : null
      ]),
      cards,
      summary,
      h("h2", { text: "Top queues by ready depth" }),
      topRows.length ? table : emptyState("No queues match the top-10 ready-depth view.")
    ]);
  }

  var QUEUE_SORT_KEYS = ["name", "type", "durable", "auto_delete", "exclusive", "messages_ready", "messages_unacknowledged", "messages", "consumers", "state"];
  var QUEUE_DEFAULT_SORT = "messages_ready";
  var QUEUE_DEFAULT_SORT_REVERSE = true;

  function loadQueues(container, route) {
    var q = route.query;
    var params = {
      page: q.get("page") || 1,
      page_size: q.get("page_size") || 50,
      name: q.get("name"),
      durable: q.get("durable"),
      has_consumers: q.get("has_consumers"),
      non_empty: q.get("non_empty"),
      sort: q.get("sort") || QUEUE_DEFAULT_SORT,
      sort_reverse: q.get("sort_reverse") || (QUEUE_DEFAULT_SORT_REVERSE ? "true" : "false")
    };
    var path = API + "/queues" + qs(params);
    var key = "queues:" + path;
    return loadList(container, key, path, {
      render: function (r) { return buildQueues(r.data, route); }
    });
  }

  function buildQueues(data, route) {
    var items = itemsOf(data);
    var columns = [
      { key: "name", label: "Name", render: function (r) {
        return h("span", null, [
          link(String(r.name), "/queues/" + encodeURIComponent(r.name)),
          r.stats_partial ? h("span", { class: "stat-partial", text: " partial" }) : null
        ]);
      } },
      { key: "type", label: "Type", render: function (r) { return h("span", { text: val(r.type) }); } },
      { key: "durable", label: "Durable", render: function (r) { return boolCell(r.durable); } },
      { key: "auto_delete", label: "Auto-delete", render: function (r) { return boolCell(r.auto_delete); } },
      { key: "exclusive", label: "Exclusive", render: function (r) { return boolCell(r.exclusive); } },
      { key: "messages_ready", label: "Ready", render: function (r) { return num(r.messages_ready); } },
      { key: "messages_unacknowledged", label: "Unacked", render: function (r) { return num(r.messages_unacknowledged); } },
      { key: "messages", label: "Total", render: function (r) { return num(r.messages); } },
      { key: "consumers", label: "Consumers", render: function (r) { return num(r.consumers); } },
      { key: "state", label: "State", render: function (r) { return stateCell(r.state); } }
    ];
    return h("div", { class: "screen screen-queues" }, [
      queuesToolbar(route),
      items.length ? tableBlock(columns, items, {
        route: route,
        sortKeys: QUEUE_SORT_KEYS,
        defaultSort: QUEUE_DEFAULT_SORT,
        defaultSortReverse: QUEUE_DEFAULT_SORT_REVERSE,
        rowKey: function (r) { return r.name; },
        onRowClick: function (r) { navigate("/queues/" + encodeURIComponent(r.name)); }
      }) : emptyState("No queues exist yet.", "Declare queue", function () { openDeclareQueue(route); }),
      pagination(data, route)
    ]);
  }

  function queuesToolbar(route) {
    var declareBtn = h("button", { type: "button", id: "declare-queue-btn", text: "Declare queue" });
    declareBtn.addEventListener("click", function () { openDeclareQueue(route); });
    return tableToolbar(route, {
      sortKeys: QUEUE_SORT_KEYS,
      defaultSort: QUEUE_DEFAULT_SORT,
      defaultSortReverse: QUEUE_DEFAULT_SORT_REVERSE,
      filters: [
        { name: "name", type: "search", label: "Filter queues by name", placeholder: "name filter" },
        { name: "durable", type: "select", label: "Filter by durable", options: [["", "durable: any"], ["true", "durable: true"], ["false", "durable: false"]] },
        { name: "has_consumers", type: "select", label: "Filter by consumers", options: [["", "consumers: any"], ["true", "has consumers"], ["false", "no consumers"]] },
        { name: "non_empty", type: "checkbox", label: "non-empty" }
      ],
      action: declareBtn
    });
  }

  function option(value, label) {
    return h("option", { value: value, text: label });
  }

  function loadQueueDetail(container, route) {
    var name = route.params.name;
    var path = queuePath("/", name);
    return loadList(container, "queue:" + path, path, {
      backHref: "/queues", backLabel: "Back to queues",
      render: function (r) { return buildQueueDetail(r.data, route); }
    });
  }

  function buildQueueDetail(queue, route) {
    var name = String(queue.name);
    var rows = [];
    var samples = state.samples[name] || (state.samples[name] = []);
    if (!isNull(queue.messages_ready)) {
      samples.push(Number(queue.messages_ready));
      if (samples.length > 30) samples.shift();
    }
    var props = h("dl", { class: "props" }, [
      h("dt", { text: "vhost" }), h("dd", { text: val(queue.vhost) }),
      h("dt", { text: "type" }), h("dd", { text: val(queue.type) }),
      h("dt", { text: "durable" }), h("dd", { text: boolCell(queue.durable) }),
      h("dt", { text: "auto-delete" }), h("dd", { text: boolCell(queue.auto_delete) }),
      h("dt", { text: "exclusive" }), h("dd", { text: boolCell(queue.exclusive) })
    ]);
    var stats = h("dl", { class: "props" }, [
      h("dt", { text: "ready" }), h("dd", null, [num(queue.messages_ready)]),
      h("dt", { text: "unacknowledged" }), h("dd", null, [num(queue.messages_unacknowledged)]),
      h("dt", { text: "total" }), h("dd", null, [num(queue.messages)]),
      h("dt", { text: "consumers" }), h("dd", null, [num(queue.consumers)]),
      h("dt", { text: "state" }), h("dd", null, [stateCell(queue.state)]),
      h("dt", { text: "stats" }), h("dd", null, [queue.stats_partial ? h("span", { class: "stat-partial", text: "partial" }) : h("span", { text: "complete" })])
    ]);
    var purgeBtn = h("button", { type: "button", id: "purge-btn", class: "danger", text: "Purge" });
    purgeBtn.addEventListener("click", function () { openPurge(queue); });
    var deleteBtn = h("button", { type: "button", id: "delete-btn", class: "danger", text: "Delete" });
    deleteBtn.addEventListener("click", function () { openDeleteQueue(queue); });
    var getBtn = h("button", { type: "button", id: "get-btn", text: "Get messages" });
    getBtn.addEventListener("click", function () { openGet(queue); });
    var publishBtn = h("button", { type: "button", id: "publish-btn", text: "Publish" });
    publishBtn.addEventListener("click", function () { openPublish(exchangePath("/", "amq.default"), "amq.default"); });
    return h("div", { class: "screen screen-queue-detail" }, [
      h("nav", { class: "breadcrumb" }, [link("Queues", "/queues"), h("span", { text: " / " }), h("span", { text: name })]),
      h("div", { class: "page-head" }, [h("h1", { text: name }), h("span", { class: "spacer" }), copyLinkButton(), getBtn, publishBtn, purgeBtn, deleteBtn]),
      props,
      h("h2", { text: "Statistics" }), stats,
      h("h2", { text: "Arguments" }), renderArguments(queue.arguments),
      h("h2", { text: "Bindings" }), queueBindingsPanel("/", name),
      h("h2", { text: "Ready-depth sparkline" }), sparkline(samples),
      h("h2", { text: "Rows" }), tableBlock(
        [{ key: "ready", label: "ready", render: function (r) { return num(r.ready); } }],
        rows
      )
    ]);
  }

  function sparkline(samples) {
    /* A CSS-only sparkline: no inline styles, no SVG namespace, no external
     * reference. Samples are bucketed into ten classes whose heights the
     * stylesheet defines, keeping the CSP style-src 'self' promise intact. */
    var bars = h("div", { class: "sparkline-bars", role: "img", "aria-label": "Session ready-depth samples" });
    if (!samples.length) {
      bars.appendChild(h("span", { class: "null", text: "no samples yet" }));
    } else {
      var max = Math.max.apply(null, samples.concat([1]));
      samples.forEach(function (v) {
        var bucket = Math.round((v / max) * 9);
        if (bucket < 0) bucket = 0;
        if (bucket > 9) bucket = 9;
        bars.appendChild(h("span", { class: "bar b" + bucket }));
      });
    }
    return h("div", { class: "sparkline" }, [bars, h("span", { text: "live, not historical (" + samples.length + " samples this session)" })]);
  }

  function renderArguments(args) {
    if (isNull(args) || (typeof args === "object" && Object.keys(args).length === 0)) {
      return h("p", { class: "null", text: "no arguments" });
    }
    var dl = h("dl", { class: "props" });
    Object.keys(args).forEach(function (k) {
      dl.appendChild(h("dt", { text: k }));
      dl.appendChild(h("dd", { text: String(args[k]) }));
    });
    return dl;
  }

  function loadExchanges(container, route) {
    var q = route.query;
    var params = { page: q.get("page") || 1, page_size: q.get("page_size") || 50, name: q.get("name"), type: q.get("type"), sort: q.get("sort"), sort_reverse: q.get("sort_reverse") };
    var path = API + "/exchanges" + qs(params);
    return loadList(container, "exchanges:" + path, path, {
      render: function (r) { return buildExchanges(r.data, route); }
    });
  }

  var EXCHANGE_SORT_KEYS = ["name", "vhost", "type", "durable", "auto_delete", "internal"];

  function buildExchanges(data, route) {
    var items = itemsOf(data);
    var columns = [
      { key: "name", label: "Name", render: function (r) {
        var isSystem = r.name === "" || String(r.name).indexOf("amq.") === 0;
        return h("span", null, [link(String(r.name || "(default)"), "/exchanges/" + encodeURIComponent(r.name)), isSystem ? badge("system", "system") : null]);
      } },
      { key: "vhost", label: "Vhost", render: function (r) { return h("span", { text: val(r.vhost) }); } },
      { key: "type", label: "Type", render: function (r) { return h("span", { text: val(r.type) }); } },
      { key: "durable", label: "Durable", render: function (r) { return boolCell(r.durable); } },
      { key: "auto_delete", label: "Auto-delete", render: function (r) { return boolCell(r.auto_delete); } },
      { key: "internal", label: "Internal", render: function (r) { return boolCell(r.internal); } }
    ];
    var declareBtn = h("button", { type: "button", id: "declare-exchange-btn", text: "Declare exchange" });
    declareBtn.addEventListener("click", function () { openDeclareExchange(route); });
    return h("div", { class: "screen screen-exchanges" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Exchanges" }), h("span", { class: "spacer" }), declareBtn]),
      tableToolbar(route, {
        sortKeys: EXCHANGE_SORT_KEYS,
        filters: [
          { name: "name", type: "search", label: "Filter exchanges by name", placeholder: "name filter" },
          { name: "type", type: "search", label: "Filter exchanges by type", placeholder: "type filter" }
        ]
      }),
      items.length ? tableBlock(columns, items, {
        route: route,
        sortKeys: EXCHANGE_SORT_KEYS,
        rowKey: function (r) { return r.name; },
        onRowClick: function (r) { navigate("/exchanges/" + encodeURIComponent(r.name)); }
      }) : emptyState("No exchanges exist."),
      pagination(data, route)
    ]);
  }

  function loadExchangeDetail(container, route) {
    var name = route.params.name;
    var path = exchangePath("/", name);
    return loadList(container, "exchange:" + path, path, {
      backHref: "/exchanges", backLabel: "Back to exchanges",
      render: function (r) { return buildExchangeDetail(r.data, route); }
    });
  }

  function buildExchangeDetail(exchange, route) {
    var name = String(exchange.name);
    var isSystem = name === "" || name.indexOf("amq.") === 0;
    var publishBtn = h("button", { type: "button", id: "publish-btn", text: "Publish" });
    publishBtn.addEventListener("click", function () { openPublish(exchangePath("/", name), name); });
    var deleteBtn = h("button", { type: "button", id: "delete-btn", class: "danger", text: "Delete" });
    deleteBtn.addEventListener("click", function () {
      if (isSystem) { toast("The default and amq.* exchanges cannot be deleted.", "error"); return; }
      openDeleteExchange(exchange);
    });
    if (isSystem) { deleteBtn.disabled = true; deleteBtn.title = "The default and amq.* exchanges cannot be deleted"; }
    var props = h("dl", { class: "props" }, [
      h("dt", { text: "vhost" }), h("dd", { text: val(exchange.vhost) }),
      h("dt", { text: "type" }), h("dd", { text: val(exchange.type) }),
      h("dt", { text: "durable" }), h("dd", { text: boolCell(exchange.durable) }),
      h("dt", { text: "auto-delete" }), h("dd", { text: boolCell(exchange.auto_delete) }),
      h("dt", { text: "internal" }), h("dd", { text: boolCell(exchange.internal) })
    ]);
    return h("div", { class: "screen screen-exchange-detail" }, [
      h("nav", { class: "breadcrumb" }, [link("Exchanges", "/exchanges"), h("span", { text: " / " }), h("span", { text: name || "(default)" })]),
      h("div", { class: "page-head" }, [h("h1", { text: name || "(default)" }), h("span", { class: "spacer" }), copyLinkButton(), publishBtn, deleteBtn]),
      props,
      h("h2", { text: "Arguments" }), renderArguments(exchange.arguments),
      h("h2", { text: "Bindings" }), exchangeBindingsPanel("/", name)
    ]);
  }

  function loadConnections(container, route) {
    var q = route.query;
    var params = { page: q.get("page") || 1, page_size: q.get("page_size") || 50, name: q.get("name"), user: q.get("user"), sort: q.get("sort"), sort_reverse: q.get("sort_reverse") };
    var path = API + "/connections" + qs(params);
    /* The per-row Close column is capability-gated, so ensure the capability
     * map is present before the first paint. */
    return ensureCapabilities().then(function () {
      return loadList(container, "connections:" + path, path, {
        render: function (r) { return buildConnections(r.data, route); }
      });
    });
  }

  /* The connections endpoint's sort allowlist is name/user/vhost/state/channels/
   * connected_at/last_activity (peer is only sortable as peer_host). */
  var CONNECTION_SORT_KEYS = ["name", "user", "vhost", "state", "channels", "connected_at", "last_activity"];

  function buildConnections(data, route) {
    var items = itemsOf(data);
    var columns = [
      { key: "name", label: "Name", render: function (r) { return link(String(r.name), "/connections/" + encodeURIComponent(r.name)); } },
      { key: "user", label: "User", render: function (r) { return h("span", { text: val(r.user) }); } },
      { key: "vhost", label: "Vhost", render: function (r) { return h("span", { text: val(r.vhost) }); } },
      { key: "state", label: "State", render: function (r) { return stateCell(r.state); } },
      { key: "channels", label: "Channels", render: function (r) { return num(r.channels); } },
      { key: "secure", label: "Secure", render: function (r) { return secureCell(r); } },
      { key: "peer", label: "Peer", render: function (r) { return h("span", { text: val(r.peer_host) + ":" + val(r.peer_port) }); } },
      { key: "connected_at", label: "Connected at", render: function (r) { return fmtTime(r.connected_at); } },
      { key: "last_activity", label: "Last activity", render: function (r) { return fmtTime(r.last_activity); } },
      { key: "actions", label: "", render: function (r) { return rowCloseButton(r); } }
    ];
    return h("div", { class: "screen screen-connections" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Connections" })]),
      h("p", { class: "null", text: "Entity data is global even if connection vhosts differ (D-1)." }),
      tableToolbar(route, {
        sortKeys: CONNECTION_SORT_KEYS,
        filters: [
          { name: "name", type: "search", label: "Filter connections by name", placeholder: "name filter" },
          { name: "user", type: "search", label: "Filter connections by user", placeholder: "user filter" }
        ]
      }),
      items.length ? tableBlock(columns, items, {
        route: route,
        sortKeys: CONNECTION_SORT_KEYS,
        rowKey: function (r) { return r.name; },
        onRowClick: function (r) { navigate("/connections/" + encodeURIComponent(r.name)); }
      }) : emptyState("No client connections."),
      pagination(data, route)
    ]);
  }

  function closeConnectionState() {
    var c = state.capabilities ? state.capabilities.close_connection : undefined;
    if (c === true) return "on";
    if (c === false) return "off";
    return "unknown";
  }

  function closeConnectionDisabledReason(st) {
    if (st === "off") return "Force-close is not implemented by this broker (close_connection capability is false)";
    return "capability unknown (not reported by this broker / lookup failed)";
  }

  /* rowCloseButton is the S2 per-row close control; it is disabled, with a
   * tooltip, when the broker reports the capability is unsupported or unknown. */
  function rowCloseButton(conn) {
    var btn = h("button", { type: "button", class: "danger row-action", dataset: { cap: "close_connection" }, text: "Close" });
    applyCapabilityGate(btn);
    btn.addEventListener("click", function (ev) {
      ev.stopPropagation();
      if (btn.disabled) return;
      openCloseConnection(conn);
    });
    return btn;
  }

  function loadConnectionDetail(container, route) {
    var name = route.params.name;
    var path = API + "/connections/" + encodeURIComponent(name);
    /* The close button is capability-gated, so make sure the capability map is
     * loaded before the first render of this screen (a deep link may beat the
     * boot-time version fetch). */
    return ensureCapabilities().then(function () {
      return loadList(container, "connection:" + path, path, {
        backHref: "/connections", backLabel: "Back to connections",
        render: function (r) { return buildConnectionDetail(r.data, route); }
      });
    });
  }

  function buildConnectionDetail(conn, route) {
    var name = String(conn.name);
    var closeBtn = h("button", { type: "button", id: "close-connection-btn", class: "danger", dataset: { cap: "close_connection" }, text: "Close" });
    applyCapabilityGate(closeBtn);
    closeBtn.addEventListener("click", function () {
      if (closeBtn.disabled) return;
      openCloseConnection(conn);
    });
    var channelsPanel = h("div", { id: "connection-channels" }, [itemsLoading()]);
    api(API + "/connections/" + encodeURIComponent(name) + "/channels").then(function (r) {
      if (r.status === 501) channelsPanel.replaceChildren(h("p", { class: "null", text: "channel detail not available" }));
      else if (r.status >= 400) channelsPanel.replaceChildren(errorState(r.data));
      else channelsPanel.replaceChildren(tableBlock(
        [{ key: "number", label: "Channel", render: function (c) { return channelLink(c); } },
         { key: "state", label: "State", render: function (c) { return stateCell(c.state); } },
         { key: "consumers", label: "Consumers", render: function (c) { return num(c.consumers); } },
         { key: "unacked", label: "Unacked", render: function (c) { return num(c.unacked); } },
         { key: "prefetch_count", label: "Prefetch", render: function (c) { return num(c.prefetch_count); } },
         { key: "confirm", label: "Confirm", render: function (c) { return boolCell(c.confirm); } },
         { key: "transactional", label: "Transactional", render: function (c) { return boolCell(c.transactional); } }],
        itemsOf(r.data), {}));
    }).catch(function () { channelsPanel.replaceChildren(h("p", { class: "null", text: "channel detail not available" })); });
    var props = h("dl", { class: "props" }, [
      h("dt", { text: "user" }), h("dd", { text: val(conn.user) }),
      h("dt", { text: "vhost" }), h("dd", { text: val(conn.vhost) }),
      h("dt", { text: "state" }), h("dd", null, [stateCell(conn.state)]),
      h("dt", { text: "SSL" }), h("dd", null, [boolCell(conn.secure, "Yes", "No")]),
      h("dt", { text: "auth_mechanism" }), h("dd", null, [authMechanismCell(conn.auth_mechanism)]),
      h("dt", { text: "protocol" }), h("dd", { text: val(conn.protocol) }),
      h("dt", { text: "peer" }), h("dd", { text: val(conn.peer_host) + ":" + val(conn.peer_port) }),
      h("dt", { text: "connected_at" }), h("dd", null, [fmtTime(conn.connected_at)]),
      h("dt", { text: "last_activity" }), h("dd", null, [fmtTime(conn.last_activity)])
    ]);
    return h("div", { class: "screen screen-connection-detail" }, [
      h("nav", { class: "breadcrumb" }, [link("Connections", "/connections"), h("span", { text: " / " }), h("span", { text: name })]),
      h("div", { class: "page-head" }, [h("h1", { text: name }), h("span", { class: "spacer" }), copyLinkButton(), closeBtn]),
      props,
      h("p", { class: "null", text: "Entity data is global even if connection vhosts differ (D-1)." }),
      h("h2", { text: "Channels" }), channelsPanel
    ]);
  }

  /* The channels endpoint's sort allowlist is number/connection/user/vhost/
   * state/consumers; unacked/prefetch/confirm/transactional are not sortable. */
  var CHANNEL_SORT_KEYS = ["number", "connection", "user", "vhost", "state", "consumers"];

  function loadChannels(container, route) {
    var q = route.query;
    var path = API + "/channels" + qs({ page: q.get("page") || 1, page_size: q.get("page_size") || 100, sort: q.get("sort"), sort_reverse: q.get("sort_reverse") });
    return loadList(container, "channels:" + path, path, {
      render: function (r) { return buildChannels(r.data, route); }
    });
  }

  function buildChannels(data, route) {
    var items = itemsOf(data);
    var columns = [
      { key: "number", label: "Number", render: function (r) { return channelLink(r); } },
      { key: "connection", label: "Connection", render: function (r) { return h("span", { text: val(r.connection) }); } },
      { key: "user", label: "User", render: function (r) { return h("span", { text: val(r.user) }); } },
      { key: "vhost", label: "Vhost", render: function (r) { return h("span", { text: val(r.vhost) }); } },
      { key: "state", label: "State", render: function (r) { return stateCell(r.state); } },
      { key: "consumers", label: "Consumers", render: function (r) { return num(r.consumers); } },
      { key: "unacked", label: "Unacked", render: function (r) { return num(r.unacked); } },
      { key: "prefetch_count", label: "Prefetch", render: function (r) { return num(r.prefetch_count); } },
      { key: "confirm", label: "Confirm", render: function (r) { return boolCell(r.confirm); } },
      { key: "transactional", label: "Transactional", render: function (r) { return boolCell(r.transactional); } }
    ];
    return h("div", { class: "screen screen-channels" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Channels" })]),
      h("p", { class: "notice empty", role: "note", text: "Channels are read-only. There is no management close-channel operation; a channel closes when its client closes it or its connection is closed." }),
      tableToolbar(route, { sortKeys: CHANNEL_SORT_KEYS }),
      items.length ? tableBlock(columns, items, { route: route, sortKeys: CHANNEL_SORT_KEYS, rowKey: function (r) { return r.number; } }) : emptyState("No open channels."),
      pagination(data, route)
    ]);
  }

  /* channelLink renders a channel number as a deep link to its detail screen,
   * addressed by the canonical "{connection}:{number}" composite id. When the
   * connection is not reported the number is not a usable key, so it renders
   * as plain text rather than a dead link. */
  function channelLink(c) {
    if (isNull(c.connection) || isNull(c.number)) return num(c.number);
    return link(String(c.number), "/channels/" + encodeURIComponent(String(c.connection) + ":" + String(c.number)));
  }

  function loadChannelDetail(container, route) {
    var id = route.params.id;
    var path = API + "/channels/" + encodeURIComponent(id);
    return loadList(container, "channel:" + path, path, {
      backHref: "/channels", backLabel: "Back to channels",
      render: function (r) { return buildChannelDetail(r.data); }
    });
  }

  function buildChannelDetail(c) {
    var props = h("dl", { class: "props" }, [
      h("dt", { text: "number" }), h("dd", null, [num(c.number)]),
      h("dt", { text: "connection" }), h("dd", { text: val(c.connection) }),
      h("dt", { text: "user" }), h("dd", { text: val(c.user) }),
      h("dt", { text: "vhost" }), h("dd", { text: val(c.vhost) }),
      h("dt", { text: "state" }), h("dd", null, [stateCell(c.state)]),
      h("dt", { text: "consumers" }), h("dd", null, [num(c.consumers)]),
      h("dt", { text: "unacked" }), h("dd", null, [num(c.unacked)]),
      h("dt", { text: "prefetch_count" }), h("dd", null, [num(c.prefetch_count)]),
      h("dt", { text: "confirm" }), h("dd", null, [boolCell(c.confirm)]),
      h("dt", { text: "transactional" }), h("dd", null, [boolCell(c.transactional)]),
      h("dt", { text: "stats" }), h("dd", null, [isNull(c.stats_partial) ? dash("stats_partial not reported") : (c.stats_partial ? h("span", { class: "stat-partial", text: "partial" }) : h("span", { text: "complete" }))])
    ]);
    return h("div", { class: "screen screen-channel-detail" }, [
      h("nav", { class: "breadcrumb" }, [link("Channels", "/channels"), h("span", { text: " / " }), h("span", { text: val(c.number) })]),
      h("div", { class: "page-head" }, [h("h1", { text: "Channel " + val(c.number) }), h("span", { class: "spacer" }), copyLinkButton()]),
      h("p", { class: "notice empty", role: "note", text: "Channels are read-only. There is no management close-channel operation; a channel closes when its client closes it or its connection is closed." }),
      props
    ]);
  }

  /* The consumers endpoint's sort allowlist is consumer_tag/queue/channel/
   * connection/ack_required/exclusive/prefetch_count; unacked is not sortable. */
  var CONSUMER_SORT_KEYS = ["consumer_tag", "queue", "channel", "connection", "ack_required", "exclusive", "prefetch_count"];

  function loadConsumers(container, route) {
    var q = route.query;
    var path = API + "/consumers" + qs({ page: q.get("page") || 1, page_size: q.get("page_size") || 100, queue: q.get("queue"), connection: q.get("connection"), sort: q.get("sort"), sort_reverse: q.get("sort_reverse") });
    return loadList(container, "consumers:" + path, path, {
      render: function (r) { return buildConsumers(r.data, route); }
    });
  }

  function buildConsumers(data, route) {
    var items = itemsOf(data);
    var columns = [
      { key: "consumer_tag", label: "Consumer tag", render: function (r) { return h("span", { text: val(r.consumer_tag) }); } },
      { key: "queue", label: "Queue", render: function (r) { return link(String(r.queue), "/queues/" + encodeURIComponent(r.queue)); } },
      { key: "channel", label: "Channel", render: function (r) { return valueOrDash(r.channel); } },
      { key: "connection", label: "Connection", render: function (r) { return valueOrDash(r.connection); } },
      { key: "ack_required", label: "Ack-required", render: function (r) { return boolCell(r.ack_required); } },
      { key: "exclusive", label: "Exclusive", render: function (r) { return boolCell(r.exclusive); } },
      { key: "prefetch_count", label: "Prefetch", render: function (r) { return num(r.prefetch_count); } },
      { key: "unacked", label: "Unacked", render: function (r) { return num(r.unacked); } }
    ];
    return h("div", { class: "screen screen-consumers" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Consumers" })]),
      h("p", { class: "null", text: "Consumer tags are not unique keys; the (connection, channel) pair identifies a consumer." }),
      tableToolbar(route, {
        sortKeys: CONSUMER_SORT_KEYS,
        filters: [
          { name: "queue", type: "search", label: "Filter consumers by queue", placeholder: "queue filter" },
          { name: "connection", type: "search", label: "Filter consumers by connection", placeholder: "connection filter" }
        ]
      }),
      items.length ? tableBlock(columns, items, { route: route, sortKeys: CONSUMER_SORT_KEYS, rowKey: function (r) { return r.consumer_tag; } }) : emptyState("No active consumers."),
      pagination(data, route)
    ]);
  }

  function valueOrDash(v) {
    if (isNull(v)) return dash("not reported by this broker");
    return h("span", { text: String(v) });
  }

  /* bindingsWriteState is the three-state capability gate for binding
   * mutations: "on" (true), "off" (false) or "unknown" (missing/unreported). A
   * control is only enabled for "on". */
  function bindingsWriteState() {
    var c = state.capabilities ? state.capabilities.bindings : undefined;
    if (c === true) return "on";
    if (c === false) return "off";
    return "unknown";
  }

  function bindingsDisabledReason(st) {
    if (st === "off") return "Binding management is not implemented by this broker (bindings capability is false)";
    return "capability unknown (not reported by this broker / lookup failed)";
  }

  var BINDING_SORT_KEYS = ["source", "vhost", "destination", "destination_type", "routing_key", "properties_key"];

  /* bindingColumns is the shared DATA-012 projection. withActions adds the
   * per-row delete control used on the aggregate screen. */
  function bindingColumns(vhost, withActions) {
    var cols = [
      { key: "source", label: "Source", render: function (r) { return h("span", { text: val(r.source) }); } },
      { key: "destination", label: "Destination", render: function (r) { return h("span", { text: val(r.destination) }); } },
      { key: "destination_type", label: "Type", render: function (r) { return h("span", { text: val(r.destination_type) }); } },
      { key: "routing_key", label: "Routing key", render: function (r) { return h("span", { text: val(r.routing_key) }); } },
      { key: "properties_key", label: "Properties key", render: function (r) { return h("span", { text: val(r.properties_key) }); } }
    ];
    if (withActions) {
      cols.push({ key: "actions", label: "", render: function (r) { return rowDeleteBindingButton(vhost, r); } });
    }
    return cols;
  }

  function rowDeleteBindingButton(vhost, b) {
    var btn = h("button", { type: "button", class: "danger row-action", text: "Delete" });
    if (isNull(b.properties_key) || b.properties_key === "") {
      btn.disabled = true;
      btn.title = "This binding has no properties_key and cannot be addressed for deletion";
      return btn;
    }
    btn.addEventListener("click", function (ev) {
      ev.stopPropagation();
      openDeleteBinding(vhost, b);
    });
    return btn;
  }

  function loadBindings(container, route) {
    if (state.capabilities && state.capabilities.bindings === false) {
      container.replaceChildren(capabilityNotice(
        { error: "not_implemented", reason: "binding management is not available in this deployment" },
        "Bindings not available"));
      return Promise.resolve();
    }
    var q = route.query;
    var vhost = q.get("vhost") || "/";
    var params = {
      page: q.get("page") || 1,
      page_size: q.get("page_size") || 50,
      source: q.get("source"),
      destination: q.get("destination"),
      sort: q.get("sort"),
      sort_reverse: q.get("sort_reverse")
    };
    var path = API + "/bindings" + (vhost !== "/" ? "/" + vhostPath(vhost) : "") + qs(params);
    return loadList(container, "bindings:" + path, path, {
      notImplementedTitle: "Bindings not available",
      render: function (r) { return buildBindings(r.data, route, vhost); }
    });
  }

  function buildBindings(data, route, vhost) {
    var items = itemsOf(data);
    var createBtn = h("button", { type: "button", id: "create-binding-btn", dataset: { cap: "bindings" }, text: "Create binding" });
    applyCapabilityGate(createBtn);
    createBtn.addEventListener("click", function () { if (!createBtn.disabled) openCreateBinding(route, vhost, null); });
    return h("div", { class: "screen screen-bindings" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Bindings" }), h("span", { class: "spacer" }), createBtn]),
      tableToolbar(route, {
        sortKeys: BINDING_SORT_KEYS,
        filters: [
          { name: "source", type: "search", label: "Filter bindings by source", placeholder: "source filter" },
          { name: "destination", type: "search", label: "Filter bindings by destination", placeholder: "destination filter" }
        ]
      }),
      items.length ? tableBlock(bindingColumns(vhost, true), items, {
        route: route,
        sortKeys: BINDING_SORT_KEYS,
        rowKey: function (r) { return bindingRowKey(r); }
      }) : emptyState("No bindings."),
      pagination(data, route)
    ]);
  }

  /* queueBindingsPanel renders a queue's bindings (destination = the queue) from
   * GET /api/queues/{vhost}/{name}/bindings. */
  function queueBindingsPanel(vhost, name) {
    var panel = h("div", { id: "queue-bindings" }, [itemsLoading()]);
    if (state.capabilities && state.capabilities.bindings === false) {
      panel.replaceChildren(capabilityNotice({ error: "not_implemented", reason: "binding management is not available in this deployment" }, "Bindings not available"));
      return panel;
    }
    api(queuePath(vhost, name) + "/bindings").then(function (r) {
      if (r.status === 501) { panel.replaceChildren(capabilityNotice(r.data, "Bindings not available")); return; }
      if (r.status >= 400) { panel.replaceChildren(errorState(r.data)); return; }
      var items = itemsOf(r.data);
      panel.replaceChildren(items.length ? tableBlock(bindingColumns(vhost, false), items, {}) : emptyState("No bindings on this queue."));
    }, function () { panel.replaceChildren(errorState(null)); });
    return panel;
  }

  /* exchangeBindingsPanel renders an exchange's bindings in both directions:
   * as source and as destination (FR-API-057). */
  function exchangeBindingsPanel(vhost, name) {
    var wrap = h("div", { id: "exchange-bindings" });
    var off = state.capabilities && state.capabilities.bindings === false;
    [["exchange-bindings-source", "As source", "source"],
     ["exchange-bindings-destination", "As destination", "destination"]].forEach(function (spec) {
      var box = h("div", { id: spec[0] }, [itemsLoading()]);
      if (off) {
        box.replaceChildren(capabilityNotice({ error: "not_implemented", reason: "binding management is not available in this deployment" }, "Bindings not available"));
      } else {
        api(exchangePath(vhost, name) + "/bindings/" + spec[2]).then(function (r) {
          if (r.status === 501) { box.replaceChildren(capabilityNotice(r.data, "Bindings not available")); return; }
          if (r.status >= 400) { box.replaceChildren(errorState(r.data)); return; }
          var items = itemsOf(r.data);
          box.replaceChildren(items.length ? tableBlock(bindingColumns(vhost, false), items, {}) : emptyState("No bindings " + spec[2] + " this exchange."));
        }, function () { box.replaceChildren(errorState(null)); });
      }
      wrap.appendChild(h("div", null, [h("h3", { text: spec[1] }), box]));
    });
    return wrap;
  }

  function loadNodes(container, route) {
    var path = route.params.name ? API + "/nodes/" + encodeURIComponent(route.params.name) : API + "/nodes";
    return loadList(container, "nodes:" + path, path, {
      backHref: "/nodes", backLabel: "Back to nodes",
      render: function (r) { return buildNodes(r.data, route); }
    });
  }

  function buildNodes(data, route) {
    var nodes = itemsOf(data);
    if (route.params.name && !nodes.length) return notFound("/nodes", "Back to nodes");
    return h("div", { class: "screen screen-nodes" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Nodes" })]),
      h("p", { class: "null", text: "single-node broker; no cluster (D-2)." }),
      nodes.length ? tableBlock([
        { key: "name", label: "Name", render: function (r) { return h("span", { text: val(r.name) }); } },
        { key: "type", label: "Type", render: function (r) { return h("span", { text: val(r.type) }); } },
        { key: "running", label: "Running", render: function (r) { return boolCell(r.running); } },
        { key: "mem_used", label: "Memory used", render: function (r) { return num(r.mem_used); } },
        { key: "disk_free", label: "Disk free", render: function (r) { return num(r.disk_free); } },
        { key: "uptime", label: "Uptime", render: function (r) { return fmtDuration(r.uptime); } },
        { key: "processors", label: "Processors", render: function (r) { return num(r.processors); } }
      ], nodes, { rowKey: function (r) { return r.name; } }) : emptyState("No node information reported."),
      pagination(data, route)
    ]);
  }

  function loadUsers(container, route) {
    return loadList(container, "users", API + "/users", {
      render: function (r) {
        var users = itemsOf(r.data);
        var userBlock = users.length ? tableBlock([
          { key: "name", label: "User", render: function (u) { return h("span", { text: val(u.name) }); } },
          { key: "tags", label: "Tags", render: function (u) { return h("span", { text: Array.isArray(u.tags) ? u.tags.join(", ") : val(u.tags) }); } }
        ], users, { rowKey: function (u) { return u.name; } }) : emptyState("No users configured.");
        var perms = h("div", { id: "permissions-panel" }, [itemsLoading()]);
        api(API + "/permissions").then(function (p) {
          if (p.status >= 400) { perms.replaceChildren(permissionDenied(p.data)); return; }
          var rows = itemsOf(p.data);
          perms.replaceChildren(rows.length ? tableBlock([
            { key: "user", label: "User", render: function (x) { return h("span", { text: val(x.user) }); } },
            { key: "vhost", label: "Vhost", render: function (x) { return h("span", { text: val(x.vhost) }); } },
            { key: "configure", label: "Configure", render: function (x) { return h("span", { text: val(x.configure) }); } },
            { key: "write", label: "Write", render: function (x) { return h("span", { text: val(x.write) }); } },
            { key: "read", label: "Read", render: function (x) { return h("span", { text: val(x.read) }); } }
          ], rows, {}) : emptyState("No permissions configured."));
        }).catch(function () { perms.replaceChildren(errorState(null)); });
        return h("div", { class: "screen screen-users" }, [
          h("div", { class: "page-head" }, [h("h1", { text: "Users & Permissions" })]),
          h("p", { class: "null", text: "Read-only. User and permission mutation controls are absent (BOB-6); no password hashes are ever returned." }),
          h("h2", { text: "Users" }), userBlock, pagination(r.data, route),
          h("h2", { text: "Permissions" }), perms
        ]);
      }
    });
  }

  function loadPermissions(container, route) {
    return loadList(container, "permissions", API + "/permissions", {
      render: function (r) {
        var items = itemsOf(r.data);
        return h("div", { class: "screen screen-permissions" }, [
          h("div", { class: "page-head" }, [h("h1", { text: "Permissions" })]),
          h("p", { class: "null", text: "Read-only. Mutation controls are absent (BOB-6)." }),
          items.length ? tableBlock([
            { key: "user", label: "User", render: function (x) { return h("span", { text: val(x.user) }); } },
            { key: "vhost", label: "Vhost", render: function (x) { return h("span", { text: val(x.vhost) }); } },
            { key: "configure", label: "Configure", render: function (x) { return h("span", { text: val(x.configure) }); } },
            { key: "write", label: "Write", render: function (x) { return h("span", { text: val(x.write) }); } },
            { key: "read", label: "Read", render: function (x) { return h("span", { text: val(x.read) }); } }
          ], items, { rowKey: function (x) { return x.user; } }) : emptyState("No permissions configured."),
          pagination(r.data, route)
        ]);
      }
    });
  }

  /* definitionsName renders the display name of one applied/skipped/failed
   * item: exchanges/queues/users use `name`, bindings use source -> destination. */
  function definitionsName(x) {
    if (x && (x.type === "binding") && (x.source || x.destination)) {
      return val(x.source) + " \u2192 " + val(x.destination);
    }
    return val(x && x.name);
  }

  /* definitionsTable renders one of the three outcome tables (applied/skipped/
   * failed). Each item is `{type,name,vhost,result,reason}` or, for a binding,
   * `{type,source,destination,destination_type,properties_key,result,reason}`. */
  function definitionsTable(id, label, items) {
    var rows = itemsOf(items);
    var section = h("div", { class: "defs-section", id: id }, [h("h4", { text: label + " (" + rows.length + ")" })]);
    if (!rows.length) {
      section.appendChild(emptyState("none"));
      return section;
    }
    section.appendChild(tableBlock([
      { key: "type", label: "Type", render: function (x) { return h("span", { text: val(x.type) }); } },
      { key: "name", label: "Name", render: function (x) { return h("span", { text: definitionsName(x) }); } },
      { key: "vhost", label: "Vhost", render: function (x) { return h("span", { text: val(x.vhost) }); } },
      { key: "result", label: "Result", render: function (x) { return h("span", { text: val(x.result) }); } },
      { key: "reason", label: "Reason", render: function (x) { return h("span", { text: val(x.reason) }); } }
    ], rows, {}));
    return section;
  }

  /* renderDefinitionsOutcome renders the dry-run preview (would-be) or the real
   * import result. The non-atomic disclosure and the "not supported (BOB-6)"
   * user/permission note are always shown (FR-UI-092/FR-IA-082). */
  function renderDefinitionsOutcome(doc, dryRun) {
    doc = doc || {};
    return h("div", { class: "fieldset", id: "definitions-result" }, [
      h("h3", { id: "definitions-result-title", text: dryRun ? "Dry-run preview" : "Import result" }),
      h("p", { class: "stat-partial", id: "definitions-atomic-status", text: "Non-atomic import: applied, skipped and failed are reported per entity; some entities may fail." }),
      definitionsTable("definitions-applied", dryRun ? "Applied (would-be)" : "Applied", doc.applied),
      definitionsTable("definitions-skipped", "Skipped", doc.skipped),
      h("p", { class: "null", id: "definitions-bob6-note", text: "Not supported (BOB-6): users and permissions are always skipped." }),
      definitionsTable("definitions-failed", "Failed", doc.failed)
    ]);
  }

  function loadDefinitions(container, route) {
    var exportLink = h("a", { href: API + "/definitions", download: "strangeq-definitions.json", id: "definitions-export" },
      [h("button", { type: "button", text: "Export definitions" })]);
    var exportNote = h("p", { id: "definitions-export-note", text: "Export is a subset: user credentials and policies are excluded. The export includes a timestamp." });
    var fileInput = h("input", { type: "file", id: "definitions-file", accept: "application/json,.json" });
    var preview = h("div", { id: "definitions-preview" });
    var confirmWrap = h("div", { id: "definitions-confirm-wrap", hidden: true });
    var lastText = null;

    /* Report what the export actually omits (strangeq.excluded_sections). */
    api(API + "/definitions").then(function (r) {
      if (r.status === 200 && r.data && r.data.strangeq) {
        var secs = r.data.strangeq.excluded_sections || [];
        exportNote.textContent = "Export is a subset (user credentials and policies are excluded); it includes a timestamp. Excluded sections: " +
          (secs.length ? secs.join(", ") : "none") + ".";
      }
    }, function () { /* the note stays on its default text */ });

    fileInput.addEventListener("change", function () {
      var file = fileInput.files && fileInput.files[0];
      if (!file) return;
      var reader = new FileReader();
      reader.onload = function () {
        lastText = String(reader.result);
        preview.replaceChildren(itemsLoading());
        confirmWrap.hidden = true;
        /* Dry-run preview: the SAME bytes go to ?dry_run=true and later to the
         * apply, so a preview cannot disagree with the real merge. */
        api(API + "/definitions?dry_run=true", { method: "POST", raw: lastText }).then(function (r) {
          if (r.status >= 400) { preview.replaceChildren(errorState(r.data)); return; }
          preview.replaceChildren(renderDefinitionsOutcome(r.data, true));
          confirmWrap.hidden = false;
        }, function () { preview.replaceChildren(errorState(null)); });
      };
      reader.readAsText(file);
    });

    var confirmBtn = h("button", { type: "button", id: "definitions-apply", text: "Confirm import" });
    confirmBtn.addEventListener("click", function () {
      if (lastText === null) return;
      confirmBtn.disabled = true;
      api(API + "/definitions", { method: "POST", raw: lastText }).then(function (r) {
        confirmBtn.disabled = false;
        if (r.status === 202) {
          /* Accepted as a background job: the merge has not completed, so never
           * show an outcome table or "Import complete". */
          preview.replaceChildren(asyncJobNotice(r.location));
          confirmWrap.hidden = true;
          invalidateAll();
          return;
        }
        if (r.status >= 400) { preview.replaceChildren(errorState(r.data)); return; }
        preview.replaceChildren(renderDefinitionsOutcome(r.data, false));
        confirmWrap.hidden = true;
        toast("Import complete", "ok");
        invalidateAll();
      }, function () { confirmBtn.disabled = false; preview.replaceChildren(errorState(null)); });
    });
    confirmWrap.appendChild(confirmBtn);

    container.replaceChildren(h("div", { class: "screen screen-definitions" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Definitions" })]),
      h("h2", { text: "Export" }),
      exportNote,
      exportLink,
      h("h2", { text: "Import" }),
      h("p", { class: "null", id: "definitions-atomic-note", text: "This import is not atomic; entities are applied one at a time and some may fail." }),
      h("p", { class: "null", text: "User and permission sections are not imported (BOB-6)." }),
      h("div", { class: "form-row" }, [fileInput]),
      preview,
      confirmWrap
    ]));
    return Promise.resolve();
  }

  function loadAbout(container) {
    return api(API + "/strangeq/version").then(function (r) {
      if (r.status === 403) { container.replaceChildren(permissionDenied(r.data)); return; }
      if (r.status >= 400) { container.replaceChildren(errorState(r.data)); return; }
      var v = r.data || {};
      state.capabilities = v.capabilities || state.capabilities;
      var caps = v.capabilities || {};
      var explanations = {
        queue_message_totals: "queue unacked/total counts (BOB-1)",
        channels: "channel listing with prefetch_count/transactional/state",
        bindings: "binding listing and create/delete",
        close_connection: "close a connection (connection.close 320 + reason)",
        user_crud: "user create/update/delete (BOB-6)",
        tag_capabilities: "tag-derived capability map",
        consumer_counts: "consumer channel/connection/unacked (BOB-8)",
        atomic_import: "atomic definitions import (BOB-10)",
        message_rates: "message rate statistics (BOB-2)",
        vhost_scoping: "multiple virtual hosts (BOB-9)",
        coherent_snapshot: "one coherent entity snapshot (BOB-11)",
        unauthenticated: "unauthenticated access"
      };
      var keys = Object.keys(caps).length ? Object.keys(caps) : Object.keys(explanations);
      var rows = keys.map(function (k) {
        var st = capabilityStatus(caps[k]);
        return h("tr", { role: "row" }, [
          h("td", { text: k }),
          h("td", null, [badge(st.label, st.kind)]),
          h("td", { text: explanations[k] || "" })
        ]);
      });
      var divergences = [
        ["D-1", "entity data is global; vhosts are not independent"],
        ["D-2", "single-node broker; no cluster, no HA"],
        ["D-3", "classic queues only; no quorum/stream/priority"],
        ["DIV-bindings", "bindings list source/destination/destination_type/routing_key/properties_key; create/delete use /api/bindings/{vhost}/e/... with properties_key as the canonical delete key"],
        ["DIV-close", "close connection sends connection.close 320 with the operator reason; the peer is force-closed only if it does not reply close-ok within 3 s"],
        ["DIV-channel", "channel prefetch_count/transactional are real lock-free atomics; state=closing is observable only while the channel is still registered"],
        ["DIV-channel-id", "GET /api/channels/{id} takes the canonical {connection}:{number} composite; a bare channel number is accepted only when it is unambiguous and is otherwise a deterministic 404 (tooling should use the composite form)"],
        ["DIV-15", "unanswerable health checks return 501, not a fake 200"],
        ["DIV-metrics-1", "two /metrics: on the telemetry listener /metrics is the unauthenticated Prometheus text; on the management listener /metrics is the SPA deep link. Different origins, different content"],
        ["DIV-metrics-2", "/api/metrics is a StrangeQ extension with no RabbitMQ counterpart; the JSON {text} wrapper is ours"],
        ["DIV-metrics-3", "a management Metrics-tab refresh increments promhttp_metric_handler_requests_total because ScrapeText uses the shared instrumented handler"],
        ["DIV-metrics-4", "the raw /metrics link is absent when metricsUrl is unset (telemetry disabled or a wildcard bind); the in-SPA matrix is same-origin and still works"],
        ["DIV-secure", "connection secure/ssl are derived from the transport (a *tls.Conn accepted socket), never from the SASL mechanism; auth_mechanism names the mechanism that authenticated and is empty when auth is disabled; SASL EXTERNAL is not accepted, so a non-TLS connection can never report secure=true"]
      ];
      container.replaceChildren(h("div", { class: "screen screen-about" }, [
        h("div", { class: "page-head" }, [h("h1", { text: "About / Capabilities" })]),
        h("dl", { class: "props" }, [
          h("dt", { text: "product" }), h("dd", { text: val(v.product) }),
          h("dt", { text: "version" }), h("dd", { text: val(v.version) }),
          h("dt", { text: "commit" }), h("dd", { text: val(v.commit) }),
          h("dt", { text: "api compatibility" }), h("dd", { text: val(v.api_compatibility) })
        ]),
        h("h2", { text: "Capabilities" }),
        h("table", { class: "data-table", role: "grid" }, [
          h("thead", null, [h("tr", null, [
            h("th", { text: "Capability" }), h("th", { text: "Status" }), h("th", { text: "Explanation" })
          ])]),
          h("tbody", null, rows)
        ]),
        h("h2", { text: "Divergences from RabbitMQ" }),
        h("table", { class: "data-table", role: "grid" }, [
          h("thead", null, [h("tr", null, [h("th", { text: "Id" }), h("th", { text: "Divergence" })])]),
          h("tbody", null, divergences.map(function (d) {
            return h("tr", { role: "row" }, [h("td", { text: d[0] }), h("td", { text: d[1] })]);
          }))
        ]),
        h("h2", { text: "Not implemented (no nav entry)" }),
        h("p", { text: "policies, operator policies, parameters, global parameters, feature flags, deprecated features, vhost limits, user limits, federation, shovels, plugins, runtime user/permission CRUD, clustering." })
      ]));
    });
  }

  function loadSearch(container, route) {
    var q = route.query.get("q") || "";
    if (!q) { container.replaceChildren(emptyState("Enter a search term.")); return Promise.resolve(); }
    var paths = [
      { type: "queues", path: API + "/queues" + qs({ name: q, page_size: 100 }) },
      { type: "exchanges", path: API + "/exchanges" + qs({ name: q, page_size: 100 }) },
      { type: "connections", path: API + "/connections" + qs({ name: q, page_size: 100 }) }
    ];
    return Promise.all(paths.map(function (p) { return api(p.path); })).then(function (rs) {
      var groups = [];
      var truncated = false;
      rs.forEach(function (r, i) {
        var items = itemsOf(r.data);
        if (items.length >= 100) truncated = true;
        if (items.length) {
          groups.push(h("h2", { text: paths[i].type }));
          groups.push(tableBlock([
            { key: "name", label: "Name", render: function (x) {
              var base = paths[i].type === "connections" ? "/connections/" : (paths[i].type === "queues" ? "/queues/" : "/exchanges/");
              return link(String(x.name), base + encodeURIComponent(x.name));
            } }
          ], items, {}));
        }
      });
      container.replaceChildren(h("div", { class: "screen screen-search" }, [
        h("div", { class: "page-head" }, [h("h1", { text: "Search" }), h("span", { class: "null", text: "results for " + q })]),
        truncated ? h("p", { class: "badge warn", text: "results truncated at 100 per type" }) : null,
        groups.length ? groups : emptyState("No matches.")
      ]));
    });
  }

  /* ------------------------------------------------------------- actions --- */

  /* validEntityName rejects an empty name or one containing whitespace, which
   * the broker would refuse (S6 name validation). */
  function validEntityName(name) {
    return name.length > 0 && !/\s/.test(name);
  }

  /* parseArguments turns "key=value" lines into an arguments object. Keys must
   * start with "x-" (S8); a malformed line or a bad key returns {error} so the
   * caller can refuse locally instead of sending a doomed request. */
  function parseArguments(text) {
    var out = {};
    var lines = String(text || "").split("\n");
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (!line) continue;
      var eq = line.indexOf("=");
      if (eq <= 0) return { error: "arguments must be key=value, one per line" };
      var key = line.slice(0, eq).trim();
      var value = line.slice(eq + 1).trim();
      if (key.indexOf("x-") !== 0) return { error: "argument keys must start with \"x-\" (got \"" + key + "\")" };
      var parsed = value;
      try { parsed = JSON.parse(value); } catch (e) { parsed = value; }
      out[key] = parsed;
    }
    return { arguments: out };
  }

  /* parseJSONObject parses a textarea holding a JSON object, returning {} for
   * empty input and null for malformed or non-object input (S7). */
  function parseJSONObject(text) {
    var t = String(text || "").trim();
    if (!t) return {};
    var parsed = null;
    try { parsed = JSON.parse(t); } catch (e) { return null; }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    return parsed;
  }

  function openDeclareExchange(route) {
    var nameInput = h("input", { type: "text", id: "declare-name", "aria-label": "Exchange name" });
    var typeSel = h("select", { id: "declare-type", "aria-label": "Exchange type" }, [
      option("direct", "direct"), option("fanout", "fanout"), option("topic", "topic"), option("headers", "headers")
    ]);
    var durable = h("input", { type: "checkbox", id: "declare-durable" }); durable.checked = true;
    var autoDelete = h("input", { type: "checkbox", id: "declare-auto-delete" });
    var internal = h("input", { type: "checkbox", id: "declare-internal" });
    var argsInput = h("textarea", { id: "declare-arguments", rows: "3", cols: "30", placeholder: "x-arguments=value (one per line)" });
    var err = h("p", { class: "error", id: "declare-error" });
    var body = h("div", null, [
      h("div", { class: "form-row" }, [h("label", { for: "declare-name", text: "Name" }), nameInput]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-type", text: "Type" }), typeSel]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-durable", text: "Durable" }), durable]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-auto-delete", text: "Auto-delete" }), autoDelete]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-internal", text: "Internal" }), internal]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-arguments", text: "Arguments" }), argsInput]),
      err
    ]);
    openModal({
      title: "Declare exchange",
      body: body,
      confirmLabel: "Declare",
      guard: function () { return validEntityName(nameInput.value.trim()); },
      onConfirm: function () {
        err.textContent = "";
        var parsed = parseArguments(argsInput.value);
        if (parsed.error) { err.textContent = parsed.error; return Promise.resolve(true); }
        return api(exchangePath("/", nameInput.value.trim()), {
          method: "PUT",
          body: { type: typeSel.value, durable: durable.checked, auto_delete: autoDelete.checked, internal: internal.checked, arguments: parsed.arguments }
        }).then(function (r) {
          if (r.status >= 400) { err.textContent = errorDetail(r.data); return true; }
          toast("Exchange declared", "ok");
          invalidateAll();
          navigate("/exchanges");
          return false;
        }, function () { err.textContent = "request failed"; return true; });
      }
    });
  }

  function openDeclareQueue(route) {
    var nameInput = h("input", { type: "text", id: "declare-queue-name", "aria-label": "Queue name" });
    var durable = h("input", { type: "checkbox", id: "declare-queue-durable" }); durable.checked = true;
    var autoDelete = h("input", { type: "checkbox", id: "declare-queue-auto-delete" });
    var argsInput = h("textarea", { id: "declare-queue-arguments", rows: "3", cols: "30", placeholder: "x-message-ttl=60000" });
    var err = h("p", { class: "error", id: "declare-error" });
    var body = h("div", null, [
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-name", text: "Name" }), nameInput]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-durable", text: "Durable" }), durable]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-auto-delete", text: "Auto-delete" }), autoDelete]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-arguments", text: "Arguments" }), argsInput]),
      h("p", { class: "null", text: "Only x-* argument keys are accepted. Supported: x-message-ttl, x-expires, x-dead-letter-exchange, x-dead-letter-routing-key, x-max-length, x-max-length-bytes and x-overflow. Priority and lazy queues are never available (D-3)." }),
      err
    ]);
    openModal({
      title: "Declare queue",
      body: body,
      confirmLabel: "Declare",
      guard: function () { return validEntityName(nameInput.value.trim()); },
      onConfirm: function () {
        err.textContent = "";
        var parsed = parseArguments(argsInput.value);
        if (parsed.error) { err.textContent = parsed.error; return Promise.resolve(true); }
        return api(queuePath("/", nameInput.value.trim()), {
          method: "PUT",
          body: { durable: durable.checked, auto_delete: autoDelete.checked, arguments: parsed.arguments }
        }).then(function (r) {
          if (r.status >= 400) { err.textContent = errorDetail(r.data); return true; }
          toast("Queue declared", "ok");
          invalidateAll();
          navigate("/queues");
          return false;
        }, function () { err.textContent = "request failed"; return true; });
      }
    });
  }

  function openPurge(queue) {
    var ack = h("input", { type: "checkbox", id: "purge-ack" });
    var countText = isNull(queue.messages) ? "all messages" : (queue.messages + " messages");
    var btn = h("button", { type: "button", class: "danger", text: "Purge" });
    openModal({
      title: "Purge queue " + queue.name,
      danger: true,
      body: h("div", null, [
        h("p", { class: "effect", text: "Purge " + countText + " from queue \u201c" + queue.name + "\u201d? This cannot be undone." }),
        h("label", { class: "effect", for: "purge-ack" }, [ack, h("span", { text: " I understand that messages will be permanently destroyed" })])
      ]),
      confirmLabel: "Purge",
      guard: function () { return ack.checked; },
      onConfirm: function () {
        return api(queuePath("/", queue.name) + "/contents", { method: "DELETE" }).then(function (r) {
          if (r.status === 202) {
            /* Accepted as a background job: no count is known yet, so never
             * claim "Purged N messages". */
            noticeToast(asyncJobNotice(r.location));
            invalidateAll();
            navigate("/queues/" + encodeURIComponent(queue.name));
            return false;
          }
          if (r.status >= 400) { toast("Purge failed: " + errorDetail(r.data), "error"); return false; }
          toast("Purged " + (r.purged || "0") + " messages from " + queue.name, "ok");
          invalidateAll();
          navigate("/queues/" + encodeURIComponent(queue.name));
          return false;
        }, function () { toast("Purge failed", "error"); return false; });
      }
    });
  }

  function openDeleteQueue(queue) {
    var input = h("input", { type: "text", id: "delete-confirm-input", "aria-label": "Type the queue name" });
    openModal({
      title: "Delete queue " + queue.name,
      danger: true,
      body: h("div", null, [
        h("p", { class: "effect", text: "Delete queue \u201c" + queue.name + "\u201d and all its messages? This cannot be undone." }),
        h("label", { for: "delete-confirm-input", text: "Type the queue name to confirm:" }),
        input
      ]),
      confirmLabel: "Delete",
      guard: function () { return input.value === queue.name; },
      initialFocus: input,
      onConfirm: function () {
        return api(queuePath("/", queue.name), { method: "DELETE" }).then(function (r) {
          if (r.status >= 400) { toast("Delete failed: " + errorDetail(r.data), "error"); return false; }
          toast("Deleted " + (r.deleted || "0") + " messages from " + queue.name, "ok");
          invalidateAll();
          navigate("/queues");
          return false;
        }, function () { toast("Delete failed", "error"); return false; });
      }
    });
  }

  function openDeleteExchange(exchange) {
    var input = h("input", { type: "text", id: "delete-confirm-input", "aria-label": "Type the exchange name" });
    openModal({
      title: "Delete exchange " + exchange.name,
      danger: true,
      body: h("div", null, [
        h("p", { class: "effect", text: "Delete exchange \u201c" + exchange.name + "\u201d? This cannot be undone." }),
        h("label", { for: "delete-confirm-input", text: "Type the exchange name to confirm:" }),
        input
      ]),
      confirmLabel: "Delete",
      guard: function () { return input.value === exchange.name; },
      initialFocus: input,
      onConfirm: function () {
        return api(exchangePath("/", exchange.name), { method: "DELETE" }).then(function (r) {
          if (r.status >= 400) { toast("Delete failed: " + errorDetail(r.data), "error"); return false; }
          toast("Exchange deleted", "ok");
          invalidateAll();
          navigate("/exchanges");
          return false;
        }, function () { toast("Delete failed", "error"); return false; });
      }
    });
  }

  /* openCreateBinding declares a new binding. The destination type is the
   * API's destination_type ("queue"/"exchange"); the path letter ("q"/"e") is
   * derived through bindingDtype so create and delete share one mapping. The
   * body carries the routing key and the x-* arguments (FR-API-091/092). */
  function openCreateBinding(route, vhost, prefill) {
    prefill = prefill || {};
    var sourceInput = h("input", { type: "text", id: "binding-source", value: prefill.source || "", "aria-label": "Source exchange" });
    var dtypeSel = h("select", { id: "binding-dest-type", "aria-label": "Destination type" }, [option("queue", "queue"), option("exchange", "exchange")]);
    if (prefill.destinationType) dtypeSel.value = prefill.destinationType;
    var destInput = h("input", { type: "text", id: "binding-destination", value: prefill.destination || "", "aria-label": "Destination" });
    var rkInput = h("input", { type: "text", id: "binding-routing-key", value: prefill.routingKey || "", "aria-label": "Routing key" });
    var argsInput = h("textarea", { id: "binding-arguments", rows: "3", cols: "30", placeholder: "x-arguments=value (one per line)" });
    var err = h("p", { class: "error", id: "binding-error" });
    var body = h("div", null, [
      h("div", { class: "form-row" }, [h("label", { for: "binding-source", text: "Source (exchange)" }), sourceInput]),
      h("div", { class: "form-row" }, [h("label", { for: "binding-dest-type", text: "Destination type" }), dtypeSel]),
      h("div", { class: "form-row" }, [h("label", { for: "binding-destination", text: "Destination" }), destInput]),
      h("div", { class: "form-row" }, [h("label", { for: "binding-routing-key", text: "Routing key" }), rkInput]),
      h("div", { class: "form-row" }, [h("label", { for: "binding-arguments", text: "Arguments" }), argsInput]),
      err
    ]);
    openModal({
      title: "Create binding",
      body: body,
      confirmLabel: "Create",
      guard: function () {
        var src = sourceInput.value.trim();
        var dst = destInput.value.trim();
        return validEntityName(dst) && !/\s/.test(src);
      },
      initialFocus: sourceInput,
      onConfirm: function () {
        err.textContent = "";
        var parsed = parseArguments(argsInput.value);
        if (parsed.error) { err.textContent = parsed.error; return Promise.resolve(true); }
        return api(bindingsPath(vhost, sourceInput.value.trim(), bindingDtype(dtypeSel.value), destInput.value.trim()), {
          method: "POST",
          body: { routing_key: rkInput.value, arguments: parsed.arguments }
        }).then(function (r) {
          if (r.status >= 400) { err.textContent = errorDetail(r.data); return true; }
          toast("Binding created", "ok");
          invalidateAll();
          navigate("/bindings");
          return false;
        }, function () { err.textContent = "request failed"; return true; });
      }
    });
  }

  /* openDeleteBinding removes one binding. The DELETE addresses the exact
   * synthesised properties_key, so an acknowledgement is required (a routing
   * change is destructive but does not delete messages). */
  function openDeleteBinding(vhost, b) {
    var ack = h("input", { type: "checkbox", id: "binding-delete-ack" });
    var desc = val(b.source) + " \u2192 " + val(b.destination) + " (" + val(b.destination_type) + ", routing key \u201c" + val(b.routing_key) + "\u201d)";
    openModal({
      title: "Delete binding",
      danger: true,
      body: h("div", null, [
        h("p", { class: "effect", text: "Delete binding " + desc + "?" }),
        h("label", { class: "effect", for: "binding-delete-ack" }, [ack, h("span", { text: " I understand this removes the binding" })])
      ]),
      confirmLabel: "Delete binding",
      guard: function () { return ack.checked; },
      onConfirm: function () {
        return api(bindingsPath(vhost, b.source, bindingDtype(b.destination_type), b.destination, b.properties_key), { method: "DELETE" }).then(function (r) {
          if (r.status >= 400) { toast("Delete binding failed: " + errorDetail(r.data), "error"); return false; }
          toast("Binding deleted", "ok");
          invalidateAll();
          navigate("/bindings");
          return false;
        }, function () { toast("Delete binding failed", "error"); return false; });
      }
    });
  }

  function openCloseConnection(conn) {
    var reason = h("input", { type: "text", id: "close-reason", "aria-label": "Optional close reason", placeholder: "optional operator reason" });
    openModal({
      title: "Close connection " + conn.name,
      danger: true,
      body: h("div", null, [
        h("p", { class: "effect", text: "Close connection \u201c" + conn.name + "\u201d to " + val(conn.peer_host) + ":" + val(conn.peer_port) + "?" }),
        h("div", { class: "form-row" }, [h("label", { for: "close-reason", text: "Reason (sent with connection.close 320)" }), reason]),
        h("p", { class: "error", text: "This sends connection.close 320 with the operator reason; the connection is force-closed only if the peer does not reply close-ok within a few seconds." })
      ]),
      confirmLabel: "Close connection",
      initialFocus: reason,
      onConfirm: function () {
        var headers = {};
        if (reason.value) headers["X-Reason"] = reason.value;
        return api(API + "/connections/" + encodeURIComponent(conn.name), { method: "DELETE", headers: headers }).then(function (r) {
          if (r.status >= 400) { toast("Close failed: " + errorDetail(r.data), "error"); return false; }
          toast("Connection closed", "ok");
          invalidateAll();
          navigate("/connections");
          return false;
        }, function () { toast("Close failed", "error"); return false; });
      }
    });
  }

  function openPublish(path, label) {
    var routingKey = h("input", { type: "text", id: "publish-routing-key", value: "" });
    var payload = h("textarea", { id: "publish-payload", rows: "5", cols: "40" });
    var encoding = h("select", { id: "publish-encoding" }, [option("string", "string"), option("base64", "base64")]);
    var props = h("textarea", { id: "publish-properties", rows: "3", cols: "40", placeholder: '{"headers": {"x-trace": "abc"}}' });
    var result = h("div", { id: "publish-result" });
    openModal({
      title: "Publish to " + label,
      confirmLabel: "Publish",
      body: h("div", null, [
        h("p", { class: "error", text: "This panel is for troubleshooting only; it is not a production publisher." }),
        h("div", { class: "form-row" }, [h("label", { for: "publish-routing-key", text: "Routing key" }), routingKey]),
        h("div", { class: "form-row" }, [h("label", { for: "publish-payload", text: "Payload" }), payload]),
        h("div", { class: "form-row" }, [h("label", { for: "publish-encoding", text: "Encoding" }), encoding]),
        h("div", { class: "form-row" }, [h("label", { for: "publish-properties", text: "Properties" }), props]),
        result
      ]),
      initialFocus: routingKey,
      onConfirm: function () {
        var properties = parseJSONObject(props.value);
        if (properties === null) {
          result.replaceChildren(errorState({ error: "bad_request", reason: "properties must be a JSON object" }));
          return Promise.resolve(true);
        }
        return api(path + "/publish", {
          method: "POST",
          body: { properties: properties, routing_key: routingKey.value, payload: payload.value, payload_encoding: encoding.value }
        }).then(function (r) {
          result.replaceChildren(h("pre", { class: "payload", text: JSON.stringify(r.data) }));
          toast("Publish result: " + JSON.stringify(r.data), r.status >= 400 ? "error" : "ok");
          return true;
        }, function () { result.replaceChildren(errorState(null)); return true; });
      }
    });
  }

  function openGet(queue) {
    var count = h("input", { type: "number", id: "get-count", value: "1", min: "1", max: "100" });
    var ackmode = h("select", { id: "get-ackmode" }, [
      option("ack_requeue_true", "ack_requeue_true"),
      option("reject_requeue_true", "reject_requeue_true"),
      option("ack_requeue_false", "ack_requeue_false"),
      option("reject_requeue_false", "reject_requeue_false")
    ]);
    var encoding = h("select", { id: "get-encoding" }, [option("auto", "auto"), option("base64", "base64")]);
    var out = h("div", { id: "get-result" });
    openModal({
      title: "Get messages from " + queue.name,
      confirmLabel: "Get",
      body: h("div", null, [
        h("div", { class: "form-row" }, [h("label", { for: "get-count", text: "Count" }), count]),
        h("div", { class: "form-row" }, [h("label", { for: "get-ackmode", text: "Ackmode" }), ackmode]),
        h("div", { class: "form-row" }, [h("label", { for: "get-encoding", text: "Encoding" }), encoding]),
        h("p", { class: "null", text: "ack_requeue_false / reject_requeue_false are destructive; requeue-default is safe." }),
        out
      ]),
      initialFocus: count,
      onConfirm: function () {
        return api(queuePath("/", queue.name) + "/get", {
          method: "POST",
          body: { count: Number(count.value), ackmode: ackmode.value, encoding: encoding.value }
        }).then(function (r) {
          if (r.status >= 400) { out.replaceChildren(errorState(r.data)); return true; }
          var msgs = itemsOf(r.data);
          out.replaceChildren(h("div", null, msgs.map(function (m) {
            var full = m.payload;
            var body = full;
            var truncated = false;
            if (typeof body === "string" && body.length > 512) { body = body.slice(0, 512); truncated = true; }
            return h("div", { class: "fieldset" }, [
              h("p", { text: "encoding: " + val(m.payload_encoding) + ", routing_key: " + val(m.routing_key) + ", redelivered: " + val(m.redelivered) }),
              h("pre", { class: "payload", text: val(body) }),
              truncated ? h("p", { class: "stat-partial", text: "truncated" }) : null,
              copyText(isNull(full) ? "" : String(full), "Copy body")
            ]);
          })));
          return true;
        }, function () { out.replaceChildren(errorState(null)); return true; });
      }
    });
  }

  /* ----------------------------------------------------------- metrics ----- */

  /* Hard caps (spec mgmt-07 §6.3): truthful truncation, never a partial matrix
   * presented as complete. */
  var METRICS_MAX_BYTES = 8 * 1024 * 1024;
  var METRICS_MAX_FAMILIES = 400;
  var METRICS_MAX_SERIES = 2000;
  var METRICS_MAX_SERIES_PER_FAMILY = 200;

  /* Ordered prefix table (spec §4.3): first prefix match wins; anything left
   * lands in "Other" so a new family is never dropped. */
  var METRIC_GROUPS = [
    ["Broker \u2014 connections", ["amqp_connections_", "amqp_connection_errors_"]],
    ["Broker \u2014 channels", ["amqp_channels_", "amqp_channel_errors_"]],
    ["Broker \u2014 consumers", ["amqp_consumers_", "amqp_consumer_"]],
    ["Broker \u2014 exchanges", ["amqp_exchanges_"]],
    ["Broker \u2014 queues", ["amqp_queues_", "amqp_queue_"]],
    ["Broker \u2014 messages", ["amqp_messages_", "amqp_message_", "amqp_deliveries_", "amqp_delivery_"]],
    ["Broker \u2014 hot cache", ["amqp_hot_cache_"]],
    ["Back-pressure", ["amqp_publisher_par", "amqp_publish_lease_", "amqp_publish_refused_", "amqp_depth_gate", "amqp_reader_paused"]],
    ["Durability \u2014 WAL", ["amqp_wal_"]],
    ["Durability \u2014 segments", ["amqp_segment_"]],
    ["Recovery", ["amqp_recovery_", "amqp_unsafe_recovery_"]],
    ["Resources", ["amqp_disk_", "amqp_memory_", "amqp_goroutines_", "amqp_file_descriptors_", "amqp_resource_alarm_"]],
    ["Authentication", ["amqp_auth_failures_"]],
    ["Transactions", ["amqp_transactions_"]],
    ["Server", ["amqp_server_", "amqp_build_info", "amqp_panics_", "amqp_shutdown_", "amqp_storage_close_"]],
    ["Management surface", ["amqp_management_"]],
    ["Go runtime", ["go_"]],
    ["Process", ["process_"]],
    ["Scrape handler", ["promhttp_"]]
  ];

  function metricGroup(name) {
    for (var i = 0; i < METRIC_GROUPS.length; i++) {
      var prefixes = METRIC_GROUPS[i][1];
      for (var j = 0; j < prefixes.length; j++) {
        if (name.indexOf(prefixes[j]) === 0) return METRIC_GROUPS[i][0];
      }
    }
    return "Other";
  }

  function unescapeLabel(v) {
    var out = "";
    for (var i = 0; i < v.length; i++) {
      var c = v.charAt(i);
      if (c === "\\" && i + 1 < v.length) {
        var nx = v.charAt(i + 1);
        if (nx === "\\") { out += "\\"; i++; continue; }
        if (nx === '"') { out += '"'; i++; continue; }
        if (nx === "n") { out += "\n"; i++; continue; }
        out += c;
        continue;
      }
      out += c;
    }
    return out;
  }

  /* parseSampleLine parses "name{k=\"v\",k2=\"v2\"} value". It returns null for
   * a malformed line so the caller can skip it rather than invent data. */
  function parseSampleLine(line) {
    var i = 0, n = line.length;
    while (i < n && line.charAt(i) !== "{" && line.charAt(i) !== " " && line.charAt(i) !== "\t") i++;
    var name = line.slice(0, i);
    if (!name) return null;
    var labels = {};
    if (line.charAt(i) === "{") {
      i++;
      while (i < n && line.charAt(i) !== "}") {
        while (i < n && (line.charAt(i) === " " || line.charAt(i) === "\t" || line.charAt(i) === ",")) i++;
        if (line.charAt(i) === "}") break;
        var keyStart = i;
        while (i < n && line.charAt(i) !== "=") i++;
        var key = line.slice(keyStart, i).trim();
        i++;
        if (line.charAt(i) !== '"') return null;
        i++;
        var buf = "";
        while (i < n && line.charAt(i) !== '"') {
          if (line.charAt(i) === "\\" && i + 1 < n) { buf += line.charAt(i) + line.charAt(i + 1); i += 2; continue; }
          buf += line.charAt(i);
          i++;
        }
        i++;
        if (key) labels[key] = unescapeLabel(buf);
      }
      i++;
    }
    while (i < n && (line.charAt(i) === " " || line.charAt(i) === "\t")) i++;
    var rest = line.slice(i).trim();
    if (rest === "") return null;
    var sp = rest.indexOf(" ");
    var raw = sp < 0 ? rest : rest.slice(0, sp);
    return { name: name, labels: labels, raw: raw, value: parseFloat(raw) };
  }

  /* parseProm is a pure, dependency-free Prometheus text parser (spec §4). It
   * has no DOM access and is exposed on window.SQ for the headless tests. */
  function parseProm(text) {
    var lines = String(text).split("\n");
    var help = {}, type = {};
    var incoming = [];
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      if (line === "" || line.charAt(0) === "\r") continue;
      if (line.charAt(0) === "#") {
        var m = line.match(/^#\s+(HELP|TYPE)\s+([a-zA-Z_:][a-zA-Z0-9_:]*)\s?(.*)$/);
        if (m) {
          if (m[1] === "HELP") help[m[2]] = m[3];
          else type[m[2]] = m[3].trim();
        }
        continue;
      }
      var s = parseSampleLine(line);
      if (s) incoming.push(s);
    }
    var byName = {};
    var order = [];
    for (var j = 0; j < incoming.length; j++) {
      var smp = incoming[j];
      var fname = smp.name;
      if (!type[fname]) {
        var base = fname.replace(/^(.+?)(_bucket|_sum|_count)$/, "$1");
        if (base !== fname && (type[base] === "histogram" || type[base] === "summary")) fname = base;
      }
      var fam = byName[fname];
      if (!fam) {
        var declared = type[fname] || (/_total$/.test(fname) ? "counter" : "untyped");
        fam = { name: fname, type: declared, help: help[fname] || "", group: metricGroup(fname), series: 0, samples: [] };
        byName[fname] = fam;
        order.push(fname);
      }
      fam.samples.push(smp);
      fam.series = fam.samples.length;
    }
    var families = order.map(function (name) { return byName[name]; });
    families.sort(function (a, b) { return a.name < b.name ? -1 : (a.name > b.name ? 1 : 0); });
    var totalSeries = 0;
    for (var k = 0; k < families.length; k++) totalSeries += families[k].series;
    return { families: families, familyCount: families.length, totalSeries: totalSeries, bytes: String(text).length };
  }

  /* Deterministic formatting (spec §4.2): no toLocaleString anywhere. */
  function groupInt(n) {
    var v = Number(n);
    if (isNaN(v) || !isFinite(v)) return String(n);
    if (Math.floor(v) !== v) return String(v);
    var s = String(v);
    var neg = s.charAt(0) === "-";
    if (neg) s = s.slice(1);
    var out = "";
    var c = 0;
    for (var i = s.length - 1; i >= 0; i--) {
      out = s.charAt(i) + out;
      c++;
      if (c % 3 === 0 && i > 0) out = "," + out;
    }
    return (neg ? "-" : "") + out;
  }

  function fmtRate(v) {
    if (v === null || v === undefined || typeof v !== "number" || !isFinite(v)) return DASH;
    return v.toFixed(2) + "/s";
  }

  function fmtBytes(v) {
    if (v === null || v === undefined) return DASH;
    var n = Number(v);
    if (isNaN(n) || !isFinite(n)) return String(v);
    var units = ["B", "KiB", "MiB", "GiB", "TiB"];
    var x = Math.abs(n);
    var i = 0;
    while (x >= 1024 && i < units.length - 1) { x = x / 1024; i++; }
    var mag = i === 0 ? groupInt(Math.round(n)) : x.toFixed(1);
    return mag + " " + units[i];
  }

  function fmtDurationStr(seconds) {
    if (seconds === null || seconds === undefined) return DASH;
    var s = Number(seconds);
    if (isNaN(s) || !isFinite(s)) return String(seconds);
    if (s < 60) return s + "s";
    if (s < 3600) return Math.floor(s / 60) + "m" + (s % 60) + "s";
    if (s < 86400) return Math.floor(s / 3600) + "h" + Math.floor((s % 3600) / 60) + "m";
    return Math.floor(s / 86400) + "d" + Math.floor((s % 86400) / 3600) + "h";
  }

  /* The vital-stat tiles (spec §5.1), rendered in declaration order and grouped
   * into three rows. Every tile is a selector evaluated against the parsed
   * text; nothing here is a literal metric value. */
  var VITALS = [
    { id: "pub_rate", label: "Messages in", group: "Throughput", sel: "rate(amqp_messages_published_total)", render: "rate", sev: null },
    { id: "deliv_rate", label: "Messages out", group: "Throughput", sel: "rate(amqp_messages_delivered_total)", render: "rate", sev: null },
    { id: "pub_bps", label: "Bytes in", group: "Throughput", sel: "rate(amqp_messages_published_bytes_total)", render: "bytesrate", sev: null },
    { id: "deliv_bps", label: "Bytes out", group: "Throughput", sel: "rate(amqp_messages_delivered_bytes_total)", render: "bytesrate", sev: null },
    { id: "ack_rate", label: "Acked", group: "Throughput", sel: "rate(amqp_messages_acknowledged_total)", render: "rate", sev: null },
    { id: "uptime", label: "Uptime", group: "Throughput", sel: "amqp_server_uptime_seconds", render: "duration", sev: null },

    { id: "connections", label: "Connections", group: "Topology", sel: "amqp_connections_total", render: "count", sev: null },
    { id: "channels", label: "Channels", group: "Topology", sel: "amqp_channels_total", render: "count", sev: null },
    { id: "consumers", label: "Consumers", group: "Topology", sel: "amqp_consumers_total", render: "count", sev: null },
    { id: "queues", label: "Queues", group: "Topology", sel: "amqp_queues_total", render: "count", sev: null },
    { id: "exchanges", label: "Exchanges", group: "Topology", sel: "amqp_exchanges_total", render: "count", sev: null },
    { id: "ready", label: "Messages ready", group: "Topology", sel: "sum(amqp_queue_messages_ready)", render: "count", sev: null },
    { id: "max_depth", label: "Max queue depth", group: "Topology", sel: "max(amqp_queue_depth_high_water)", render: "count", sev: null },
    { id: "inflight", label: "In flight", group: "Topology", sel: "amqp_messages_inflight", render: "count", sev: null },
    { id: "parked_now", label: "Publishers parked", group: "Topology", sel: "amqp_publisher_parked_current", render: "count", sev: "warn" },
    { id: "depth_gated", label: "Depth-gated conns", group: "Topology", sel: "amqp_depth_gated_current", render: "count", sev: "warn" },
    { id: "reader_paused", label: "Readers paused", group: "Topology", sel: "amqp_reader_paused_current", render: "count", sev: "warn" },

    { id: "alarms", label: "Resource alarms", group: "Health", sel: "max(amqp_resource_alarm_active)", render: "alarm", sev: "alarm" },
    { id: "unsafe_artifacts", label: "Unsafe-recovery discards", group: "Health", sel: "amqp_unsafe_recovery_discarded_artifacts", render: "count", sev: "bad" },
    { id: "recovery_faults", label: "Recovery faults", group: "Health", sel: "sum(amqp_recovery_faults_total{outcome!=\"benign\"})", render: "count", sev: "bad" },
    { id: "recovery_discarded", label: "Recovery discards", group: "Health", sel: "sum(amqp_recovery_discarded_records_total)", render: "count", sev: "bad" },
    { id: "unroutable", label: "Unroutable", group: "Health", sel: "amqp_messages_unroutable_total", render: "count", sev: "warn" },
    { id: "refused", label: "Publish refused", group: "Health", sel: "sum(amqp_publish_refused_total)", render: "count", sev: "bad" },
    { id: "dropped", label: "Deliveries dropped", group: "Health", sel: "sum(amqp_deliveries_dropped_total)", render: "count", sev: "bad" },
    { id: "redelivered", label: "Redelivered", group: "Health", sel: "amqp_messages_redelivered_total", render: "count", sev: "warn" },
    { id: "rejected", label: "Rejected", group: "Health", sel: "amqp_messages_rejected_total", render: "count", sev: "warn" },
    { id: "conn_errors", label: "Connection errors", group: "Health", sel: "sum(amqp_connection_errors_total)", render: "count", sev: "bad" },
    { id: "chan_errors", label: "Channel errors", group: "Health", sel: "sum(amqp_channel_errors_total)", render: "count", sev: "bad" },
    { id: "auth_failures", label: "Auth failures", group: "Health", sel: "sum(amqp_auth_failures_total)", render: "count", sev: "warn" },
    { id: "wal_write_errors", label: "WAL write errors", group: "Health", sel: "amqp_wal_write_errors_total", render: "count", sev: "bad" },
    { id: "wal_fsync_errors", label: "WAL fsync errors", group: "Health", sel: "amqp_wal_fsync_errors_total", render: "count", sev: "bad" },
    { id: "wal_full_flush", label: "WAL full flushes", group: "Health", sel: "sum(amqp_wal_full_flush_total{site!~\"boot_tail|boot_mkdir|close\"})", render: "count", sev: "bad" },
    { id: "wal_files", label: "WAL files", group: "Health", sel: "amqp_wal_files", render: "count", sev: null },
    { id: "segment_read_errors", label: "Segment read errors", group: "Health", sel: "amqp_segment_read_errors_total", render: "count", sev: "bad" },
    { id: "segment_fsync_errors", label: "Segment fsync errors", group: "Health", sel: "amqp_segment_fsync_errors_total", render: "count", sev: "bad" },
    { id: "segment_compaction_failures", label: "Segment compaction withdrawn", group: "Health", sel: "amqp_segment_compaction_failures_total", render: "count", sev: "bad" },
    { id: "panics", label: "Recovered panics", group: "Health", sel: "sum(amqp_panics_total)", render: "count", sev: "bad" },
    { id: "storage_close_errors", label: "Storage close errors", group: "Health", sel: "amqp_storage_close_errors_total", render: "count", sev: "bad" },
    { id: "shutdown_incomplete", label: "Incomplete shutdowns", group: "Health", sel: "amqp_shutdown_total{result=\"incomplete\"}", render: "count", sev: "bad" },
    { id: "mgmt_5xx", label: "Management 5xx", group: "Health", sel: "sum(amqp_management_requests_total{status_class=\"5\"})", render: "count", sev: "bad" },
    { id: "mgmt_rate_limited", label: "Management rate-limited", group: "Health", sel: "amqp_management_rate_limited_total", render: "count", sev: "bad" },
    { id: "lease_leaked", label: "Publish leases leaked", group: "Health", sel: "amqp_publish_lease_leaked_total", render: "count", sev: "bad" },
    { id: "disk_free", label: "Disk free", group: "Health", sel: "amqp_disk_free_bytes", render: "bytes", sev: null },
    { id: "memory_used", label: "Memory used", group: "Health", sel: "amqp_memory_used_bytes", render: "bytes", sev: null },
    { id: "goroutines", label: "Goroutines", group: "Health", sel: "amqp_goroutines_total", render: "count", sev: null }
  ];

  function parseSelector(selText) {
    var out = { agg: "none", rate: false, name: "", filter: null };
    var s = String(selText).trim();
    var m = s.match(/^(rate|sum|max)\((.*)\)$/);
    if (m) {
      if (m[1] === "rate") out.rate = true;
      else out.agg = m[1];
      s = m[2].trim();
    }
    var fm = s.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{(.*)\})?$/);
    if (!fm) return null;
    out.name = fm[1];
    if (fm[2] !== undefined && fm[2] !== null && fm[2] !== "") {
      var lm = fm[2].match(/^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*$/);
      if (lm) {
        out.filter = { label: lm[1], op: lm[2], value: unescapeLabel(lm[3]) };
        if (out.filter.op === "=~" || out.filter.op === "!~") {
          /* A Prometheus regex matcher is fully anchored. Compile it once here
           * so a malformed pattern is a parse-time fact (filter.invalid), not a
           * per-sample false that silently selects nothing and would render a
           * fabricated present zero. */
          try { out.filter.re = new RegExp("^(?:" + out.filter.value + ")$"); }
          catch (e) { out.filter.invalid = true; }
        }
      }
    }
    return out;
  }

  function familyByName(model, name) {
    if (!model) return null;
    for (var i = 0; i < model.families.length; i++) {
      if (model.families[i].name === name) return model.families[i];
    }
    return null;
  }

  /* labelMatches applies one label matcher to a sample's value, tri-state: true
   * selects the sample, false does not, and undefined means the matcher itself
   * is unusable (a malformed regex). A Prometheus regex matcher is fully
   * anchored: it matches the whole label value, never a substring, so
   * site!~"close" excludes exactly "close" and not "segment_close". An invalid
   * pattern returns undefined so the tile degrades to unknown instead of
   * selecting nothing and being read as a present zero. */
  function labelMatches(filter, v) {
    if (filter.op === "=") return v === filter.value;
    if (filter.op === "!=") return v !== filter.value;
    if (filter.invalid) return undefined;
    var hit = filter.re.test(v === undefined || v === null ? "" : v);
    return filter.op === "!~" ? !hit : hit;
  }

  /* matchingSamples returns the selected samples, or null when any sample's
   * matcher is unusable (undefined), so the caller can tell "selected nothing"
   * from "could not evaluate". */
  function matchingSamples(fam, filter) {
    var out = [];
    for (var i = 0; i < fam.samples.length; i++) {
      var smp = fam.samples[i];
      if (filter) {
        var m = labelMatches(filter, smp.labels[filter.label]);
        if (m === undefined) return null;
        if (!m) continue;
      }
      out.push(smp);
    }
    return out;
  }

  function buildSamples(model, at) {
    var out = {};
    for (var i = 0; i < model.families.length; i++) {
      var f = model.families[i];
      var sum = 0;
      for (var j = 0; j < f.samples.length; j++) {
        if (!isFinite(f.samples[j].value)) { sum = NaN; break; }
        sum += f.samples[j].value;
      }
      out[f.name] = { value: sum, at: at };
    }
    return out;
  }

  /* evalTile returns {found, value}: found is false when the family is absent
   * or the arithmetic is undefined, so the tile renders an em-dash, never a
   * fabricated zero or Infinity (spec §5.2). */
  function evalTile(model, selText, ctx) {
    var sel = parseSelector(selText);
    if (!sel) return { found: false };
    if (sel.rate) {
      var p = ctx.prev[sel.name];
      var c = ctx.curr[sel.name];
      if (!p || !c) return { found: false };
      if (!isFinite(p.value) || !isFinite(c.value)) return { found: false };
      var dt = (c.at - p.at) / 1000;
      if (dt <= 0) return { found: false };
      if (c.value < p.value) return { found: false };
      return { found: true, value: (c.value - p.value) / dt };
    }
    var fam = familyByName(model, sel.name);
    if (!fam) return { found: false };
    var matches = matchingSamples(fam, sel.filter);
    if (matches === null) {
      /* The matcher itself is unusable (a malformed regex): the family is
       * present but no truthful value can be computed, so the tile stays
       * unknown — never a fabricated present zero. */
      return { found: false, invalid: true, pattern: sel.filter.value };
    }
    if (!matches.length) {
      /* The family IS emitted; the filter selects none of its series. Summed
       * over an empty set the aggregate is a defined zero, so a filtered sum
       * renders a present 0 (ok) rather than an unknown dash — which is what
       * keeps a traffic-path-only counter truthful when the broker
       * pre-initializes the excluded sites. Any other selector, and an absent
       * family, stays unknown. */
      if (sel.agg === "sum" && sel.filter) return { found: true, value: 0 };
      return { found: false };
    }
    var acc = 0;
    for (var i = 0; i < matches.length; i++) {
      if (!isFinite(matches[i].value)) return { found: false };
      if (sel.agg === "max") acc = i === 0 ? matches[i].value : Math.max(acc, matches[i].value);
      else acc += matches[i].value;
    }
    return { found: true, value: acc };
  }

  function renderTile(kind, v) {
    if (kind === "rate") return fmtRate(v);
    if (kind === "bytesrate") return fmtBytes(v) + "/s";
    if (kind === "bytes") return fmtBytes(v);
    if (kind === "duration") return fmtDurationStr(v);
    return groupInt(v);
  }

  function computeTiles(model, prev, curr) {
    var out = [];
    for (var i = 0; i < VITALS.length; i++) {
      var t = VITALS[i];
      var ev = evalTile(model, t.sel, { prev: prev, curr: curr });
      var state = "unknown";
      var text = DASH;
      var title = "";
      if (ev.invalid) {
        /* An unusable selector pattern is unknown, never a fabricated value:
         * the tile keeps the em-dash and names the offending pattern. */
        title = 'invalid selector pattern "' + ev.pattern + '": the tile is unknown, never a fabricated value';
      } else if (ev.found) {
        if (t.render === "alarm") {
          state = ev.value === 1 ? "bad" : "ok";
          text = ev.value === 1 ? "set" : "none";
        } else {
          /* The WAL full-flush alarm is written for barrier mode, where a
           * traffic-path full flush must stay 0. Under durability: flush every
           * confirm is a full drive-cache flush, so the count is expected to
           * climb and is rendered without an alarm. */
          var sev = t.sev;
          if (t.id === "wal_full_flush" && DURABILITY === "flush") sev = null;
          state = (sev && ev.value > 0) ? sev : "ok";
          text = renderTile(t.render, ev.value);
        }
      }
      out.push({ id: t.id, label: t.label, group: t.group, state: state, text: text, title: title });
    }
    return out;
  }

  function metricCardEl(t) {
    return h("div", { class: "card", dataset: { tile: t.id, severity: "unknown" } }, [
      h("div", { class: "card-value", id: "metrics-tile-" + t.id, text: DASH, title: "not emitted by this broker" }),
      h("div", { class: "card-label", text: t.label }),
      h("span", { class: "card-sev", text: DASH })
    ]);
  }

  function pad2(n) { return (n < 10 ? "0" : "") + n; }

  function buildMetricsSkeleton(container, route) {
    var groups = ["Throughput", "Topology", "Health"];
    var rows = groups.map(function (g) {
      var cards = h("div", { class: "cards", dataset: { group: g } });
      VITALS.forEach(function (t) { if (t.group === g) cards.appendChild(metricCardEl(t)); });
      return h("div", { class: "card-group" }, [h("h2", { text: g }), cards]);
    });

    var filter = h("input", {
      id: "metrics-filter", type: "search", "aria-label": "Filter metric families",
      placeholder: "Filter families by name", autocomplete: "off"
    });
    filter.value = state.metrics.filter || "";
    filter.addEventListener("input", function () {
      state.metrics.filter = filter.value;
      renderMetricsFamilies(container, state.metrics.model, isRawView());
    });
    var count = h("span", { id: "metrics-filter-count", class: "null" });
    var toolbar = h("form", { class: "toolbar" }, [filter, count]);
    toolbar.addEventListener("submit", function (ev) { ev.preventDefault(); });

    var toggle = h("button", { type: "button", id: "metrics-view-toggle", text: "Raw text" });
    toggle.addEventListener("click", function () {
      navigate(listHref(state.route, { view: isRawView() ? "" : "raw" }));
    });

    var rawLink = METRICS_URL ? h("a", {
      id: "metrics-raw-endpoint", href: METRICS_URL, target: "_blank", rel: "noopener",
      title: "Open the raw Prometheus endpoint"
    }, [h("button", { type: "button", text: "Raw endpoint" })]) : null;

    var screen = h("div", { class: "screen screen-metrics", id: "metrics-screen" }, [
      h("div", { class: "page-head" }, [
        h("h1", { text: "Metrics" }),
        h("span", { class: "spacer" }),
        h("span", { id: "metrics-scraped", class: "null" }),
        rawLink,
        toggle
      ]),
      h("div", { id: "metrics-notice" }),
      h("div", { id: "metrics-truncation", hidden: true }),
      h("div", { id: "metrics-vitals" }, rows),
      toolbar,
      h("div", { id: "metrics-families" }),
      h("div", { id: "metrics-raw", hidden: true }, [h("pre", { class: "payload" })])
    ]);
    container.replaceChildren(screen);
  }

  function ensureMetricsSkeleton(container, route) {
    if (!container.querySelector("#metrics-screen")) buildMetricsSkeleton(container, route);
  }

  function isRawView() {
    return !!(state.route && state.route.query && state.route.query.get("view") === "raw");
  }

  function setMetricsNotice(container, text, kind) {
    var el = container.querySelector("#metrics-notice");
    if (!el) return;
    el.className = kind === "danger" ? "notice permission" : (kind === "warn" ? "notice" : "notice empty");
    el.dataset.severity = kind || "ok";
    el.replaceChildren(h("p", { text: text }));
  }

  function renderMetricsNotice(container, tiles, model) {
    var el = container.querySelector("#metrics-notice");
    if (!el) return;
    if (!tiles) { el.className = "notice empty"; el.replaceChildren(); return; }
    var hasAmqp = model && model.families.some(function (f) { return f.name.indexOf("amqp_") === 0; });
    if (!hasAmqp) {
      el.className = "notice empty";
      el.dataset.severity = "unknown";
      el.replaceChildren(h("p", { text: "the endpoint emitted no amqp_* families; there is nothing truthful to show." }));
      return;
    }
    var bad = tiles.filter(function (t) { return t.state === "bad"; });
    var warn = tiles.filter(function (t) { return t.state === "warn"; });
    if (bad.length) {
      el.className = "notice permission";
      el.dataset.severity = "bad";
      el.replaceChildren(h("p", { class: "error", text: bad.length + " vital signal(s) need attention: " + bad.map(function (t) { return t.label; }).join(", ") }));
      return;
    }
    if (warn.length) {
      el.className = "notice";
      el.dataset.severity = "warn";
      el.replaceChildren(h("p", { text: warn.length + " vital signal(s) are noise-grade: " + warn.map(function (t) { return t.label; }).join(", ") }));
      return;
    }
    el.className = "notice empty";
    el.dataset.severity = "ok";
    var unknown = tiles.filter(function (t) { return t.state === "unknown"; });
    el.replaceChildren(h("p", { text: "No bad vital signals; " + unknown.length + " of " + tiles.length + " signals unknown." }));
  }

  function renderMetricsVitals(container, tiles) {
    if (!tiles) return;
    for (var i = 0; i < tiles.length; i++) {
      var t = tiles[i];
      var card = container.querySelector('.card[data-tile="' + t.id + '"]');
      if (!card) continue;
      card.dataset.severity = t.state;
      var value = card.querySelector(".card-value");
      if (value) {
        value.textContent = t.text;
        value.title = (t.state === "unknown") ? (t.title || "not emitted by this broker") : "";
      }
      var sev = card.querySelector(".card-sev");
      if (sev) sev.textContent = t.state === "unknown" ? DASH : t.state;
    }
  }

  function metricFamilyNode(f) {
    var header = h("h3", null, [
      h("span", { text: f.name }),
      h("span", { class: "badge", text: f.type }),
      h("span", { class: "metric-group", text: f.group }),
      h("span", { class: "null", text: " " + f.series + " series" })
    ]);
    var unlabelled = f.series === 1 && Object.keys(f.samples[0].labels).length === 0;
    var body;
    var rendered = 0;
    var capped = false;
    if (unlabelled) {
      body = h("span", { class: "metric-single-value", text: f.samples[0].raw });
      rendered = 1;
    } else {
      var limit = Math.min(f.samples.length, METRICS_MAX_SERIES_PER_FAMILY);
      if (f.samples.length > METRICS_MAX_SERIES_PER_FAMILY) capped = true;
      var rows = [];
      for (var i = 0; i < limit; i++) {
        var smp = f.samples[i];
        var labelText = Object.keys(smp.labels).sort().map(function (k) {
          return k + '="' + smp.labels[k] + '"';
        }).join(", ");
        rows.push(h("tr", { role: "row" }, [h("td", { text: labelText }), h("td", { text: smp.raw })]));
      }
      body = h("table", { class: "data-table", role: "grid" }, [
        h("thead", null, [h("tr", null, [h("th", { text: "Labels" }), h("th", { text: "Value" })])]),
        h("tbody", null, rows)
      ]);
      rendered = limit;
    }
    var node = h("section", { class: "metric-family", dataset: { family: f.name, group: f.group } }, [
      header,
      h("p", { text: f.help || "no help line" }),
      body
    ]);
    return { node: node, rendered: rendered, capped: capped };
  }

  function renderMetricsFamilies(container, model, raw) {
    var region = container.querySelector("#metrics-families");
    var trunc = container.querySelector("#metrics-truncation");
    var count = container.querySelector("#metrics-filter-count");
    if (!region) return;
    if (!model || raw) {
      region.replaceChildren();
      if (trunc) { trunc.hidden = true; trunc.textContent = ""; }
      if (count) count.textContent = "";
      return;
    }
    var filter = (state.metrics.filter || "").toLowerCase();
    var cap = Math.min(model.families.length, METRICS_MAX_FAMILIES);
    var capped = model.families.length > METRICS_MAX_FAMILIES;
    var sections = [];
    var shownSeries = 0;
    var shown = 0;
    for (var i = 0; i < cap; i++) {
      var f = model.families[i];
      if (shownSeries >= METRICS_MAX_SERIES) { capped = true; break; }
      if (filter && f.name.toLowerCase().indexOf(filter) < 0) continue;
      var res = metricFamilyNode(f);
      if (res.capped) capped = true;
      shownSeries += res.rendered;
      if (shownSeries > METRICS_MAX_SERIES) capped = true;
      sections.push(res.node);
      shown++;
    }
    region.replaceChildren();
    for (var s = 0; s < sections.length; s++) region.appendChild(sections[s]);
    if (trunc) {
      if (capped) {
        trunc.hidden = false;
        trunc.textContent = "showing " + shown + " of " + model.families.length +
          " families (" + shownSeries + " of " + model.totalSeries + " series; hard caps " +
          METRICS_MAX_FAMILIES + " families / " + METRICS_MAX_SERIES + " series)";
      } else {
        trunc.hidden = true;
        trunc.textContent = "";
      }
    }
    if (count) count.textContent = shown + " of " + model.familyCount + " families";
  }

  function renderMetricsRaw(container, text, raw) {
    var pre = container.querySelector("#metrics-raw pre.payload");
    if (pre) pre.textContent = raw ? (text || "") : "";
  }

  function renderMetricsScraped(container, model) {
    var el = container.querySelector("#metrics-scraped");
    if (!el) return;
    if (!model) { el.textContent = ""; return; }
    var when = state.metrics.at > 0 ? new Date(state.metrics.at) : new Date();
    var stamp = pad2(when.getHours()) + ":" + pad2(when.getMinutes()) + ":" + pad2(when.getSeconds());
    var kib = Math.round((state.metrics.text ? state.metrics.text.length : 0) / 1024);
    el.textContent = "scraped " + stamp + " \u00b7 " + model.familyCount + " families \u00b7 " +
      model.totalSeries + " series \u00b7 " + kib + " KiB";
  }

  function renderMetrics(container, route) {
    var raw = isRawView();
    var toggle = container.querySelector("#metrics-view-toggle");
    if (toggle) toggle.textContent = raw ? "Matrix" : "Raw text";
    var famRegion = container.querySelector("#metrics-families");
    if (famRegion) famRegion.hidden = raw;
    var rawRegion = container.querySelector("#metrics-raw");
    if (rawRegion) rawRegion.hidden = !raw;
    renderMetricsNotice(container, state.metrics.tiles, state.metrics.model);
    renderMetricsVitals(container, state.metrics.tiles);
    renderMetricsFamilies(container, state.metrics.model, raw);
    renderMetricsRaw(container, state.metrics.text, raw);
    renderMetricsScraped(container, state.metrics.model);
  }

  function renderMetricsUnavailable(container, route, detail, text, raw) {
    ensureMetricsSkeleton(container, route);
    state.metrics.model = null;
    state.metrics.tiles = null;
    state.metrics.text = text || "";
    setMetricsNotice(container, detail, "danger");
    renderMetricsVitals(container, []);
    renderMetricsFamilies(container, null, raw);
    renderMetricsRaw(container, text || "", raw);
    renderMetricsScraped(container, null);
    var toggle = container.querySelector("#metrics-view-toggle");
    if (toggle) toggle.textContent = raw ? "Matrix" : "Raw text";
    var famRegion = container.querySelector("#metrics-families");
    if (famRegion) famRegion.hidden = raw;
    var rawRegion = container.querySelector("#metrics-raw");
    if (rawRegion) rawRegion.hidden = !raw;
  }

  function loadMetrics(container, route) {
    var raw = isRawView();
    return api(METRICS_API, { ifNoneMatch: state.etags.metrics }).then(function (r) {
      if (r.status === 401) return; /* handleUnauthorized already rendered login */
      if (r.status === 403) {
        state.displayed = "metrics";
        container.replaceChildren(permissionDenied(r.data));
        return;
      }
      if (r.status === 501) {
        state.displayed = "metrics";
        renderMetricsUnavailable(container, route,
          "Metrics are not implemented: " + errorDetail(r.data), "", raw);
        return;
      }
      if (r.status >= 400 && r.status !== 304) {
        state.displayed = "metrics";
        ensureMetricsSkeleton(container, route);
        setMetricsNotice(container, errorDetail(r.data), "danger");
        var err = new Error("metrics failed: " + r.status);
        err.status = r.status;
        throw err;
      }
      if (r.status === 304) {
        /* A 304 is not a scrape: keep the recorded samples and the last
         * computed tiles (spec §5.2). */
        if (!state.metrics.tiles) {
          state.metrics.model = { families: [], familyCount: 0, totalSeries: 0, bytes: 0 };
          state.metrics.tiles = computeTiles(state.metrics.model, {}, {});
        }
      } else {
        if (r.etag) state.etags.metrics = r.etag;
        var text = (r.data && typeof r.data.text === "string") ? r.data.text : "";
        if (text.length > METRICS_MAX_BYTES) {
          state.displayed = "metrics";
          renderMetricsUnavailable(container, route,
            "scrape too large to render (" + Math.round(text.length / 1024) + " KiB)", text, raw);
          return;
        }
        var model = parseProm(text);
        var at = nowMs();
        var prev = state.metrics.samples || {};
        var curr = buildSamples(model, at);
        state.metrics.text = text;
        state.metrics.model = model;
        state.metrics.tiles = computeTiles(model, prev, curr);
        state.metrics.samples = curr;
        state.metrics.at = at;
      }
      state.displayed = "metrics";
      ensureMetricsSkeleton(container, route);
      renderMetrics(container, route);
    });
  }

  /* --------------------------------------------------------------- boot ---- */

  var SCREENS = {
    overview: loadOverview,
    queues: loadQueues,
    queue: loadQueueDetail,
    exchanges: loadExchanges,
    exchange: loadExchangeDetail,
    connections: loadConnections,
    connection: loadConnectionDetail,
    channels: loadChannels,
    channel: loadChannelDetail,
    consumers: loadConsumers,
    bindings: loadBindings,
    nodes: loadNodes,
    node: loadNodes,
    users: loadUsers,
    permissions: loadPermissions,
    definitions: loadDefinitions,
    metrics: loadMetrics,
    about: loadAbout,
    search: loadSearch,
    notfound: function (container) {
      container.replaceChildren(notFound("/", "Back to overview"));
      return Promise.resolve();
    }
  };

  function setActiveNav(screen) {
    var map = {
      overview: "overview", connections: "connections", connection: "connections",
      channels: "channels", channel: "channels", consumers: "consumers", exchanges: "exchanges", exchange: "exchanges",
      queues: "queues", queue: "queues", bindings: "bindings", nodes: "nodes", node: "nodes",
      users: "users", permissions: "permissions", definitions: "definitions", about: "about", search: "overview",
      metrics: "metrics"
    };
    var active = map[screen] || "overview";
    var links = document.querySelectorAll("#nav a[data-nav]");
    Array.prototype.forEach.call(links, function (a) {
      if (a.dataset.nav === active) a.setAttribute("aria-current", "page");
      else a.removeAttribute("aria-current");
    });
  }

  function render() {
    var route = parsePath(location.pathname, location.search);
    state.route = route;
    if (!state.authed) { renderLogin(); return; }
    setAuthedChrome(true);
    setActiveNav(route.screen);
    var container = document.getElementById("content");
    container.replaceChildren(itemsLoading());
    state.displayed = null;
    state.refresh.base = route.screen === "overview" ? S1_REFRESH_MS
      : (route.screen === "metrics" ? (CONFIG.metricsRefreshMs || DEFAULT_REFRESH_MS) : DEFAULT_REFRESH_MS);
    state.refresh.errCount = 0;
    state.refresh.delay = state.refresh.base;
    var loader = SCREENS[route.screen] || SCREENS.notfound;
    state.currentLoad = function () { return loader(container, route); };
    updateRefreshStatus();
    refreshTick();
  }

  /* --------------------------------------------------------------- login --- */

  function renderLogin() {
    setAuthedChrome(false);
    var username = h("input", { type: "text", id: "login-username", name: "username", autocomplete: "username" });
    var password = h("input", { type: "password", id: "login-password", name: "password", autocomplete: "current-password" });
    var err = h("p", { class: "error", id: "login-error" });
    var form = h("form", { id: "login-form", class: "fieldset" }, [
      h("h1", { text: "Sign in to StrangeQ Management" }),
      h("div", { class: "form-row" }, [h("label", { for: "login-username", text: "Username" }), username]),
      h("div", { class: "form-row" }, [h("label", { for: "login-password", text: "Password" }), password]),
      h("div", { class: "form-row" }, [h("button", { type: "submit", id: "login-submit", text: "Sign in" })]),
      err
    ]);
    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      err.textContent = "";
      var submit = document.getElementById("login-submit");
      submit.disabled = true;
      api(API + "/strangeq/session", { method: "POST", body: { username: username.value, password: password.value }, timeoutMs: AUTH_TIMEOUT_MS }).then(function (r) {
        submit.disabled = false;
        if (r.status >= 400 || !r.data) { err.textContent = "Sign in failed: " + errorDetail(r.data); return; }
        state.csrf = r.data.csrf_token || readCSRFCookie();
        state.principal = r.data;
        state.authed = true;
        updatePrincipal();
        loadVersion();
        refreshHealth();
        render();
      }, function () {
        submit.disabled = false;
        err.textContent = "Sign in failed";
      });
    });
    var container = document.getElementById("content");
    container.replaceChildren(h("div", { class: "screen screen-login" }, [form]));
  }

  function ensureCapabilities() {
    if (state.capabilitiesLoaded && Object.keys(state.capabilities).length) return Promise.resolve();
    if (state.capabilitiesLoaded) return Promise.resolve();
    return loadVersion();
  }

  /* CAPABILITY_GATES maps a capability-gated control's data-cap attribute to
   * its three-state gate and the text explaining why the control is disabled. */
  var CAPABILITY_GATES = {
    bindings: { state: bindingsWriteState, reason: bindingsDisabledReason },
    close_connection: { state: closeConnectionState, reason: closeConnectionDisabledReason }
  };

  /* applyCapabilityGate sets one already-built control's enabled/disabled state
   * (and its disabled tooltip) from the current capability map. It is the single
   * code path used both when a control is built and when capabilities arrive
   * later. */
  function applyCapabilityGate(btn) {
    if (!btn) return;
    var gate = CAPABILITY_GATES[btn.dataset.cap];
    if (!gate) return;
    var st = gate.state();
    if (st === "on") { btn.disabled = false; btn.removeAttribute("title"); }
    else { btn.disabled = true; btn.title = gate.reason(st); }
  }

  /* refreshCapabilityControls re-applies the capability gates to the controls
   * that are already rendered. boot() renders the screen before loadVersion()
   * settles, so a gated control can be painted disabled while its capability is
   * still unknown, and a refresh tick does NOT recover it (loadList() skips the
   * rebuild on a 304). This is deliberately a targeted re-render of only the
   * gated controls: a full render() would re-run the screen's data load, which
   * double-scrapes /metrics and resets its rate samples, and would discard any
   * in-progress input. */
  function refreshCapabilityControls() {
    Array.prototype.forEach.call(document.querySelectorAll("[data-cap]"), applyCapabilityGate);
  }

  /* capabilitiesEqual compares two capability maps so a re-fetch that reports
   * the same set does not re-apply the gates (no needless DOM churn). */
  function capabilitiesEqual(a, b) {
    return JSON.stringify(a || {}) === JSON.stringify(b || {});
  }

  function loadVersion() {
    return api(API + "/strangeq/version").then(function (r) {
      if (r.status < 400 && r.data) {
        var before = state.capabilities;
        state.version = r.data;
        state.capabilities = r.data.capabilities || {};
        state.capabilitiesLoaded = true;
        var el = document.getElementById("app-version");
        if (el) el.textContent = r.data.version || "";
        /* Deliver late-arriving (or changed) capabilities to the already-rendered
         * UI: the first paint happened while the capability map was still empty,
         * so any gated control is currently disabled. */
        if (state.authed && !capabilitiesEqual(before, state.capabilities)) {
          refreshCapabilityControls();
        }
      }
    }).catch(function () { state.capabilitiesLoaded = false; /* version is best-effort */ });
  }

  function updatePrincipal() {
    var el = document.getElementById("principal");
    if (!el) return;
    el.replaceChildren();
    if (state.principal && state.principal.name) {
      appendChildren(el, [h("span", { text: state.principal.name })]);
      var logout = h("button", { type: "button", id: "logout-btn", text: "Sign out" });
      logout.addEventListener("click", function () {
        api(API + "/strangeq/session", { method: "DELETE" }).catch(function () { /* ignore */ }).then(function () {
          state.csrf = null;
          state.principal = null;
          state.authed = false;
          renderLogin();
        });
      });
      el.appendChild(document.createTextNode(" "));
      el.appendChild(logout);
    }
  }

  function boot() {
    document.addEventListener("click", function (ev) {
      var a = ev.target && ev.target.closest ? ev.target.closest("a[href]") : null;
      if (!a) return;
      var href = a.getAttribute("href");
      if (!href || href.charAt(0) !== "/") return;
      if (a.hasAttribute("download") || a.getAttribute("target") === "_blank") return;
      ev.preventDefault();
      navigate(href);
    });
    window.addEventListener("popstate", render);
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) { refreshNow(); }
    });

    var toggle = document.getElementById("refresh-toggle");
    if (toggle) {
      toggle.addEventListener("click", function () {
        state.refresh.paused = !state.refresh.paused;
        toggle.setAttribute("aria-pressed", state.refresh.paused ? "true" : "false");
        toggle.textContent = state.refresh.paused ? "Resume" : "Pause";
        updateRefreshStatus();
        if (!state.refresh.paused) refreshNow();
      });
    }
    var refreshBtn = document.getElementById("refresh-now");
    if (refreshBtn) refreshBtn.addEventListener("click", function () { refreshNow(); });

    var searchForm = document.getElementById("global-search");
    if (searchForm) {
      searchForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        var input = document.getElementById("search-input");
        navigate("/search?q=" + encodeURIComponent(input.value));
      });
    }

    api(API + "/whoami", { timeoutMs: AUTH_TIMEOUT_MS }).then(function (r) {
      if (r.status === 401) { renderLogin(); return; }
      if (r.status >= 400) { renderLogin(); return; }
      state.principal = r.data;
      state.authed = true;
      if (!state.csrf) state.csrf = readCSRFCookie();
      updatePrincipal();
      loadVersion();
      refreshHealth();
      render();
    }, function () {
      renderLogin();
    });
  }

  window.SQ = {
    config: CONFIG,
    state: state,
    api: api,
    esc: esc,
    refreshNow: refreshNow,
    refreshCapabilities: loadVersion,
    navigate: navigate,
    render: render,
    parseProm: parseProm,
    evalTile: evalTile,
    VITALS: VITALS
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
