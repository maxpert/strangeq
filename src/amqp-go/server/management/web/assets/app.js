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
  var CONFIG = window.__SQ_CONFIG__ || {};
  var API = CONFIG.apiBase || '/api';
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
    route: null,
    currentLoad: null,
    etags: {},
    cache: {},
    displayed: null,
    noticeDismissed: false,
    degraded: null,
    samples: {},
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

  function boolCell(v, trueText, falseText) {
    if (isNull(v)) return dash();
    return h("span", { text: v ? (trueText || "true") : (falseText || "false") });
  }

  function link(text, href) {
    return h("a", { href: href, text: text });
  }

  /* --------------------------------------------------------------- API ----- */

  function api(path, opts) {
    opts = opts || {};
    var method = opts.method || "GET";
    var headers = { Accept: "application/json" };
    var hasBody = opts.body !== undefined && opts.body !== null;
    if (hasBody) headers["Content-Type"] = "application/json";
    if (method !== "GET" && method !== "HEAD" && state.csrf) headers["X-CSRF-Token"] = state.csrf;
    if (opts.ifNoneMatch) headers["If-None-Match"] = opts.ifNoneMatch;
    var init = { method: method, headers: headers, credentials: "same-origin", cache: "no-store" };
    if (hasBody) init.body = JSON.stringify(opts.body);
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

  /* ---------------------------------------------------------- feedback ----- */

  function toast(message, kind) {
    var root = document.getElementById("toast-root");
    if (!root) return;
    var node = h("div", { class: "toast" + (kind ? " " + kind : ""), text: message });
    root.appendChild(node);
    setTimeout(function () { if (node.parentNode) node.parentNode.removeChild(node); }, 6000);
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
    var head = h("tr", null, columns.map(function (c) {
      return h("th", { role: "columnheader", scope: "col", text: c.label, dataset: { col: c.key }, title: c.title || null });
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
    if (!el || !txt) return;
    var status = h && h.status ? h.status : "unknown";
    var mapped = status === "ok" ? "ok" : (status === "alarm" ? "alarm" : (status === "not_ready" ? "not_ready" : "unknown"));
    el.dataset.state = mapped;
    txt.textContent = mapped === "not_ready" ? "not ready" : mapped;
    var banner = document.getElementById("alarm-banner");
    if (!banner) return;
    if (mapped === "alarm") {
      banner.hidden = false;
      banner.dataset.kind = "warn";
      banner.textContent = "Alarm active: " + ((h && h.reason) || "yes");
    } else if (mapped === "not_ready") {
      banner.hidden = false;
      banner.dataset.kind = "danger";
      banner.textContent = "Not ready: " + ((h && h.reason) || "starting up");
    } else if (!state.degraded) {
      banner.hidden = true;
    }
  }

  function applyDegraded(overview) {
    if (!overview || !overview.unsafe_recovery) return;
    var u = overview.unsafe_recovery;
    var banner = document.getElementById("alarm-banner");
    if (u.degraded) {
      state.degraded = u;
      if (banner) {
        banner.hidden = false;
        banner.dataset.kind = "danger";
        banner.textContent = "broker booted degraded; " + (u.discarded_artifacts || 0) + " artifacts discarded";
      }
    } else {
      state.degraded = null;
    }
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

  function backoffDelay(errCount, base) {
    var steps = [base, base * 2, base * 4, MAX_BACKOFF_MS];
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
    state.refresh.delay = backoffDelay(state.refresh.errCount, state.refresh.base);
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
    var r = { screen: "overview", params: {}, query: new URLSearchParams(search || "") };
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
      h("div", { class: "page-head" }, [h("h1", { text: "Overview" })]),
      cards,
      summary,
      h("h2", { text: "Top queues by ready depth" }),
      topRows.length ? table : emptyState("No queues match the top-10 ready-depth view.")
    ]);
  }

  function loadQueues(container, route) {
    var q = route.query;
    var params = {
      page: q.get("page") || 1,
      page_size: q.get("page_size") || 50,
      name: q.get("name"),
      durable: q.get("durable"),
      has_consumers: q.get("has_consumers"),
      non_empty: q.get("non_empty"),
      sort: q.get("sort") || "messages_ready",
      sort_reverse: q.get("sort_reverse") || "true"
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
      { key: "name", label: "Name", render: function (r) { return link(String(r.name), "/queues/" + encodeURIComponent(r.name)); } },
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
        onRowClick: function (r) { navigate("/queues/" + encodeURIComponent(r.name)); }
      }) : emptyState("No queues exist yet.", "Declare queue", function () { openDeclareQueue(route); })
    ]);
  }

  function queuesToolbar(route) {
    var q = route.query;
    var nameInput = h("input", { type: "search", value: q.get("name") || "", placeholder: "name filter", "aria-label": "Filter queues by name" });
    var durableSel = h("select", { "aria-label": "Filter by durable" }, [
      option("", "durable: any"), option("true", "durable: true"), option("false", "durable: false")
    ]);
    durableSel.value = q.get("durable") || "";
    var consumersSel = h("select", { "aria-label": "Filter by consumers" }, [
      option("", "consumers: any"), option("true", "has consumers"), option("false", "no consumers")
    ]);
    consumersSel.value = q.get("has_consumers") || "";
    var nonEmpty = h("input", { type: "checkbox", id: "filter-non-empty" });
    nonEmpty.checked = q.get("non_empty") === "true";
    var form = h("form", { class: "toolbar" }, [
      nameInput, durableSel, consumersSel,
      h("label", { for: "filter-non-empty", text: "non-empty" }), nonEmpty,
      h("button", { type: "submit", text: "Apply" }),
      h("button", { type: "button", text: "Declare queue", onclick: function () { openDeclareQueue(route); } })
    ]);
    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var p = new URLSearchParams();
      if (nameInput.value) p.set("name", nameInput.value);
      if (durableSel.value) p.set("durable", durableSel.value);
      if (consumersSel.value) p.set("has_consumers", consumersSel.value);
      if (nonEmpty.checked) p.set("non_empty", "true");
      navigate("/queues" + (p.toString() ? "?" + p.toString() : ""));
    });
    return form;
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
    var bindingsNotice = capabilityNotice({ error: "not_implemented", reason: "aggregate binding listing is not implemented (BOB-4)" }, "Bindings not available");
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
      h("div", { class: "page-head" }, [h("h1", { text: name }), h("span", { class: "spacer" }), getBtn, publishBtn, purgeBtn, deleteBtn]),
      props,
      h("h2", { text: "Statistics" }), stats,
      h("h2", { text: "Arguments" }), renderArguments(queue.arguments),
      h("h2", { text: "Bindings" }), bindingsNotice,
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
    var params = { page: q.get("page") || 1, page_size: q.get("page_size") || 50, name: q.get("name"), type: q.get("type") };
    var path = API + "/exchanges" + qs(params);
    return loadList(container, "exchanges:" + path, path, {
      render: function (r) { return buildExchanges(r.data, route); }
    });
  }

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
      items.length ? tableBlock(columns, items, { onRowClick: function (r) { navigate("/exchanges/" + encodeURIComponent(r.name)); } })
        : emptyState("No exchanges exist.")
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
      h("div", { class: "page-head" }, [h("h1", { text: name || "(default)" }), h("span", { class: "spacer" }), publishBtn, deleteBtn]),
      props,
      h("h2", { text: "Arguments" }), renderArguments(exchange.arguments),
      h("h2", { text: "Bindings" }), capabilityNotice({ error: "not_implemented", reason: "aggregate binding listing is not implemented (BOB-4)" }, "Bindings not available")
    ]);
  }

  function loadConnections(container, route) {
    var q = route.query;
    var params = { page: q.get("page") || 1, page_size: q.get("page_size") || 50, name: q.get("name"), user: q.get("user") };
    var path = API + "/connections" + qs(params);
    return loadList(container, "connections:" + path, path, {
      render: function (r) { return buildConnections(r.data); }
    });
  }

  function buildConnections(data) {
    var items = itemsOf(data);
    var columns = [
      { key: "name", label: "Name", render: function (r) { return link(String(r.name), "/connections/" + encodeURIComponent(r.name)); } },
      { key: "user", label: "User", render: function (r) { return h("span", { text: val(r.user) }); } },
      { key: "vhost", label: "Vhost", render: function (r) { return h("span", { text: val(r.vhost) }); } },
      { key: "state", label: "State", render: function (r) { return stateCell(r.state); } },
      { key: "channels", label: "Channels", render: function (r) { return num(r.channels); } },
      { key: "peer", label: "Peer", render: function (r) { return h("span", { text: val(r.peer_host) + ":" + val(r.peer_port) }); } },
      { key: "connected_at", label: "Connected at", render: function (r) { return fmtTime(r.connected_at); } },
      { key: "last_activity", label: "Last activity", render: function (r) { return fmtTime(r.last_activity); } }
    ];
    return h("div", { class: "screen screen-connections" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Connections" })]),
      h("p", { class: "null", text: "Entity data is global even if connection vhosts differ (D-1)." }),
      items.length ? tableBlock(columns, items, { onRowClick: function (r) { navigate("/connections/" + encodeURIComponent(r.name)); } })
        : emptyState("No client connections.")
    ]);
  }

  function loadConnectionDetail(container, route) {
    var name = route.params.name;
    var path = API + "/connections/" + encodeURIComponent(name);
    return loadList(container, "connection:" + path, path, {
      backHref: "/connections", backLabel: "Back to connections",
      render: function (r) { return buildConnectionDetail(r.data, route); }
    });
  }

  function buildConnectionDetail(conn, route) {
    var name = String(conn.name);
    var closeBtn = h("button", { type: "button", id: "close-connection-btn", class: "danger", text: "Close" });
    closeBtn.addEventListener("click", function () { openCloseConnection(conn); });
    var channelsPanel = h("div", { id: "connection-channels" }, [itemsLoading()]);
    api(API + "/connections/" + encodeURIComponent(name) + "/channels").then(function (r) {
      if (r.status === 501) channelsPanel.replaceChildren(h("p", { class: "null", text: "channel detail not available" }));
      else if (r.status >= 400) channelsPanel.replaceChildren(errorState(r.data));
      else channelsPanel.replaceChildren(tableBlock(
        [{ key: "number", label: "Channel", render: function (c) { return num(c.number); } },
         { key: "state", label: "State", render: function (c) { return stateCell(c.state); } }],
        itemsOf(r.data), {}));
    }).catch(function () { channelsPanel.replaceChildren(h("p", { class: "null", text: "channel detail not available" })); });
    var props = h("dl", { class: "props" }, [
      h("dt", { text: "user" }), h("dd", { text: val(conn.user) }),
      h("dt", { text: "vhost" }), h("dd", { text: val(conn.vhost) }),
      h("dt", { text: "state" }), h("dd", null, [stateCell(conn.state)]),
      h("dt", { text: "protocol" }), h("dd", { text: val(conn.protocol) }),
      h("dt", { text: "peer" }), h("dd", { text: val(conn.peer_host) + ":" + val(conn.peer_port) }),
      h("dt", { text: "connected_at" }), h("dd", null, [fmtTime(conn.connected_at)]),
      h("dt", { text: "last_activity" }), h("dd", null, [fmtTime(conn.last_activity)])
    ]);
    return h("div", { class: "screen screen-connection-detail" }, [
      h("nav", { class: "breadcrumb" }, [link("Connections", "/connections"), h("span", { text: " / " }), h("span", { text: name })]),
      h("div", { class: "page-head" }, [h("h1", { text: name }), h("span", { class: "spacer" }), closeBtn]),
      props,
      h("p", { class: "null", text: "Entity data is global even if connection vhosts differ (D-1)." }),
      h("h2", { text: "Channels" }), channelsPanel
    ]);
  }

  function loadChannels(container) {
    var path = API + "/channels" + qs({ page: 1, page_size: 100 });
    return loadList(container, "channels", path, {
      render: function (r) { return buildChannels(r.data); }
    });
  }

  function buildChannels(data) {
    var items = itemsOf(data);
    var columns = [
      { key: "number", label: "Number", render: function (r) { return num(r.number); } },
      { key: "connection", label: "Connection", render: function (r) { return h("span", { text: val(r.connection) }); } },
      { key: "user", label: "User", render: function (r) { return h("span", { text: val(r.user) }); } },
      { key: "vhost", label: "Vhost", render: function (r) { return h("span", { text: val(r.vhost) }); } },
      { key: "state", label: "State", render: function (r) { return stateCell(r.state); } },
      { key: "consumers", label: "Consumers", render: function (r) { return num(r.consumers); } },
      { key: "unacked", label: "Unacked", render: function (r) { return num(r.unacked); } },
      { key: "prefetch_count", label: "Prefetch", render: function (r) { return num(r.prefetch_count); } },
      { key: "confirm", label: "Confirm", render: function (r) { return boolCell(r.confirm); } },
      { key: "transactional", label: "Transactional", render: function (r) { return num(r.transactional); } }
    ];
    return h("div", { class: "screen screen-channels" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Channels" })]),
      h("p", { class: "notice empty", role: "note", text: "Channels are read-only. There is no management close-channel operation; a channel closes when its client closes it or its connection is closed." }),
      items.length ? tableBlock(columns, items, {}) : emptyState("No open channels.")
    ]);
  }

  function loadConsumers(container, route) {
    var q = route.query;
    var path = API + "/consumers" + qs({ page: 1, page_size: 100, queue: q.get("queue"), connection: q.get("connection") });
    return loadList(container, "consumers:" + path, path, {
      render: function (r) { return buildConsumers(r.data); }
    });
  }

  function buildConsumers(data) {
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
      items.length ? tableBlock(columns, items, {}) : emptyState("No active consumers.")
    ]);
  }

  function valueOrDash(v) {
    if (isNull(v)) return dash("not reported by this broker");
    return h("span", { text: String(v) });
  }

  function loadBindings(container) {
    if (state.capabilities && state.capabilities.bindings === false) {
      container.replaceChildren(capabilityNotice(
        { error: "not_implemented", reason: "aggregate binding listing is not implemented (BOB-4)" },
        "Bindings not available"));
      return Promise.resolve();
    }
    var path = API + "/bindings";
    return loadList(container, "bindings", path, {
      notImplementedTitle: "Bindings not available",
      render: function (r) { return buildBindings(r.data); }
    });
  }

  function buildBindings(data) {
    var items = itemsOf(data);
    var columns = [
      { key: "source", label: "Source", render: function (r) { return h("span", { text: val(r.source) }); } },
      { key: "destination", label: "Destination", render: function (r) { return h("span", { text: val(r.destination) }); } },
      { key: "destination_type", label: "Type", render: function (r) { return h("span", { text: val(r.destination_type) }); } },
      { key: "routing_key", label: "Routing key", render: function (r) { return h("span", { text: val(r.routing_key) }); } }
    ];
    return h("div", { class: "screen screen-bindings" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Bindings" })]),
      items.length ? tableBlock(columns, items, {}) : emptyState("No bindings.")
    ]);
  }

  function loadNodes(container, route) {
    var path = route.params.name ? API + "/nodes/" + encodeURIComponent(route.params.name) : API + "/nodes";
    return loadList(container, "nodes:" + path, path, {
      backHref: "/nodes", backLabel: "Back to nodes",
      render: function (r) { return buildNodes(itemsOf(r.data), route); }
    });
  }

  function buildNodes(nodes, route) {
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
      ], nodes, {}) : emptyState("No node information reported.")
    ]);
  }

  function loadUsers(container) {
    return loadList(container, "users", API + "/users", {
      render: function (r) {
        var users = itemsOf(r.data);
        var userBlock = users.length ? tableBlock([
          { key: "name", label: "User", render: function (u) { return h("span", { text: val(u.name) }); } },
          { key: "tags", label: "Tags", render: function (u) { return h("span", { text: Array.isArray(u.tags) ? u.tags.join(", ") : val(u.tags) }); } }
        ], users, {}) : emptyState("No users configured.");
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
          h("h2", { text: "Users" }), userBlock,
          h("h2", { text: "Permissions" }), perms
        ]);
      }
    });
  }

  function loadPermissions(container) {
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
          ], items, {}) : emptyState("No permissions configured.")
        ]);
      }
    });
  }

  function loadDefinitions(container) {
    var exportLink = h("a", { href: API + "/definitions", download: "strangeq-definitions.json" },
      [h("button", { type: "button", text: "Export definitions" })]);
    var fileInput = h("input", { type: "file", id: "definitions-file", accept: "application/json" });
    var preview = h("div", { id: "definitions-preview" });
    fileInput.addEventListener("change", function () {
      var file = fileInput.files && fileInput.files[0];
      if (!file) return;
      var reader = new FileReader();
      reader.onload = function () {
        var parsed = null;
        try { parsed = JSON.parse(String(reader.result)); } catch (e) { parsed = null; }
        if (!parsed) { preview.replaceChildren(errorState({ error: "bad_request", reason: "not valid JSON" })); return; }
        preview.replaceChildren(h("div", { class: "fieldset" }, [
          h("h3", { text: "Dry-run preview" }),
          h("p", { text: "vhosts: " + itemsOf(parsed.vhosts).length + ", exchanges: " + itemsOf(parsed.exchanges).length + ", queues: " + itemsOf(parsed.queues).length + ", bindings: " + itemsOf(parsed.bindings).length }),
          h("p", { class: "null", text: "Import is non-atomic; sections may partially apply (BOB-10)." }),
          h("button", { type: "button", id: "definitions-apply", text: "Apply import", onclick: function () { applyDefinitions(parsed, preview); } })
        ]));
      };
      reader.readAsText(file);
    });
    return h("div", { class: "screen screen-definitions" }, [
      h("div", { class: "page-head" }, [h("h1", { text: "Definitions" })]),
      h("h2", { text: "Export" }),
      h("p", { text: "Export omits users/permissions unless permitted, and omits bindings while BOB-4 is unbuilt." }),
      exportLink,
      h("h2", { text: "Import" }),
      h("p", { class: "null", text: "Import is non-atomic. Skipped sections: users/permissions, bindings." }),
      h("div", { class: "form-row" }, [fileInput]),
      preview
    ]);
  }

  function applyDefinitions(doc, out) {
    api(API + "/definitions", { method: "POST", body: doc }).then(function (r) {
      if (r.status >= 400) { out.replaceChildren(errorState(r.data)); return; }
      var res = r.data || {};
      var atomic = res.strangeq && res.strangeq.atomic === true;
      out.replaceChildren(h("div", { class: "fieldset" }, [
        h("h3", { text: "Import result" }),
        h("p", { text: "applied: " + itemsOf(res.applied).length + ", skipped: " + itemsOf(res.skipped).length + ", failed: " + itemsOf(res.failed).length }),
        h("p", { class: atomic ? "null" : "stat-partial", text: atomic ? "atomic import" : "non-atomic import; sections may partially apply" })
      ]));
      toast("Import complete", "ok");
      invalidateAll();
    }, function () { out.replaceChildren(errorState(null)); });
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
        channels: "channel listing",
        bindings: "aggregate binding listing (BOB-4)",
        close_connection: "force-close a connection (DIV-close)",
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
        var enabled = caps[k] === true;
        return h("tr", { role: "row" }, [
          h("td", { text: k }),
          h("td", null, [badge(enabled ? "supported" : "unavailable", enabled ? "" : "warn")]),
          h("td", { text: explanations[k] || "" })
        ]);
      });
      var divergences = [
        ["D-1", "entity data is global; vhosts are not independent"],
        ["D-2", "single-node broker; no cluster, no HA"],
        ["D-3", "classic queues only; no quorum/stream/priority"],
        ["DIV-close", "close connection force-closes the socket; the peer is not sent connection.close"],
        ["DIV-channel", "channel prefetch/transactional are read as null (mutex-guarded)"],
        ["DIV-15", "unanswerable health checks return 501, not a fake 200"]
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

  function openDeclareExchange(route) {
    var nameInput = h("input", { type: "text", id: "declare-name", "aria-label": "Exchange name" });
    var typeSel = h("select", { id: "declare-type", "aria-label": "Exchange type" }, [
      option("direct", "direct"), option("fanout", "fanout"), option("topic", "topic"), option("headers", "headers")
    ]);
    var durable = h("input", { type: "checkbox", id: "declare-durable" }); durable.checked = true;
    var autoDelete = h("input", { type: "checkbox", id: "declare-auto-delete" });
    var internal = h("input", { type: "checkbox", id: "declare-internal" });
    var err = h("p", { class: "error", id: "declare-error" });
    var body = h("div", null, [
      h("div", { class: "form-row" }, [h("label", { for: "declare-name", text: "Name" }), nameInput]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-type", text: "Type" }), typeSel]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-durable", text: "Durable" }), durable]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-auto-delete", text: "Auto-delete" }), autoDelete]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-internal", text: "Internal" }), internal]),
      err
    ]);
    openModal({
      title: "Declare exchange",
      body: body,
      confirmLabel: "Declare",
      guard: function () { return nameInput.value.trim().length > 0; },
      onConfirm: function () {
        err.textContent = "";
        return api(exchangePath("/", nameInput.value.trim()), {
          method: "PUT",
          body: { type: typeSel.value, durable: durable.checked, auto_delete: autoDelete.checked, internal: internal.checked, arguments: {} }
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
    var err = h("p", { class: "error" });
    var body = h("div", null, [
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-name", text: "Name" }), nameInput]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-durable", text: "Durable" }), durable]),
      h("div", { class: "form-row" }, [h("label", { for: "declare-queue-auto-delete", text: "Auto-delete" }), autoDelete]),
      h("p", { class: "null", text: "Arguments: only x-message-ttl, x-expires, x-dead-letter-exchange, x-dead-letter-routing-key, x-max-length, x-max-length-bytes and x-overflow are supported. Priority and lazy queues are never available (D-3)." }),
      err
    ]);
    openModal({
      title: "Declare queue",
      body: body,
      confirmLabel: "Declare",
      guard: function () { return nameInput.value.trim().length > 0; },
      onConfirm: function () {
        return api(queuePath("/", nameInput.value.trim()), {
          method: "PUT",
          body: { durable: durable.checked, auto_delete: autoDelete.checked, arguments: {} }
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

  function openCloseConnection(conn) {
    openModal({
      title: "Close connection " + conn.name,
      body: h("div", null, [
        h("p", { class: "effect", text: "Close connection \u201c" + conn.name + "\u201d to " + val(conn.peer_host) + ":" + val(conn.peer_port) + "?" }),
        h("p", { class: "error", text: "This force-closes the socket; the peer is not sent connection.close (DIV-close)." })
      ]),
      confirmLabel: "Close connection",
      onConfirm: function () {
        return api(API + "/connections/" + encodeURIComponent(conn.name), { method: "DELETE" }).then(function (r) {
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
    var result = h("div", { id: "publish-result" });
    openModal({
      title: "Publish to " + label,
      confirmLabel: "Publish",
      body: h("div", null, [
        h("p", { class: "error", text: "This panel is for troubleshooting only; it is not a production publisher." }),
        h("div", { class: "form-row" }, [h("label", { for: "publish-routing-key", text: "Routing key" }), routingKey]),
        h("div", { class: "form-row" }, [h("label", { for: "publish-payload", text: "Payload" }), payload]),
        h("div", { class: "form-row" }, [h("label", { for: "publish-encoding", text: "Encoding" }), encoding]),
        result
      ]),
      initialFocus: routingKey,
      onConfirm: function () {
        return api(path + "/publish", {
          method: "POST",
          body: { properties: {}, routing_key: routingKey.value, payload: payload.value, payload_encoding: encoding.value }
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
            var body = m.payload;
            var truncated = false;
            if (typeof body === "string" && body.length > 512) { body = body.slice(0, 512); truncated = true; }
            return h("div", { class: "fieldset" }, [
              h("p", { text: "encoding: " + val(m.payload_encoding) + ", routing_key: " + val(m.routing_key) + ", redelivered: " + val(m.redelivered) }),
              h("pre", { class: "payload", text: val(body) }),
              truncated ? h("p", { class: "stat-partial", text: "truncated" }) : null
            ]);
          })));
          return true;
        }, function () { out.replaceChildren(errorState(null)); return true; });
      }
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
    consumers: loadConsumers,
    bindings: loadBindings,
    nodes: loadNodes,
    node: loadNodes,
    users: loadUsers,
    permissions: loadPermissions,
    definitions: loadDefinitions,
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
      channels: "channels", consumers: "consumers", exchanges: "exchanges", exchange: "exchanges",
      queues: "queues", queue: "queues", bindings: "bindings", nodes: "nodes", node: "nodes",
      users: "users", permissions: "permissions", definitions: "definitions", about: "about", search: "overview"
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
    state.refresh.base = route.screen === "overview" ? S1_REFRESH_MS : DEFAULT_REFRESH_MS;
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

  function loadVersion() {
    return api(API + "/strangeq/version").then(function (r) {
      if (r.status < 400 && r.data) {
        state.version = r.data;
        state.capabilities = r.data.capabilities || {};
        var el = document.getElementById("app-version");
        if (el) el.textContent = r.data.version || "";
      }
    }).catch(function () { /* version is best-effort */ });
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

    var dismiss = document.getElementById("capability-dismiss");
    var notice = document.getElementById("capability-notice");
    if (dismiss && notice) {
      dismiss.addEventListener("click", function () {
        state.noticeDismissed = true;
        notice.hidden = true;
      });
    }

    api(API + "/whoami", { timeoutMs: AUTH_TIMEOUT_MS }).then(function (r) {
      if (r.status === 401) { renderLogin(); return; }
      if (r.status >= 400) { renderLogin(); return; }
      state.principal = r.data;
      state.authed = true;
      if (!state.csrf) state.csrf = readCSRFCookie();
      if (notice && !state.noticeDismissed) notice.hidden = false;
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
    navigate: navigate,
    render: render
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
