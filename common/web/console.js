/* Shared helpers for every WaterManagement console page.
 * Data from other components is untrusted: it is only ever inserted with
 * textContent (via WM.el), never as HTML. */
(function () {
  "use strict";
  const WM = (window.WM = {});
  const configNode = document.getElementById("config");
  WM.config = configNode ? JSON.parse(configNode.textContent) : {};

  /* ---------- DOM ---------- */
  WM.$ = (selector, root) => (root || document).querySelector(selector);
  WM.el = function (tag, attrs, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs || {})) {
      if (value == null || value === false) continue;
      if (key === "class") node.className = value;
      else if (key === "text") node.textContent = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else if (key === "style") node.setAttribute("style", value);
      else if (value === true) node.setAttribute(key, "");
      else node.setAttribute(key, String(value));
    }
    for (const child of children.flat()) {
      if (child == null || child === false) continue;
      node.appendChild(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
  };
  WM.replace = function (container, ...children) {
    container.replaceChildren(...children.flat().filter((c) => c != null && c !== false));
  };
  WM.svg = function (path, size) {
    const ns = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(ns, "svg");
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.setAttribute("fill", "none");
    svg.setAttribute("stroke", "currentColor");
    svg.setAttribute("stroke-width", "2");
    svg.setAttribute("stroke-linecap", "round");
    svg.setAttribute("stroke-linejoin", "round");
    svg.setAttribute("aria-hidden", "true");
    if (size) { svg.setAttribute("width", size); svg.setAttribute("height", size); }
    for (const d of [].concat(path)) {
      const p = document.createElementNS(ns, "path");
      p.setAttribute("d", d);
      svg.appendChild(p);
    }
    return svg;
  };
  WM.icons = {
    drop: "M12 2.7c3.6 4.2 6 7.6 6 10.8a6 6 0 0 1-12 0c0-3.2 2.4-6.6 6-10.8z",
    valve: ["M4 14h16", "M6 14v5h12v-5", "M12 14V6", "M8 6h8"],
  };

  /* ---------- formatting ---------- */
  const STATUS_LABEL = {
    AVAILABLE: "Available", WATERING: "Watering", LEAK: "Leak", OUT_OF_SERVICE: "Out of service",
    DISCONNECTED: "Disconnected", AUTHORIZED: "Authorized", DENIED: "Denied", ENDED: "Ended",
    SUBMITTED: "Submitted", COMPLETED: "Completed", REJECTED: "Rejected", NO_RESULT: "No result",
    IDLE: "Idle", FAILED: "Failed", INTERRUPTED: "Interrupted", STOPPED: "Stopped", ACTIVE: "Active", BLOCKED: "Blocked", PENDING: "Pending",
  };
  WM.label = (status) => STATUS_LABEL[status] || String(status || "—").replace(/_/g, " ").toLowerCase().replace(/^./, (c) => c.toUpperCase());
  WM.chip = (status, text) => WM.el("span", { class: "chip s-" + status, text: text || WM.label(status) });
  WM.reason = (value) => (value ? String(value).replace(/_/g, " ").toLowerCase() : "");
  WM.num = function (value, digits) {
    if (value == null || value === "" || Number.isNaN(Number(value))) return "—";
    return Number(value).toFixed(digits == null ? 1 : digits);
  };
  WM.litres = (value) => (value == null ? "—" : WM.num(value, 1) + " L");
  WM.flow = (value) => (value == null ? "—" : WM.num(value, 1) + " L/min");
  WM.duration = function (seconds) {
    if (seconds == null || Number.isNaN(Number(seconds))) return "—";
    const s = Math.max(0, Math.round(Number(seconds)));
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), r = s % 60;
    if (h) return h + "h " + String(m).padStart(2, "0") + "m";
    if (m) return m + "m " + String(r).padStart(2, "0") + "s";
    return r + "s";
  };
  WM.time = (iso) => (iso ? new Date(iso).toLocaleTimeString() : "—");
  WM.ago = function (iso) {
    if (!iso) return "—";
    const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
    if (seconds < 5) return "just now";
    if (seconds < 60) return Math.floor(seconds) + "s ago";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
    return Math.floor(seconds / 3600) + "h ago";
  };
  WM.shortId = (id) => (id ? String(id).slice(0, 8) : "—");
  // Who started an irrigation: Central, the station's own menu, or a Field Operator.
  WM.startedBy = (id) => (id === "CENTRAL" ? "Started by Central"
    : id === "LOCAL" ? "Started at the station" : "Operator " + (id || "?"));
  WM.mode = function (item) {
    if (item.duration_seconds != null) return WM.duration(item.duration_seconds);
    if (item.target_volume_liters != null) return WM.litres(item.target_volume_liters) + " target";
    return "Until stopped";
  };

  /* progress bar for one irrigation: by duration, by volume, or open-ended */
  WM.progress = function (item, finished, failed) {
    let pct = null, left = "", right = "";
    const elapsed = item.elapsed_seconds, volume = item.volume_liters;
    if (item.duration_seconds) {
      pct = elapsed == null ? 0 : (100 * elapsed) / item.duration_seconds;
      left = WM.duration(elapsed || 0) + " of " + WM.duration(item.duration_seconds);
      right = volume == null ? "" : WM.litres(volume);
    } else if (item.target_volume_liters) {
      pct = volume == null ? 0 : (100 * volume) / item.target_volume_liters;
      left = WM.litres(volume || 0) + " of " + WM.litres(item.target_volume_liters);
      right = elapsed == null ? "" : WM.duration(elapsed);
    } else {
      left = elapsed == null ? "Waiting for the station" : WM.duration(elapsed) + " elapsed";
      right = volume == null ? "" : WM.litres(volume);
    }
    if (finished) pct = failed ? Math.max(pct || 0, 100) : 100;
    const bar = WM.el("span");
    const cls = "progress" + (pct == null ? " indeterminate" : "") + (finished ? (failed ? " failed" : " done") : "");
    if (pct != null) bar.style.width = Math.max(0, Math.min(100, pct)).toFixed(1) + "%";
    return WM.el("div", {},
      WM.el("div", { class: cls, role: "progressbar", "aria-valuemin": 0, "aria-valuemax": 100,
        "aria-valuenow": pct == null ? null : Math.round(Math.min(100, pct)) }, bar),
      WM.el("div", { class: "progress-row" }, WM.el("span", { text: left }), WM.el("span", { text: right })));
  };

  /* ---------- API ---------- */
  WM.headers = () => ({});
  WM.api = async function (path, options) {
    const opts = options || {};
    const headers = Object.assign({ Accept: "application/json" }, WM.headers(), opts.headers || {});
    const init = { method: opts.method || "GET", headers, cache: "no-store" };
    if (opts.body !== undefined) {
      headers["Content-Type"] = "application/json";
      init.body = JSON.stringify(opts.body);
    }
    let response;
    try {
      response = await fetch(path, init);
    } catch (error) {
      const failure = new Error("component unreachable");
      failure.status = 0;
      throw failure;
    }
    let data = null;
    try { data = await response.json(); } catch (error) { data = null; }
    if (!response.ok) {
      const failure = new Error((data && data.error) || "HTTP " + response.status);
      failure.status = response.status;
      failure.data = data;
      throw failure;
    }
    return data;
  };

  /* ---------- live indicator ---------- */
  WM.setLive = function (ok, message) {
    const node = WM.$("#live");
    if (!node) return;
    node.className = "live " + (ok ? "on" : "down");
    node.textContent = ok ? "Live stream" : message || "Disconnected";
  };
  /* ---------- toasts ---------- */
  WM.toast = function (message, kind, detail) {
    let stack = WM.$(".toasts");
    if (!stack) { stack = WM.el("div", { class: "toasts", role: "status", "aria-live": "polite" }); document.body.appendChild(stack); }
    const node = WM.el("div", { class: "toast " + (kind || "") },
      WM.el("div", {}, WM.el("div", { text: message, style: "font-weight:600" }), detail ? WM.el("div", { class: "muted", text: detail }) : null));
    stack.appendChild(node);
    setTimeout(() => node.remove(), kind === "bad" ? 7000 : 4500);
  };

  /* ---------- dialogs ---------- */
  WM.confirm = function ({ title, message, confirmText, danger }) {
    return new Promise((resolve) => {
      const ok = WM.el("button", { class: "btn " + (danger ? "danger solid" : "primary"), value: "ok", text: confirmText || "Confirm" });
      const cancel = WM.el("button", { class: "btn", value: "cancel", formnovalidate: true, text: "Cancel" });
      const dialog = WM.el("dialog", {},
        WM.el("form", { method: "dialog" },
          WM.el("h2", { text: title }),
          message ? WM.el("p", { class: "lead", text: message }) : null,
          WM.el("div", { class: "actions" }, cancel, ok)));
      document.body.appendChild(dialog);
      dialog.addEventListener("close", () => { resolve(dialog.returnValue === "ok"); dialog.remove(); });
      dialog.showModal();
      ok.focus();
    });
  };
  WM.closeOnBackdrop = function (dialog) {
    dialog.addEventListener("click", (event) => { if (event.target === dialog) dialog.close("cancel"); });
  };

  /* segmented control: returns getter */
  WM.segmented = function (container, onChange) {
    const buttons = Array.from(container.querySelectorAll("button"));
    function select(value) {
      buttons.forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.value === value)));
      if (onChange) onChange(value);
    }
    buttons.forEach((b) => b.addEventListener("click", (event) => { event.preventDefault(); select(b.dataset.value); }));
    return { select, value: () => (buttons.find((b) => b.getAttribute("aria-pressed") === "true") || buttons[0]).dataset.value };
  };

  /* ---------- live event table ---------- */
  function category(name) {
    if (/LEAK|FAULT|KO|REJECT|DENIED|TIMEOUT|EXPIRED|ERROR|LOST|UNREACHABLE/.test(name)) return "bad";
    if (/BLOCK/.test(name)) return "warn";
    if (/DISCONNECT/.test(name)) return "off";
    if (/START|PROGRESS|TELEMETRY|COMPLETED|AUTHORIZED|ACTIVATE|REGISTER|HEALTHY|RESOLVED|CONNECTED|ACK|REACHABLE/.test(name)) return "ok";
    return "";
  }
  /* Returns add(events): rows are inserted newest first, only with textContent. */
  WM.eventTable = function ({ body, filter, empty }) {
    const seen = new Set();
    function matches(row) {
      const q = (filter && filter.value || "").trim().toLowerCase();
      return !q || row.dataset.search.includes(q);
    }
    function add(e) {
      if (seen.has(e.seq)) return;
      seen.add(e.seq);
      const row = WM.el("tr", { class: category(e.event), title: e.request_id || "" },
        WM.el("td", { text: WM.time(e.time) }),
        WM.el("td", {}, WM.el("span", { class: "dir " + e.direction, text: e.direction })),
        WM.el("td", { text: e.peer }),
        WM.el("td", { class: "event", text: e.event }),
        WM.el("td", { text: e.ws_id || "" }),
        WM.el("td", { text: e.request_id ? WM.shortId(e.request_id) : "" }),
        WM.el("td", { class: "detail", text: e.detail || "" }));
      row.dataset.search = [e.peer, e.event, e.ws_id, e.request_id, e.detail].join(" ").toLowerCase();
      row.hidden = !matches(row);
      body.insertBefore(row, body.firstChild);
      while (body.children.length > 400) body.removeChild(body.lastChild);
      if (empty) empty.hidden = true;
    }
    if (filter) filter.addEventListener("input", () => { for (const row of body.children) row.hidden = !matches(row); });
    return (events) => events.forEach(add);
  };

  /* ---------- live stream (Server-Sent Events over fetch) ----------
   * fetch() is used instead of EventSource so Central's page can send its
   * Authorization header. The component pushes "events" the moment they
   * happen and "state" whenever its snapshot changes; the page never polls.
   * On a dropped connection it reconnects and resumes after the last event. */
  WM.stream = function ({ path, onState, onEvents, onUnauthorized }) {
    let last = 0, stopped = false, controller = null, retry = 1000;
    function dispatch(block) {
      let type = "message", data = "";
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) type = line.slice(6).trim();
        else if (line.startsWith("data:")) data += line.slice(5).trim();
      }
      if (!data) return;
      const payload = JSON.parse(data);
      if (type === "events") { last = Math.max(last, payload.last); if (onEvents) onEvents(payload.events); }
      else if (type === "state" && onState) onState(payload);
    }
    async function connect() {
      controller = new AbortController();
      try {
        const response = await fetch(path + "?after=" + last, {
          headers: Object.assign({ Accept: "text/event-stream" }, WM.headers()),
          cache: "no-store", signal: controller.signal,
        });
        if (!response.ok) { const failure = new Error("HTTP " + response.status); failure.status = response.status; throw failure; }
        WM.setLive(true);
        retry = 1000;
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let cut;
          while ((cut = buffer.indexOf("\n\n")) >= 0) {
            const block = buffer.slice(0, cut);
            buffer = buffer.slice(cut + 2);
            if (block && !block.startsWith(":")) dispatch(block);
          }
        }
        throw new Error("stream closed");
      } catch (error) {
        if (stopped) return;
        if (error.status === 401 && onUnauthorized) { onUnauthorized(); return; }
        WM.setLive(false, error.status ? "Error: " + error.message : "Reconnecting…");
        setTimeout(connect, retry);
        retry = Math.min(retry * 2, 10000);
      }
    }
    connect();
    return { stop() { stopped = true; if (controller) controller.abort(); } };
  };

  /* ---------- chain (component links) ---------- */
  WM.chain = function (container, nodes) {
    const parts = [];
    nodes.forEach((node, index) => {
      if (index > 0) {
        const link = node.link || {};
        parts.push(WM.el("div", { class: "link " + (link.ok === true ? "ok" : link.ok === false ? "bad" : ""), title: link.title || "" },
          link.tag ? WM.el("span", { class: "tag", text: link.tag }) : null));
      }
      parts.push(WM.el("div", { class: "node" + (node.self ? " self" : "") },
        WM.el("div", { class: "icon", text: node.icon }),
        WM.el("div", {}, WM.el("div", { class: "name", text: node.name }), WM.el("div", { class: "state", text: node.state }))));
    });
    WM.replace(container, parts);
  };
})();
