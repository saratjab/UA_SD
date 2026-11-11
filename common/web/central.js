/* WM_Central console: the whole control menu plus live irrigations. */
(function () {
  "use strict";
  const { el, $, replace } = WM;
  const API = WM.config.apiBase || "/v1";
  const TOKEN_KEY = "wm-central-token";
  const STATUS_ORDER = ["AVAILABLE", "WATERING", "LEAK", "OUT_OF_SERVICE", "DISCONNECTED"];
  const STATUS_DOT = { AVAILABLE: "var(--ok)", WATERING: "var(--accent)", LEAK: "var(--bad)", OUT_OF_SERVICE: "var(--warn)", DISCONNECTED: "var(--off)" };

  let stations = [];
  let operators = { default: "CENTRAL", operators: [] };
  let live = null;
  let addEvents = null;

  $("#brand-mark").appendChild(WM.svg(WM.icons.drop));

  /* ---------- token ---------- */
  function token() { try { return sessionStorage.getItem(TOKEN_KEY); } catch (e) { return null; } }
  function setToken(value) { try { value ? sessionStorage.setItem(TOKEN_KEY, value) : sessionStorage.removeItem(TOKEN_KEY); } catch (e) { /* storage blocked */ } }
  let memoryToken = null;
  WM.headers = () => { const t = token() || memoryToken; return t ? { Authorization: "Bearer " + t } : {}; };

  function showLogin(message) {
    if (live) { live.stop(); live = null; }
    $("#app").hidden = true;
    $("#sign-out").hidden = true;
    $("#login").hidden = false;
    $("#login-error").textContent = message || "";
    WM.setLive(false, "Signed out");
    $("#token-input").focus();
  }

  $("#login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const value = $("#token-input").value.trim();
    if (!value) return;
    memoryToken = value;
    setToken(value);
    try {
      await WM.api(API + "/overview");
      $("#token-input").value = "";
      startConsole();
    } catch (error) {
      memoryToken = null;
      setToken(null);
      $("#login-error").textContent = error.status === 401 ? "That token was not accepted: it must be exactly the CENTRAL_ADMIN_TOKEN this Central was started with." :
        error.status === 403 ? "Central requires HTTPS for the admin console." : "Could not reach Central: " + error.message;
    }
  });
  $("#sign-out").addEventListener("click", () => { memoryToken = null; setToken(null); showLogin(); });

  /* ---------- rendering ---------- */
  function renderStats(overview) {
    const items = STATUS_ORDER.map((s) => ({ label: WM.label(s), value: overview.by_status[s] || 0, dot: STATUS_DOT[s] }));
    items.push({ label: "Blocked", value: overview.blocked, dot: "var(--warn)" });
    items.push({ label: "Live irrigations", value: overview.live_irrigations, dot: "var(--accent)" });
    replace($("#stats"), items.map((i) => {
      const node = el("div", { class: "stat" }, el("div", { class: "label", text: i.label }), el("div", { class: "value", text: i.value }));
      node.style.setProperty("--dot", i.dot);
      return node;
    }));
  }

  function stationActions(s) {
    const buttons = [];
    const blocked = s.admin_state === "BLOCKED";
    if (s.status === "AVAILABLE" && !s.request_id) buttons.push(el("button", { class: "btn primary sm", text: "Start", onclick: () => openStart(s.station_id) }));
    if (s.request_id) buttons.push(el("button", { class: "btn danger sm", text: "Stop", onclick: () => stopOne(s.station_id) }));
    if (blocked) buttons.push(el("button", { class: "btn okish sm", text: "Activate", onclick: () => control({ action: "activate", station_id: s.station_id }, "Activate " + s.station_id) }));
    else buttons.push(el("button", { class: "btn warn sm", text: "Block", onclick: () => confirmBlock(s.station_id) }));
    return buttons;
  }

  function renderStations() {
    const q = ($("#station-filter").value || "").trim().toLowerCase();
    const list = stations.filter((s) => !q || (s.station_id + " " + s.location + " " + s.status).toLowerCase().includes(q));
    $("#station-count").textContent = stations.length;
    if (!stations.length) {
      replace($("#stations"), el("div", { class: "empty" }, el("div", { class: "title", text: "No stations registered yet" }), "Stations appear here when their Monitor connects to Central."));
      return;
    }
    replace($("#stations"), list.map((s) => {
      const irrigation = s.irrigation;
      const card = el("article", { class: "card status-" + s.status, id: "station-" + s.station_id },
        el("div", { class: "card-head" },
          el("div", { class: "grow" }, el("div", { class: "card-title mono", text: s.station_id }), el("div", { class: "card-sub", text: s.location || "—" })),
          el("div", { class: "row", style: "gap:4px;justify-content:flex-end" },
            WM.chip(s.status),
            s.admin_state === "BLOCKED" && s.status !== "OUT_OF_SERVICE" ? WM.chip("BLOCKED") : null)),
        el("div", { class: "metrics" },
          metric("Flow", s.status === "WATERING" ? WM.flow(s.flow_rate_lpm) : "—"),
          metric("Volume", s.status === "WATERING" ? WM.litres(s.total_volume_liters) : "—"),
          metric("Operator", irrigation ? (irrigation.operator_id === "CENTRAL" ? "Central" : irrigation.operator_id) : "—")),
        irrigation ? WM.progress(irrigation) : null,
        s.fault ? el("div", { class: "fault", text: s.fault }) : null,
        el("div", { class: "card-actions" }, stationActions(s)));
      return card;
    }));
  }

  function metric(label, value) {
    return el("div", { class: "metric" }, el("div", { class: "label", text: label }), el("div", { class: "value", text: value, title: value }));
  }

  function renderLive(live) {
    $("#live-count").textContent = live.length;
    if (!live.length) {
      replace($("#live-list"), el("div", { class: "empty" }, el("div", { class: "title", text: "No irrigation running" }), "Start one from the menu or a station card."));
      return;
    }
    replace($("#live-list"), live.map((i) => el("article", { class: "card" },
      el("div", { class: "card-head" },
        el("div", { class: "grow" },
          el("div", { class: "card-title" }, el("span", { class: "mono", text: i.station_id }), i.location ? el("span", { class: "muted", text: " · " + i.location }) : null),
          el("div", { class: "card-sub" }, operatorLabel(i.operator_id), " · " + WM.mode(i))),
        WM.chip(i.status)),
      WM.progress(i),
      el("div", { class: "metrics" },
        metric("Elapsed", WM.duration(i.elapsed_seconds)),
        metric("Volume", WM.litres(i.volume_liters)),
        metric("Flow", WM.flow(i.flow_rate_lpm))),
      el("div", { class: "row" },
        el("span", { class: "muted mono", title: i.request_id, text: "req " + WM.shortId(i.request_id) }),
        el("span", { class: "muted", text: i.started_at ? "started " + WM.time(i.started_at) : "waiting for the station to start" }),
        el("div", { class: "grow" }),
        el("button", { class: "btn danger sm", text: "Stop", onclick: () => stopOne(i.station_id) })))));
  }

  function operatorLabel(id) {
    return id === "CENTRAL" ? "Started by Central" : "Field Operator " + (id || "?");
  }

  function renderHistory(recent) {
    $("#history-empty").hidden = recent.length > 0;
    replace($("#history"), recent.map((i) => {
      const result = i.status === "DENIED" ? "DENIED" : ["LEAK", "DISCONNECTED", "BLOCKED", "CENTRAL_RESTART"].includes(i.end_reason) ? "INTERRUPTED" : i.end_reason === "MANUAL" ? "STOPPED" : "COMPLETED";
      return el("tr", {},
        el("td", { text: WM.time(i.requested_at) }),
        el("td", { class: "mono", text: i.station_id }),
        el("td", { text: i.operator_id || "—" }),
        el("td", { text: WM.mode(i) }),
        el("td", {}, WM.chip(result)),
        el("td", { text: WM.reason(i.deny_reason || i.end_reason) || "—" }),
        el("td", { class: "num", text: WM.duration(i.elapsed_seconds) }),
        el("td", { class: "num", text: WM.litres(i.volume_liters) }),
        el("td", { class: "mono muted", title: i.request_id, text: WM.shortId(i.request_id) }));
    }));
  }

  /* ---------- controls ---------- */
  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    const b = crypto.getRandomValues(new Uint8Array(16));
    b[6] = (b[6] & 0x0f) | 0x40; b[8] = (b[8] & 0x3f) | 0x80;
    const h = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
    return h.slice(0, 8) + "-" + h.slice(8, 12) + "-" + h.slice(12, 16) + "-" + h.slice(16, 20) + "-" + h.slice(20);
  }

  async function control(body, title) {
    let data, failed = false;
    try {
      data = await WM.api(API + "/control", { method: "POST", body, headers: { "Idempotency-Key": uuid() } });
    } catch (error) {
      if (error.status === 401) { showLogin("Your session expired. Sign in again."); return null; }
      if (!(error.data && error.data.results)) { WM.toast(title + " failed", "bad", humanError(error.message)); return null; }
      data = error.data; failed = true;
    }
    showResults(title, body.action, data.results);
    return { data, failed };
  }

  function humanError(message) {
    const text = WM.reason(message);
    return text ? text.charAt(0).toUpperCase() + text.slice(1) : message;
  }

  function resultOk(action, r) { return action === "start" ? r.status === "AUTHORIZED" : !!r.accepted; }

  function showResults(title, action, results) {
    const ok = results.filter((r) => resultOk(action, r));
    if (results.length === 1) {
      const r = results[0];
      if (resultOk(action, r)) WM.toast(title + " — done", "ok", action === "start" ? "Request " + WM.shortId(r.request_id) + " authorized" : null);
      else WM.toast(title + " — refused", "bad", action === "start" ? "Denied: " + WM.reason(r.reason || r.status) : "The station is disconnected, faulted or already in that state.");
      return;
    }
    $("#result-title").textContent = title + " — " + ok.length + " of " + results.length + " accepted";
    replace($("#result-list"), results.map((r) => {
      const good = resultOk(action, r);
      const detail = action === "start" ? (good ? "authorized · req " + WM.shortId(r.request_id) : "denied: " + WM.reason(r.reason || r.status)) : good ? "accepted" : "refused";
      return el("div", { class: "result-item" }, el("span", { class: "chip " + (good ? "ok" : "bad"), text: good ? "OK" : "No" }), el("span", { class: "mono", text: r.station_id }), el("span", { class: "muted", text: detail }));
    }));
    $("#result-dialog").showModal();
  }

  /* start dialog (menu 2 and 3, station card "Start") */
  const startDialog = $("#start-dialog");
  WM.closeOnBackdrop(startDialog);
  let startAll = false;
  const PRESETS = { duration: [[30, "30 s"], [60, "1 min"], [300, "5 min"], [900, "15 min"]], volume: [[10, "10 L"], [25, "25 L"], [50, "50 L"], [100, "100 L"]] };
  const modeControl = WM.segmented($("#start-mode"), (mode) => {
    $("#start-amount-field").hidden = mode === "manual";
    $("#start-amount-label").textContent = mode === "volume" ? "Target volume (litres)" : "Duration (seconds)";
    const input = $("#start-amount");
    input.step = mode === "volume" ? "0.1" : "1";
    input.value = mode === "volume" ? "25" : "60";
    replace($("#start-presets"), (PRESETS[mode] || []).map(([v, label]) => el("button", { type: "button", class: "btn sm", text: label, onclick: () => { input.value = v; } })));
  });
  modeControl.select("duration");

  function eligible() { return stations.filter((s) => s.status === "AVAILABLE" && !s.request_id); }

  function fillOperators() {
    const options = [el("option", { value: operators.default, text: operators.default + " (Central)" })];
    operators.operators.filter((o) => o.operator_id !== operators.default)
      .forEach((o) => options.push(el("option", { value: o.operator_id, text: o.operator_id + (o.name ? " — " + o.name : "") })));
    replace($("#start-operator"), options);
  }

  function openStart(stationId) { openStartDialog(false, stationId); }

  function openStartDialog(all, stationId) {
    startAll = all;
    const list = eligible();
    $("#start-error").textContent = "";
    $("#start-title").textContent = all ? "Start irrigation on all eligible stations" : "Start irrigation";
    $("#start-station-field").hidden = all;
    $("#start-station").required = !all;
    if (all) {
      $("#start-lead").textContent = list.length ? list.length + " available station(s): " + list.map((s) => s.station_id).join(", ") : "No station is available right now.";
    } else {
      $("#start-lead").textContent = "Only available stations can start an irrigation.";
      replace($("#start-station"), list.length ? list.map((s) => el("option", { value: s.station_id, text: s.station_id + " — " + s.location })) : [el("option", { value: "", text: "No available station" })]);
      if (stationId) $("#start-station").value = stationId;
    }
    $("#start-submit").disabled = list.length === 0;
    $("#start-submit").textContent = all ? "Start on " + list.length + " station(s)" : "Start irrigation";
    fillOperators();
    startDialog.showModal();
  }

  $("#start-form").addEventListener("submit", async (event) => {
    if (event.submitter && event.submitter.value === "cancel") return;
    event.preventDefault();
    const mode = modeControl.value();
    const body = { action: "start", station_id: startAll ? "all" : $("#start-station").value, operator_id: $("#start-operator").value };
    if (!body.station_id) { $("#start-error").textContent = "Choose a station."; return; }
    if (mode !== "manual") {
      const amount = Number($("#start-amount").value);
      if (!(amount > 0)) { $("#start-error").textContent = "Enter a positive number."; return; }
      if (mode === "duration") {
        if (!Number.isInteger(amount)) { $("#start-error").textContent = "Duration must be a whole number of seconds."; return; }
        body.duration_seconds = amount;
      } else body.target_volume_liters = amount;
    }
    $("#start-submit").disabled = true;
    const title = startAll ? "Start on all eligible stations" : "Start " + body.station_id;
    const outcome = await control(body, title);
    $("#start-submit").disabled = false;
    if (outcome) startDialog.close("ok");
  });

  /* one-station dialog: block / activate / stop (menu 4, 6, 8) */
  const stationDialog = $("#station-dialog");
  WM.closeOnBackdrop(stationDialog);
  let stationAction = null;
  const ONE = {
    block: { title: "Block one station", lead: "A blocked station stops any irrigation and refuses new ones until it is activated.", button: "Block station", cls: "btn warn", filter: (s) => s.admin_state !== "BLOCKED" },
    activate: { title: "Activate one station", lead: "Returns a blocked station to service.", button: "Activate station", cls: "btn primary", filter: (s) => s.admin_state === "BLOCKED" },
    stop: { title: "Stop irrigation on one station", lead: "Stops the running or pending irrigation; the operator gets the final summary.", button: "Stop irrigation", cls: "btn danger solid", filter: (s) => !!s.request_id },
  };
  function openStationDialog(action) {
    stationAction = action;
    const spec = ONE[action];
    const list = stations.filter(spec.filter);
    $("#station-title").textContent = spec.title;
    $("#station-lead").textContent = spec.lead;
    $("#station-error").textContent = "";
    replace($("#station-select"), list.length ? list.map((s) => el("option", { value: s.station_id, text: s.station_id + " — " + s.location + " (" + WM.label(s.status) + ")" })) : [el("option", { value: "", text: "No matching station" })]);
    const submit = $("#station-submit");
    submit.className = spec.cls; submit.textContent = spec.button; submit.disabled = !list.length;
    stationDialog.showModal();
  }
  $("#station-form").addEventListener("submit", async (event) => {
    if (event.submitter && event.submitter.value === "cancel") return;
    event.preventDefault();
    const id = $("#station-select").value;
    if (!id) return;
    const outcome = await control({ action: stationAction, station_id: id }, ONE[stationAction].button + " " + id);
    if (outcome) stationDialog.close("ok");
  });

  async function confirmBlock(id) {
    if (await WM.confirm({ title: "Block " + id + "?", message: "Any irrigation on it stops and new requests are refused until it is activated.", confirmText: "Block station", danger: true })) {
      control({ action: "block", station_id: id }, "Block " + id);
    }
  }
  async function stopOne(id) {
    if (await WM.confirm({ title: "Stop irrigation on " + id + "?", message: "The station closes its valve and reports the final totals.", confirmText: "Stop irrigation", danger: true })) {
      control({ action: "stop", station_id: id }, "Stop " + id);
    }
  }
  async function bulk(action, title, message, filterFn, danger) {
    const targets = stations.filter(filterFn);
    if (!targets.length) { WM.toast(title, "warn", "No station matches."); return; }
    if (await WM.confirm({ title: title + "?", message: message + " (" + targets.map((s) => s.station_id).join(", ") + ")", confirmText: title, danger })) {
      control({ action, station_id: "all" }, title);
    }
  }

  const MENU = {
    1: () => { $("#stations-section").scrollIntoView({ behavior: "smooth" }); },
    2: () => openStartDialog(false),
    3: () => openStartDialog(true),
    4: () => openStationDialog("block"),
    5: () => bulk("block", "Block all stations", "Every station stops irrigating and refuses new requests", () => true, true),
    6: () => openStationDialog("activate"),
    7: () => bulk("activate", "Activate all stations", "Every blocked station returns to service", () => true, false),
    8: () => openStationDialog("stop"),
    9: () => bulk("stop", "Stop all active irrigations", "Every running or pending irrigation is stopped", (s) => !!s.request_id, true),
  };
  document.querySelectorAll("[data-menu]").forEach((b) => b.addEventListener("click", () => MENU[b.dataset.menu]()));
  document.addEventListener("keydown", (event) => {
    if (event.target.closest("input, select, textarea") || document.querySelector("dialog[open]") || event.metaKey || event.ctrlKey || event.altKey || $("#app").hidden) return;
    if (MENU[event.key]) { event.preventDefault(); MENU[event.key](); }
  });
  $("#station-filter").addEventListener("input", renderStations);

  /* ---------- live stream ---------- */
  const lastSeen = {};
  function changed(key, ...values) {
    const text = JSON.stringify(values);
    if (lastSeen[key] === text) return false;
    lastSeen[key] = text;
    return true;
  }

  // Central pushes its whole picture whenever anything changes.
  function applyState(state) {
    const { overview, irrigations } = state;
    stations = state.stations;
    // Re-render only what changed, so buttons are not rebuilt under the cursor.
    if (changed("stats", overview.by_status, overview.blocked, overview.live_irrigations)) renderStats(overview);
    if (changed("stations", stations)) renderStations();
    if (changed("live", irrigations.live)) renderLive(irrigations.live);
    if (changed("history", irrigations.recent)) renderHistory(irrigations.recent);
  }

  async function startConsole() {
    $("#login").hidden = true;
    $("#app").hidden = false;
    $("#sign-out").hidden = false;
    addEvents = addEvents || WM.eventTable({ body: $("#event-rows"), filter: $("#event-filter"), empty: $("#event-empty") });
    try { operators = await WM.api(API + "/operators"); } catch (e) {
      if (e.status === 401) { showLogin("That token was not accepted: it must be exactly the CENTRAL_ADMIN_TOKEN this Central was started with."); return; }
    }
    if (!live) live = WM.stream({ path: WM.config.streamPath, onState: applyState, onEvents: addEvents,
      onUnauthorized: () => { live = null; showLogin("Your session expired. Sign in again."); } });
  }

  if (token()) startConsole(); else showLogin();
})();
