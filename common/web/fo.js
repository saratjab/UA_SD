/* WM_FO console: pick an available station, send requests without waiting,
 * and follow each irrigation as Central relays the station's progress. */
(function () {
  "use strict";
  const { el, $, replace } = WM;
  const ACTIVE = ["SUBMITTED", "AUTHORIZED", "WATERING"];
  const FINISHED_OK = ["COMPLETED", "STOPPED"];
  let stations = [];
  let requests = [];
  let stationTarget = null;
  let stationsLoading = false;

  $("#brand-mark").appendChild(WM.svg(WM.icons.drop));
  $("#operator-sub").textContent = "Operator " + (WM.config.operatorId || "?");
  document.title = "WM_FO " + (WM.config.operatorId || "");

  const filter = WM.segmented($("#request-filter"), () => renderRequests(true));

  /* ---------- stations ---------- */
  async function loadStations() {
    if (stationsLoading) return;
    stationsLoading = true;
    $("#refresh-stations").disabled = true;
    try {
      const data = await WM.api("api/stations");
      stations = data.stations;
      replace($("#stations-banner"));
      $("#stations-updated").textContent = "updated " + new Date().toLocaleTimeString();
      renderStations();
    } catch (error) {
      replace($("#stations-banner"), el("div", { class: "banner warn" },
        el("div", {}, el("div", { class: "title", text: "Could not load the station list" }),
          el("div", { class: "body", text: error.status === 0 ? "This FO is not reachable." : error.message + ". Central may be down; the list will retry." }))));
    } finally {
      stationsLoading = false;
      $("#refresh-stations").disabled = false;
    }
  }

  function renderStations() {
    $("#station-count").textContent = stations.length;
    renderStats(requests.filter((r) => ACTIVE.includes(r.status)));
    if (!stations.length) {
      replace($("#stations"), el("div", { class: "empty" }, el("div", { class: "title", text: "No station is available right now" }),
        "Stations that are watering, blocked, faulted or disconnected are not offered. The list refreshes automatically."));
      return;
    }
    replace($("#stations"), stations.map((s) => el("article", { class: "card flat" },
      el("div", { class: "row" },
        el("div", { class: "brand-mark", style: "background:var(--ok-soft);color:var(--ok)" }, WM.svg(WM.icons.drop)),
        el("div", { class: "grow" }, el("div", { class: "card-title mono", text: s.station_id }), el("div", { class: "card-sub", text: s.location })),
        WM.chip("AVAILABLE"),
        el("button", { class: "btn primary sm", text: "Request irrigation", onclick: () => openRequest(s) })))));
  }

  /* ---------- requests ---------- */
  function stepState(r) {
    const steps = ["Submitted", "Authorized", "Watering", "Finished"];
    let reached = 0, failedAt = -1, finalLabel = "Finished";
    switch (r.status) {
      case "SUBMITTED": reached = 0; break;
      case "AUTHORIZED": reached = 1; break;
      case "WATERING": reached = 2; break;
      case "COMPLETED": case "STOPPED": reached = 4; finalLabel = WM.label(r.status); break;
      case "INTERRUPTED": reached = 3; failedAt = 3; finalLabel = "Interrupted"; break;
      case "DENIED": reached = 1; failedAt = 1; break;
      case "REJECTED": reached = 2; failedAt = 2; break;
      case "NO_RESULT": reached = r.started_at ? 3 : r.authorized_at ? 2 : 1; failedAt = reached; finalLabel = "No result"; break;
    }
    steps[3] = finalLabel;
    if (r.status === "DENIED") steps[1] = "Denied";
    if (r.status === "REJECTED") steps[2] = "Rejected";
    return el("div", { class: "steps", "aria-label": "Request progress" }, steps.map((name, i) => {
      const cls = i === failedAt ? "failed" : i < reached ? "done" : i === reached ? "current" : "";
      return el("div", { class: "step " + cls }, el("span", { class: "dot" }), el("span", { text: name }));
    }));
  }

  function summary(r) {
    switch (r.status) {
      case "SUBMITTED": return "Waiting for Central to authorize the request…";
      case "AUTHORIZED": return "Authorized by Central — waiting for the station to open its valve…";
      case "WATERING": return "The station is watering.";
      case "COMPLETED": return "Finished: " + WM.reason(r.end_reason || "completed") + ".";
      case "STOPPED": return "Stopped before the end (" + WM.reason(r.end_reason) + ").";
      case "INTERRUPTED": return "Interrupted: " + WM.reason(r.end_reason) + ".";
      case "DENIED": return "Central denied the request: " + WM.reason(r.reason || "unknown reason") + ".";
      case "REJECTED": return "The station could not start: " + WM.reason(r.reason || "unknown reason") + ".";
      case "NO_RESULT": return "No final result arrived in time; the station may still report it.";
      default: return "";
    }
  }

  function metric(label, value) {
    return el("div", { class: "metric" }, el("div", { class: "label", text: label }), el("div", { class: "value", text: value }));
  }

  function renderRequests(force) {
    const showAll = filter.value() === "all";
    const list = requests.filter((r) => showAll || ACTIVE.includes(r.status));
    const active = requests.filter((r) => ACTIVE.includes(r.status));
    $("#request-count").textContent = active.length + " running";
    renderStats(active);
    if (!list.length) {
      replace($("#requests"), el("div", { class: "empty" },
        el("div", { class: "title", text: showAll ? "No requests yet" : "Nothing in progress" }),
        showAll ? "Requests you send appear here with live progress." : "Pick an available station to start an irrigation. Finished ones are under All."));
      return;
    }
    replace($("#requests"), list.map((r) => {
      const finished = !ACTIVE.includes(r.status);
      const bad = finished && !FINISHED_OK.includes(r.status);
      const showProgress = r.status === "WATERING" || (finished && r.started_at);
      return el("article", { class: "card" + (r.status === "WATERING" ? " status-WATERING" : bad ? " status-LEAK" : "") },
        el("div", { class: "card-head" },
          el("div", { class: "grow" },
            el("div", { class: "card-title" }, el("span", { class: "mono", text: r.ws_id }), el("span", { class: "muted", text: " · " + WM.mode(r) })),
            el("div", { class: "card-sub mono", title: r.request_id, text: "req " + WM.shortId(r.request_id) + (r.submitted_at ? " · sent " + WM.time(r.submitted_at) : "") })),
          WM.chip(r.status)),
        stepState(r),
        showProgress ? WM.progress(r, finished, bad) : null,
        r.started_at || r.volume_liters != null ? el("div", { class: "metrics" },
          metric("Elapsed", WM.duration(r.elapsed_seconds)), metric("Volume", WM.litres(r.volume_liters)), metric("Flow", r.status === "WATERING" ? WM.flow(r.flow_rate_lpm) : "—")) : null,
        el("div", { class: bad ? "fault" : "muted", style: bad ? "" : "font-size:12.5px;margin-top:6px", text: summary(r) }));
    }));
  }

  function renderStats(active) {
    const done = requests.filter((r) => FINISHED_OK.includes(r.status)).length;
    const failed = requests.filter((r) => ["DENIED", "REJECTED", "INTERRUPTED", "NO_RESULT"].includes(r.status)).length;
    const volume = requests.reduce((sum, r) => sum + (Number(r.volume_liters) || 0), 0);
    const items = [
      ["Available stations", stations.length, "var(--ok)"],
      ["Running", active.length, "var(--accent)"],
      ["Finished", done, "var(--ok)"],
      ["Denied / failed", failed, "var(--bad)"],
      ["Water delivered", WM.litres(volume), "var(--info)"],
    ];
    replace($("#stats"), items.map(([label, value, dot]) => {
      const node = el("div", { class: "stat" }, el("div", { class: "label", text: label }), el("div", { class: "value", text: value }));
      node.style.setProperty("--dot", dot);
      return node;
    }));
  }

  /* ---------- request dialog ---------- */
  const dialog = $("#request-dialog");
  WM.closeOnBackdrop(dialog);
  const PRESETS = { duration: [[30, "30 s"], [60, "1 min"], [300, "5 min"], [900, "15 min"]], volume: [[10, "10 L"], [25, "25 L"], [50, "50 L"], [100, "100 L"]] };
  const mode = WM.segmented($("#request-mode"), (value) => {
    const input = $("#request-amount");
    $("#request-amount-label").textContent = value === "volume" ? "Target volume (litres)" : "Duration (seconds)";
    input.step = value === "volume" ? "0.1" : "1";
    input.value = value === "volume" ? "25" : "60";
    replace($("#request-presets"), PRESETS[value].map(([v, label]) => el("button", { type: "button", class: "btn sm", text: label, onclick: () => { input.value = v; } })));
  });
  mode.select("duration");

  function openRequest(station) {
    stationTarget = station;
    $("#request-lead").textContent = station.station_id + " — " + station.location;
    $("#request-error").textContent = "";
    $("#request-submit").disabled = false;
    dialog.showModal();
    $("#request-amount").focus();
  }

  $("#request-form").addEventListener("submit", async (event) => {
    if (event.submitter && event.submitter.value === "cancel") return;
    event.preventDefault();
    const amount = Number($("#request-amount").value);
    const body = { ws_id: stationTarget.station_id };
    if (!(amount > 0)) { $("#request-error").textContent = "Enter a positive number."; return; }
    if (mode.value() === "duration") {
      if (!Number.isInteger(amount)) { $("#request-error").textContent = "Duration must be a whole number of seconds."; return; }
      body.duration_seconds = amount;
    } else body.target_volume_liters = amount;
    $("#request-submit").disabled = true;
    try {
      const result = await WM.api("api/irrigations", { method: "POST", body });
      dialog.close("ok");
      WM.toast("Request sent to Central", "ok", body.ws_id + " · req " + WM.shortId(result.request_id));
      filter.select("active");
      stations = stations.filter((s) => s.station_id !== body.ws_id);
      renderStations();
      setTimeout(loadStations, 1500);
    } catch (error) {
      $("#request-error").textContent = error.message;
      $("#request-submit").disabled = false;
    }
  });
  $("#refresh-stations").addEventListener("click", loadStations);

  /* ---------- live stream ---------- */
  // The FO pushes its requests (with the progress Central relays) and its
  // events the moment they change; nothing here polls.
  let lastRequests = "";
  function applyRequests(next) {
    const text = JSON.stringify(next);
    if (text === lastRequests) return;
    lastRequests = text;
    const before = requests.filter((r) => ACTIVE.includes(r.status)).map((r) => r.request_id);
    requests = next;
    renderRequests();
    // A request just finished: its station may be available again.
    const after = new Set(requests.filter((r) => ACTIVE.includes(r.status)).map((r) => r.request_id));
    const done = requests.find((r) => before.includes(r.request_id) && !after.has(r.request_id));
    if (done) {
      WM.toast(done.ws_id + ": " + WM.label(done.status), FINISHED_OK.includes(done.status) ? "ok" : "bad", summary(done));
      setTimeout(loadStations, 500);
    }
  }
  function applyStatus(data) {
    const chip = $("#central-chip");
    const link = data.central || {};
    chip.className = "chip " + (link.ok === true ? "ok" : link.ok === false ? "bad" : "");
    chip.textContent = "Central: " + (link.ok === true ? "reachable" : link.ok === false ? "not answering" : "checking");
    chip.title = link.checked_at ? "last answer " + WM.ago(link.checked_at) : "";
  }

  const addEvents = WM.eventTable({ body: $("#event-rows"), filter: $("#event-filter"), empty: $("#event-empty") });
  WM.stream({
    path: WM.config.streamPath || "stream",
    onState: (state) => { applyStatus(state.status); applyRequests(state.requests); },
    onEvents: addEvents,
  });
  // Station availability belongs to Central: ask it on load, after every
  // request, and every few seconds while the page is visible.
  loadStations();
  setInterval(() => { if (!document.hidden && !dialog.open) loadStations(); }, 5000);
})();
