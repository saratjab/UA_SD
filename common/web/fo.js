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
            el("div", { class: "card-sub mono", title: r.request_id, text: "req " + WM.shortId(r.request_id) + (r.submitted_at ? " · sent " + WM.time(r.submitted_at) : "") }),
            r.origin && r.origin.file ? el("div", { class: "origin-tag", text: "From " + r.origin.file + " · line " + r.origin.line }) : null),
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

  /* ---------- request file ---------- */
  // The file is read in the browser and its text sent to this FO, which sends
  // the requests one at a time (4 s after each one concludes) and pushes the
  // run's progress back over the live stream.
  const FILE_STATE_LABEL = { RUNNING: "Running", WAITING: "Waiting", CANCELLING: "Cancelling", FINISHED: "Finished", CANCELLED: "Cancelled" };
  const LINE_LABEL = { QUEUED: "Queued", SENT: "In progress", SKIPPED: "Skipped", SEND_FAILED: "Not sent", CANCELLED: "Not sent" };
  let fileRun = null;
  let lastFileRun = "";
  let fileTick = null;
  const fileInput = $("#file-input");

  function fileRunning() { return !!fileRun && ["RUNNING", "WAITING", "CANCELLING"].includes(fileRun.state); }
  function syncFileButtons() {
    $("#file-run").disabled = !fileInput.files.length || fileRunning();
    $("#file-cancel").hidden = !fileRun || !["RUNNING", "WAITING"].includes(fileRun.state);
  }
  fileInput.addEventListener("change", () => { $("#file-error").textContent = ""; syncFileButtons(); });

  $("#file-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const file = fileInput.files[0];
    if (!file) return;
    $("#file-error").textContent = "";
    $("#file-run").disabled = true;
    try {
      const content = await file.text();
      const run = await WM.api("api/request-file", { method: "POST", body: { filename: file.name, content } });
      applyFileRun(run);
      WM.toast("Processing " + run.filename, "ok", run.total + " request(s), one at a time" + (run.skipped ? " · " + run.skipped + " line(s) skipped" : ""));
      fileInput.value = "";
    } catch (error) {
      $("#file-error").textContent = error.message;
    } finally {
      syncFileButtons();
    }
  });

  $("#file-cancel").addEventListener("click", async () => {
    $("#file-cancel").disabled = true;
    try {
      const result = await WM.api("api/request-file/cancel", { method: "POST", body: {} });
      applyFileRun(result.file_run);
    } catch (error) {
      $("#file-error").textContent = error.message;
    } finally {
      $("#file-cancel").disabled = false;
    }
  });

  function lineOutcome(item) {
    if (item.state === "SKIPPED") return { chip: WM.chip("SKIPPED", LINE_LABEL.SKIPPED), why: item.error, cls: "skipped" };
    if (item.state === "SEND_FAILED") return { chip: WM.chip("SEND_FAILED", LINE_LABEL.SEND_FAILED), why: "Could not reach Kafka: " + item.error, cls: "failed" };
    if (item.state === "CANCELLED") return { chip: WM.chip("CANCELLED", LINE_LABEL.CANCELLED), why: "The run was cancelled before this line.", cls: "" };
    if (item.state === "QUEUED") return { chip: WM.chip("QUEUED", LINE_LABEL.QUEUED), why: "", cls: "" };
    // Sent: show the live state of its request, as Central reports it.
    const live = requests.find((r) => r.request_id === item.request_id);
    const status = item.state === "DONE" ? (item.outcome || (live && live.status)) : (live ? live.status : "SUBMITTED");
    const why = live ? summary(live) : item.reason ? WM.reason(item.reason) : "";
    const bad = ["DENIED", "REJECTED", "INTERRUPTED", "NO_RESULT"].includes(status);
    return { chip: WM.chip(status), why, cls: bad ? "failed" : item.state === "SENT" ? "current" : "" };
  }

  function renderFileRun() {
    const panel = $("#file-run-panel");
    if (fileTick) { clearTimeout(fileTick); fileTick = null; }
    syncFileButtons();
    if (!fileRun) { replace(panel); $("#file-progress").textContent = ""; return; }
    let stateText = FILE_STATE_LABEL[fileRun.state] || WM.label(fileRun.state);
    if (fileRun.state === "WAITING" && fileRun.next_at) {
      const left = Math.max(0, Math.ceil((new Date(fileRun.next_at) - Date.now()) / 1000));
      stateText = "Next request in " + left + " s";
      fileTick = setTimeout(renderFileRun, 500);
    }
    $("#file-progress").textContent = fileRun.sent + " of " + fileRun.total + " sent" + (fileRun.skipped ? " · " + fileRun.skipped + " skipped" : "");
    const head = el("div", { class: "row", style: "margin-bottom:8px;flex-wrap:wrap" },
      el("div", { class: "grow" },
        el("div", { class: "card-title mono", text: fileRun.filename }),
        el("div", { class: "card-sub", text: "Started " + WM.time(fileRun.started_at) + (fileRun.finished_at ? " · ended " + WM.time(fileRun.finished_at) : "") })),
      WM.chip(fileRun.state, stateText));
    const rows = fileRun.items.map((item) => {
      const outcome = lineOutcome(item);
      return el("tr", { class: outcome.cls },
        el("td", { class: "mono", text: item.line ? String(item.line) : "—" }),
        el("td", { class: "mono", text: item.ws_id || "—" }),
        el("td", { text: item.mode || "—" }),
        el("td", {}, outcome.chip),
        el("td", { class: "mono", title: item.request_id || "", text: item.request_id ? WM.shortId(item.request_id) : "—" }),
        el("td", { class: "why", text: outcome.why || "" }));
    });
    replace(panel, el("div", { class: "card flat", style: "padding:12px" }, head,
      el("div", { class: "table-wrap", style: "box-shadow:none" },
        el("table", { class: "file-lines" },
          el("thead", {}, el("tr", {}, ["Line", "Station", "Request", "Status", "Request ID", "Details"].map((h) => el("th", { text: h })))),
          el("tbody", {}, rows))),
      fileRun.summary ? el("div", { class: "muted", style: "font-size:12.5px;margin-top:8px", text: "Result: " + fileRun.summary + "." }) : null));
  }

  function applyFileRun(next) {
    const text = JSON.stringify(next || null);
    if (text === lastFileRun) return;
    const wasRunning = fileRunning();
    lastFileRun = text;
    fileRun = next || null;
    renderFileRun();
    if (wasRunning && fileRun && !fileRunning()) {
      WM.toast(fileRun.filename + ": " + (FILE_STATE_LABEL[fileRun.state] || fileRun.state).toLowerCase(),
        fileRun.state === "FINISHED" ? "ok" : "bad", fileRun.summary);
    }
  }

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
    if (fileRun) renderFileRun();
    // A request just finished: its station may be available again.
    const after = new Set(requests.filter((r) => ACTIVE.includes(r.status)).map((r) => r.request_id));
    const finished = requests.filter((r) => before.includes(r.request_id) && !after.has(r.request_id));
    finished.forEach((done) => {
      const where = done.origin && done.origin.line ? " (file line " + done.origin.line + ")" : "";
      WM.toast(done.ws_id + ": " + WM.label(done.status) + where, FINISHED_OK.includes(done.status) ? "ok" : "bad", summary(done));
    });
    if (finished.length) setTimeout(loadStations, 500);
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
    onState: (state) => { applyStatus(state.status); applyRequests(state.requests); applyFileRun(state.file_run); },
    onEvents: addEvents,
  });
  // Station availability belongs to Central: ask it on load, after every
  // request, and every few seconds while the page is visible.
  loadStations();
  setInterval(() => { if (!document.hidden && !dialog.open) loadStations(); }, 5000);
})();
