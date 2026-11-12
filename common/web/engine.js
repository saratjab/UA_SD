/* WM_WS_E console: valve, current irrigation, links to Monitor and Central. */
(function () {
  "use strict";
  const { el, $, replace } = WM;
  let status = null;

  $("#brand-mark").appendChild(WM.svg(WM.icons.drop));
  $("#valve").prepend(WM.svg(WM.icons.valve));
  $("#station-sub").textContent = "Station " + (WM.config.wsId || "?");

  function banner(kind, title, body) {
    return el("div", { class: "banner " + kind }, el("div", {}, el("div", { class: "title", text: title }), el("div", { class: "body", text: body })));
  }

  function render(s) {
    const watering = s.state === "WATERING";
    const monitorUp = s.monitor.connected;
    const centralUp = monitorUp && s.central.reachable;

    /* banners: what the other components told this Engine */
    const banners = [];
    if (!monitorUp) banners.push(banner("bad", "Monitor disconnected", "No health-check session with WM_WS_M. Irrigation is stopped and new starts are refused until the Monitor accepts this Engine again."));
    else if (!s.central.reachable) banners.push(banner("bad", "Monitor reports Central unreachable", "WM_WS_M lost its connection to WM_Central, so this station cannot be supervised: irrigation was halted and new starts are refused until the Monitor reports Central is back."));
    if (s.failed) banners.push(banner("warn", "Simulated failure active", "The Engine answers KO to health checks. Central shows this station as faulted until you clear the failure."));
    if (s.blocked) banners.push(banner("warn", "Blocked by Central", "Central blocked this station. It will not water until Central activates it."));
    replace($("#banners"), banners);

    /* chain */
    WM.chain($("#chain"), [
      { icon: "E", name: "Engine " + (s.ws_id || ""), state: WM.label(s.state), self: true },
      { icon: "M", name: "Monitor (WM_WS_M)", state: monitorUp ? "health checks OK" : "not connected",
        link: { ok: monitorUp, tag: "health checks" } },
      { icon: "C", name: "Central", state: !monitorUp ? "unknown (no Monitor)" : s.central.reachable ? "reachable via Monitor" : "unreachable",
        link: { ok: monitorUp ? s.central.reachable : null, tag: "via Monitor socket" } },
    ]);

    /* hero */
    const valve = $("#valve");
    valve.className = "valve" + (watering ? " on" : s.failed ? " fail" : "");
    $("#state-text").textContent = watering ? "Watering" : s.failed ? "Failed" : "Idle";
    $("#state-sub").textContent = watering ? "Valve open · nominal flow " + WM.flow(s.flow_rate_lpm) : s.valve_open ? "Valve open" : "Valve closed";
    replace($("#state-chip"), WM.chip(s.state));

    const irr = s.irrigation;
    if (irr) {
      const t = s.telemetry || {};
      const item = { duration_seconds: irr.duration_seconds, target_volume_liters: irr.target_volume_liters,
        elapsed_seconds: t.duration_seconds, volume_liters: t.accumulated_volume_liters };
      replace($("#irrigation"),
        el("div", { style: "margin-top:16px" },
          el("div", { class: "row", style: "margin-bottom:8px" },
            el("span", { class: "muted", text: WM.startedBy(irr.operator_id) + " · " + WM.mode(irr) }),
            el("div", { class: "grow" }),
            el("span", { class: "mono muted", title: irr.request_id, text: "req " + WM.shortId(irr.request_id) })),
          WM.progress(item),
          el("div", { class: "metrics" },
            metric("Elapsed", WM.duration(t.duration_seconds)), metric("Volume", WM.litres(t.accumulated_volume_liters)), metric("Flow", WM.flow(t.flow_rate_lpm)))));
    } else {
      replace($("#irrigation"), el("div", { class: "empty", style: "margin-top:16px" },
        el("div", { class: "title", text: "No irrigation running" }),
        "Starts arrive from Central (authorized FO or Central requests) or from the manual start below."));
    }

    /* controls */
    const local = s.local_menu;
    $("#btn-start").hidden = !local.enabled;
    $("#btn-start").disabled = !local.enabled || !!irr || !!local.pending_request || watering || !centralUp || s.failed || s.blocked;
    $("#btn-stop").disabled = !watering && !irr && !local.pending_request;
    $("#btn-fail").disabled = s.failed;
    $("#btn-clear").disabled = !s.failed;
    $("#local-hint").textContent = local.pending_request ? "Waiting on request " + WM.shortId(local.pending_request) + "." : "";

    const last = s.last_completed;
    replace($("#last-completed"), last ? el("div", { class: "card flat", style: "padding:12px" },
      el("div", { class: "row", style: "margin-bottom:6px" }, el("h3", { text: "Last irrigation" }), el("div", { class: "grow" }), el("span", { class: "muted", text: WM.time(last.finished_at) })),
      el("dl", { class: "kv" },
        el("dt", { text: "Ended by" }), el("dd", { text: WM.reason(last.end_reason) }),
        el("dt", { text: "Duration" }), el("dd", { text: WM.duration(last.duration_seconds) }),
        el("dt", { text: "Volume" }), el("dd", { text: WM.litres(last.accumulated_volume_liters) }),
        el("dt", { text: "Operator" }), el("dd", { text: last.operator_id ? WM.startedBy(last.operator_id) : "—" }),
        el("dt", { text: "Request" }), el("dd", { class: "mono", text: WM.shortId(last.request_id) }))) : null);
  }

  function metric(label, value) {
    return el("div", { class: "metric" }, el("div", { class: "label", text: label }), el("div", { class: "value", text: value }));
  }

  async function act(path, body, button) {
    button.disabled = true;
    try {
      const data = await WM.api(path, { method: "POST", body: body || {} });
      WM.toast(data.message, "ok");
    } catch (error) {
      WM.toast(error.message, "bad");
    }
  }
  $("#btn-start").addEventListener("click", (e) => act("api/irrigation/start", {}, e.currentTarget));
  $("#btn-stop").addEventListener("click", async (e) => {
    const button = e.currentTarget;
    if (await WM.confirm({ title: "Stop irrigation?", message: "The valve closes and the final totals are reported to Central and the operator.", confirmText: "Stop irrigation", danger: true })) act("api/irrigation/stop", {}, button);
  });
  $("#btn-fail").addEventListener("click", async (e) => {
    const button = e.currentTarget;
    if (await WM.confirm({ title: "Simulate an Engine failure?", message: "Any irrigation stops; the Monitor reports the fault to Central.", confirmText: "Simulate failure", danger: true })) act("api/failure", { active: true }, button);
  });
  $("#btn-clear").addEventListener("click", (e) => act("api/failure", { active: false }, e.currentTarget));

  // The Engine pushes its status and events the moment they change.
  WM.stream({
    path: WM.config.streamPath || "stream",
    onState: (next) => { status = next; render(status); },
    onEvents: WM.eventTable({ body: $("#event-rows"), filter: $("#event-filter"), empty: $("#event-empty") }),
  });
})();
