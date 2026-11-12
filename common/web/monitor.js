/* WM_WS_M console (read-only): Engine health checks and Central registration. */
(function () {
  "use strict";
  const { el, $, replace } = WM;

  $("#brand-mark").appendChild(WM.svg(["M3 12h4l3-8 4 16 3-8h4"]));
  $("#station-sub").textContent = "Station " + (WM.config.wsId || "?");

  function kv(container, rows) {
    replace(container, rows.flatMap(([k, v, cls]) => [el("dt", { text: k }), el("dd", { class: cls || "", text: v })]));
  }
  function banner(kind, title, body) {
    return el("div", { class: "banner " + kind }, el("div", {}, el("div", { class: "title", text: title }), el("div", { class: "body", text: body })));
  }

  function render(s) {
    const engineUp = s.engine.connected;
    const last = s.last_health;
    const healthy = engineUp && last && last.ok;
    const central = s.central.connected;

    const banners = [];
    if (central === false) banners.push(banner("bad", "Central unreachable", "This Monitor cannot reach WM_Central. It keeps retrying and tells the Engine, which halts irrigation until Central is back."));
    if (!engineUp) banners.push(banner("warn", "Engine not connected", "Waiting for WM_WS_E to connect. Central is told the station is faulted while no Engine is supervised."));
    else if (last && !last.ok) banners.push(banner("bad", "Engine reported a fault", (last.fault ? WM.reason(last.fault) + ": " : "") + (last.detail || "health check failed")));
    replace($("#banners"), banners);

    WM.chain($("#chain"), [
      { icon: "E", name: "Engine (WM_WS_E)", state: !engineUp ? "not connected" : healthy ? "healthy" : "fault",
        },
      { icon: "M", name: "Monitor " + s.ws_id, state: s.location || "—", self: true,
        link: { ok: engineUp ? healthy : false, tag: "health checks" } },
      { icon: "C", name: "Central", state: central === true ? "registered" : central === false ? "unreachable" : "connecting",
        link: { ok: central, tag: "socket " + s.central_address } },
    ]);

    replace($("#heartbeat"), s.health.map((h) => el("i", { class: h.ok ? "ok" : "bad", title: "#" + h.sequence + " " + (h.ok ? "OK" : "KO") + " at " + WM.time(h.time) })));
    replace($("#health-chip"), !engineUp ? WM.chip("DISCONNECTED", "No Engine") : healthy ? el("span", { class: "chip ok", text: "OK" }) : el("span", { class: "chip bad", text: "KO" }));
    $("#health-sub").textContent = s.health.length ? "last " + s.health.length + " checks, every " + s.health_interval + " s" : "no checks yet";
    kv($("#health-kv"), [
      ["Engine session", engineUp ? "connected since " + WM.time(s.engine.since) : s.engine.since ? "lost at " + WM.time(s.engine.since) : "never connected"],
      ["Last check", last ? "#" + last.sequence + " · " + (last.ok ? "OK" : "KO") + " · " + WM.ago(last.time) : "—"],
      ["Timeout", s.health_timeout + " s"],
      ["Listening for Engine", "port " + s.engine_port, "mono"],
    ]);
    replace($("#fault"), s.fault_active ? el("div", { class: "fault", text: "Fault reported to Central" + (last && last.fault ? ": " + WM.reason(last.fault) : "") + ". It is cleared once the Engine answers OK again." }) : null);

    replace($("#central-chip"), central === true ? el("span", { class: "chip ok", text: "Registered" }) : central === false ? el("span", { class: "chip bad", text: "Unreachable" }) : WM.chip("PENDING", "Connecting"));
    kv($("#central-kv"), [
      ["Central address", s.central_address, "mono"],
      ["Station", s.ws_id + (s.location ? " · " + s.location : "")],
      ["Last check", s.central.checked_at ? WM.ago(s.central.checked_at) : "—"],
      ["Engine told", central === false ? "Central unreachable — halt" : "Central reachable"],
    ]);
  }

  // The Monitor pushes its status after every health check and each event at once.
  WM.stream({
    path: WM.config.streamPath || "stream",
    onState: render,
    onEvents: WM.eventTable({ body: $("#event-rows"), filter: $("#event-filter"), empty: $("#event-empty") }),
  });
})();
