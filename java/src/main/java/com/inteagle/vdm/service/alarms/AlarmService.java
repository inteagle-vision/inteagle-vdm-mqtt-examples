package com.inteagle.vdm.service.alarms;

import com.fasterxml.jackson.databind.*;
import com.inteagle.vdm.service.*;
import java.util.*;

public final class AlarmService {
  @FunctionalInterface
  public interface StateReader {
    JsonNode fetch(String connection, String device) throws Exception;
  }

  private final Store store;
  private final StateReader reader;

  public AlarmService(Store store, RpcGateway rpc) {
    this(store, (c, d) -> rpc.call(c, d, "getAlarmState", Map.of()));
  }

  public AlarmService(Store store, StateReader reader) {
    this.store = store;
    this.reader = reader;
  }

  public List<Map<String, Object>> local(String c, String d) throws Exception {
    var out = new ArrayList<Map<String, Object>>();
    for (var row : store.rows("select * from incidents where c=? and d=? order by ts desc", c, d)) {
      var item =
          Store.JSON.convertValue(
              Store.JSON.readTree((String) row.get("payload")),
              new com.fasterxml.jackson.core.type.TypeReference<
                  LinkedHashMap<String, Object>>() {});
      item.put("needsReconcile", ((Number) row.get("needs_reconcile")).intValue() != 0);
      if (row.get("reconciled_at") != null) {
        item.put("currentActive", ((Number) row.get("current_active")).intValue() != 0);
        item.put(
            "currentState",
            row.get("current_state") == null
                ? null
                : Store.JSON.readTree((String) row.get("current_state")));
        item.put("reconciledAt", row.get("reconciled_at"));
      }
      out.add(item);
    }
    return out;
  }

  public void reconcile(String c, String d) throws Exception {
    // Fence the snapshot: inbox processing may continue while RPC is in flight.
    var before =
        store.rows(
            "select alarm_id,event_id,ts,reconcile_version from incidents where c=? and d=? and"
                + " needs_reconcile=1",
            c,
            d);
    JsonNode response = reader.fetch(c, d);
    JsonNode body =
        response.has("getAlarmState") ? response.get("getAlarmState") : response.path("data");
    if (body.isMissingNode()) body = response;
    JsonNode active = body.path("active");
    if (active.isMissingNode() && response.has("getAlarmState"))
      active = Store.JSON.createArrayNode();
    if (!active.isArray())
      throw new IllegalArgumentException("getAlarmState response missing active list");
    var states = new HashMap<String, JsonNode>();
    for (var state : active) states.put(state.path("alarmId").asText(), state);
    store.transaction(
        () -> {
          for (var old : before) {
            String id = (String) old.get("alarm_id");
            JsonNode state = states.get(id);
            // A current-state snapshot has no event timestamp/transition. Preserve the
            // last observed event metadata; only replace the authoritative current state.
            store.update(
                "update incidents set"
                    + " current_active=?,current_state=?,reconciled_at=?,needs_reconcile=0 where"
                    + " c=? and d=? and alarm_id=? and event_id=? and ts=? and reconcile_version=?",
                state == null ? 0 : 1,
                state == null ? null : state.toString(),
                Store.now(),
                c,
                d,
                id,
                old.get("event_id"),
                old.get("ts"),
                old.get("reconcile_version"));
          }
        });
  }
}
