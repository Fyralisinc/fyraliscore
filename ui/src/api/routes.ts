export const API_ROUTES = {
  ceo: {
    home: "/view/ceo/home",
    ask: "/view/ceo/ask",
    turnAction: "/view/ceo/turn-action",
  },
  cards: {
    conversation: (cardId: string) =>
      `/v1/cards/${encodeURIComponent(cardId)}/conversation`,
    probe: (cardId: string) =>
      `/v1/cards/${encodeURIComponent(cardId)}/probe`,
  },
  decisionDeltas: {
    list: (query = "") => `/v1/decision_deltas/${query}`,
    detail: (id: string) => `/v1/decision_deltas/${encodeURIComponent(id)}`,
    accept: (id: string) =>
      `/v1/decision_deltas/${encodeURIComponent(id)}/accept`,
    delegate: (id: string) =>
      `/v1/decision_deltas/${encodeURIComponent(id)}/delegate`,
    contest: (id: string) =>
      `/v1/decision_deltas/${encodeURIComponent(id)}/contest`,
    addContext: (id: string) =>
      `/v1/decision_deltas/${encodeURIComponent(id)}/add_context`,
  },
  demo: {
    companies: "/v1/demo/companies",
    startSession: "/v1/demo/sessions/start",
    session: (sessionId: string) =>
      `/v1/demo/sessions/${encodeURIComponent(sessionId)}`,
    endSession: (sessionId: string) =>
      `/v1/demo/sessions/${encodeURIComponent(sessionId)}/end`,
    resetSession: (sessionId: string) =>
      `/v1/demo/sessions/${encodeURIComponent(sessionId)}/reset`,
    simulatorSuggested: "/v1/demo/simulator/suggested",
    simulatorInject: "/v1/demo/simulator/inject",
  },
  forecasts: {
    page: (horizonDays: number) =>
      `/v1/forecasts/page?horizon_days=${horizonDays}`,
    detail: (id: string) =>
      `/v1/forecasts/detail/${encodeURIComponent(id)}`,
    patterns: "/v1/forecasts/patterns",
    ask: "/v1/forecasts/ask",
    accuracy: (rangeDays: number) =>
      `/v1/forecasts/accuracy?days=${rangeDays}`,
    createScenario: "/v1/forecasts/",
  },
  map: {
    snapshot: (query = "") => `/map/snapshot${query}`,
    topologyEvents: (query = "") => `/map/topology_events${query}`,
    modelStory: (modelId: string) =>
      `/map/models/${encodeURIComponent(modelId)}`,
  },
  modelPage: {
    overview: (mode: string) =>
      `/model/overview?mode=${encodeURIComponent(mode)}`,
    categoryFocus: (categoryId: string, mode: string) =>
      `/model/categories/${encodeURIComponent(categoryId)}/focus?mode=${encodeURIComponent(mode)}`,
    relationship: (bundleId: string) =>
      `/model/relationships/${encodeURIComponent(bundleId)}`,
    item: (itemId: string) => `/model/items/${encodeURIComponent(itemId)}`,
    trace: (itemId: string, direction: string, depth: number) =>
      `/model/items/${encodeURIComponent(itemId)}/trace?direction=${direction}&depth=${depth}`,
  },
  modelTrace: {
    trace: (nodeId: string, query: string) =>
      `/v1/model/${encodeURIComponent(nodeId)}/trace?${query}`,
    supports: (nodeId: string) =>
      `/v1/model/${encodeURIComponent(nodeId)}/supports`,
    dependsOn: (nodeId: string) =>
      `/v1/model/${encodeURIComponent(nodeId)}/depends_on`,
  },
  recommendationStream: "/v1/recommendations/stream",
  spec: {
    operatingThreads: (query = "") =>
      `/v1/spec/operating_threads/${query}`,
    operatingThread: (id: string) =>
      `/v1/spec/operating_threads/${encodeURIComponent(id)}`,
    recentModelChanges: "/v1/spec/operating_threads/recent_changes",
    decisionDeltas: "/v1/spec/decision_deltas/",
    decisionDelta: (id: string) =>
      `/v1/spec/decision_deltas/${encodeURIComponent(id)}`,
    decisionDeltaMutation: (id: string, op: string) =>
      `/v1/spec/decision_deltas/${encodeURIComponent(id)}/${op}`,
    forecasts: "/v1/spec/forecasts/",
    forecast: (id: string) =>
      `/v1/spec/forecasts/${encodeURIComponent(id)}`,
    ledgerEvents: (query = "") => `/v1/spec/ledger_events/${query}`,
  },
  structure: {
    overlay: (commitmentId: string) =>
      `/v1/structure/overlay/${encodeURIComponent(commitmentId)}`,
    recent: (sinceMinutes: number) =>
      `/v1/structure/recent?since_minutes=${sinceMinutes}`,
    resourceAggregate: "/v1/structure/resources/aggregate",
    resourceOverlay: (resourceId: string) =>
      `/v1/structure/resources/${encodeURIComponent(resourceId)}/overlay`,
  },
  today: {
    legacy: "/v1/today",
    brand: "/v1/today/brand",
    artifact: (kind: string, id: string) =>
      `/v1/artifacts/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`,
    recommendationAct: (recommendationId: string) =>
      `/v1/recommendations/${encodeURIComponent(recommendationId)}/act`,
    recommendationDismiss: (recommendationId: string) =>
      `/v1/recommendations/${encodeURIComponent(recommendationId)}/dismiss`,
    recommendationTriage: (recommendationId: string) =>
      `/v1/recommendations/${encodeURIComponent(recommendationId)}/triage`,
    recommendationWatch: (recommendationId: string) =>
      `/v1/recommendations/${encodeURIComponent(recommendationId)}/watch`,
  },
  todayPage: {
    root: (query = "") => `/today${query}`,
    delta: (deltaId: string) =>
      `/today/deltas/${encodeURIComponent(deltaId)}`,
    evidence: (deltaId: string) =>
      `/today/deltas/${encodeURIComponent(deltaId)}/evidence`,
    apply: (deltaId: string) =>
      `/today/deltas/${encodeURIComponent(deltaId)}/apply`,
    delegate: (deltaId: string) =>
      `/today/deltas/${encodeURIComponent(deltaId)}/delegate`,
    correction: (deltaId: string) =>
      `/today/deltas/${encodeURIComponent(deltaId)}/correction`,
  },
} as const;
