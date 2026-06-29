import { describe, expect, it } from "vitest";

import { SAMPLE_CONTROL_PANEL_STATE } from "@/features/control-panel/data/mock-control-panel";
import {
  API_SURFACES,
  HOST_NAV,
  OBSERVABILITY_SURFACES,
  PRESERVED_SURFACES
} from "../data/surfaces";

describe("product console surface inventory", () => {
  it("keeps host navigation limited to internal surfaces", () => {
    expect(HOST_NAV.map((item) => item.href)).toEqual([
      "/host/control-panel",
      "/host/surfaces",
      "/host/observability"
    ]);
  });

  it("preserves the required BYOC and observability surfaces", () => {
    expect(PRESERVED_SURFACES.map((surface) => surface.title)).toContain(
      "BYOC Onboarding App"
    );
    expect(PRESERVED_SURFACES.map((surface) => surface.title)).toContain(
      "BYOC Control Panel"
    );
    expect(PRESERVED_SURFACES.map((surface) => surface.title)).toContain(
      "UI-Facing Backend APIs"
    );
    expect(PRESERVED_SURFACES.map((surface) => surface.title)).toContain(
      "Operator Observability"
    );
  });

  it("keeps the control-panel and control-plane endpoint families explicit", () => {
    const routes = API_SURFACES.flatMap((surface) => surface.routes);
    expect(routes).toContain("GET /byoc/control-panel/deployments");
    expect(routes).toContain("GET /byoc/control-panel/state");
    expect(routes).toContain("POST /byoc/agent/enroll");
    expect(routes).toContain("POST /byoc/agent/heartbeat");
  });

  it("keeps operator observability surfaces discoverable", () => {
    expect(OBSERVABILITY_SURFACES.map((surface) => surface.name)).toContain(
      "Grafana"
    );
    expect(OBSERVABILITY_SURFACES.map((surface) => surface.name)).toContain(
      "Prometheus"
    );
    expect(OBSERVABILITY_SURFACES.some((surface) =>
      surface.location.includes("observability/grafana/dashboards")
    )).toBe(true);
  });

  it("keeps sample control-panel state inside the sanitized metadata boundary", () => {
    expect(SAMPLE_CONTROL_PANEL_STATE.stored_scope).toBe(
      "sanitized_control_panel_metadata_only"
    );
    expect(SAMPLE_CONTROL_PANEL_STATE.overview.stored_scope).toBe(
      "sanitized_deployment_metadata_only"
    );
    expect(SAMPLE_CONTROL_PANEL_STATE.product_health.stored_scope).toBe(
      "sanitized_product_health_metadata_only"
    );
  });
});
