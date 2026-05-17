import React from "react";
import ReactDOM from "react-dom/client";
import {
  BrowserRouter,
  Navigate,
  Route,
  Routes,
  useParams,
} from "react-router-dom";

import TodayBriefing from "./pages/today-v2/Briefing";
import ModelPageV2 from "./pages/model-v2/ModelPage";
import ForecastsSpec from "./pages/forecasts/ForecastsSpec";
import LedgerSpec from "./pages/ledger/LedgerSpec";
import { AutoDemoSession } from "./shell/AutoDemoSession";
import { DebugLayout } from "./debug/DebugLayout";
import { SignalsList } from "./debug/pages/SignalsList";
import { SignalDetailPage } from "./debug/pages/SignalDetail";
import { ThinkRunsList, ThinkRunDetail } from "./debug/pages/ThinkRuns";
import { ModelsList, ModelDetail } from "./debug/pages/Models";
import { Acts } from "./debug/pages/Acts";
import { Renders } from "./debug/pages/Renders";
import { Cache } from "./debug/pages/Cache";

import "./index.css";
import "./styles/spec.css";
import "./pages/model-v2/styles.css";

const root = document.getElementById("root");
if (!root) throw new Error("root element missing");

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Navigate to="/today" replace />} />
        <Route
          path="/today"
          element={
            <AutoDemoSession>
              <TodayBriefing />
            </AutoDemoSession>
          }
        />
        {/* Legacy focused-review deep link → in-place expansion. */}
        <Route
          path="/today/review/:deltaId"
          element={<TodayReviewRedirect />}
        />
        <Route
          path="/model"
          element={
            <AutoDemoSession>
              <ModelPageV2 />
            </AutoDemoSession>
          }
        />
        <Route
          path="/forecasts"
          element={
            <AutoDemoSession>
              <ForecastsSpec />
            </AutoDemoSession>
          }
        />
        <Route
          path="/ledger"
          element={
            <AutoDemoSession>
              <LedgerSpec />
            </AutoDemoSession>
          }
        />

        {/* Spec navigation aliases */}
        <Route path="/sources" element={<Navigate to="/model" replace />} />
        <Route path="/settings" element={<Navigate to="/today" replace />} />

        {/* Legacy redirects */}
        <Route path="/structure" element={<Navigate to="/model" replace />} />
        <Route path="/map" element={<Navigate to="/model" replace />} />
        <Route path="/history" element={<Navigate to="/ledger" replace />} />
        <Route path="/mind" element={<Navigate to="/today" replace />} />
        <Route path="/demo" element={<Navigate to="/today" replace />} />
        <Route path="/commitments" element={<Navigate to="/model" replace />} />
        <Route path="/customers" element={<Navigate to="/model" replace />} />
        <Route path="/risks" element={<Navigate to="/model" replace />} />
        <Route path="/decisions" element={<Navigate to="/model" replace />} />
        <Route path="/owners" element={<Navigate to="/model" replace />} />
        <Route path="/teams" element={<Navigate to="/model" replace />} />
        <Route path="/ask" element={<Navigate to="/today" replace />} />

        <Route path="/debug" element={<DebugLayout />}>
          <Route index element={<Navigate to="signals" replace />} />
          <Route path="signals" element={<SignalsList />} />
          <Route path="signals/:id" element={<SignalDetailPage />} />
          <Route path="think-runs" element={<ThinkRunsList />} />
          <Route path="think-runs/:id" element={<ThinkRunDetail />} />
          <Route path="models" element={<ModelsList />} />
          <Route path="models/:id" element={<ModelDetail />} />
          <Route path="acts" element={<Acts />} />
          <Route path="renders" element={<Renders />} />
          <Route path="cache" element={<Cache />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
);

// Legacy URL: /today/review/:deltaId → /today?expand=:deltaId.
// Spec §6.2 retires the dedicated focused-review route in favor of an
// in-place expansion on Today. Existing links (Slack messages,
// bookmarks) shouldn't 404.
function TodayReviewRedirect() {
  const { deltaId } = useParams<{ deltaId: string }>();
  const target = deltaId ? `/today?expand=${encodeURIComponent(deltaId)}` : "/today";
  return <Navigate to={target} replace />;
}
