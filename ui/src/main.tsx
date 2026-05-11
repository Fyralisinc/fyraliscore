import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import DemoLanding from "./pages/DemoLanding";
import DemoPicker from "./pages/DemoPicker";
import Structure from "./pages/Structure";
import History from "./pages/History";
import MyMind from "./pages/MyMind";
import Bench from "./pages/Bench";
import BenchNew from "./pages/BenchNew";
import BenchRun from "./pages/BenchRun";
import BenchProfile from "./pages/BenchProfile";
import BenchCompare from "./pages/BenchCompare";
import BenchTrends from "./pages/BenchTrends";
import BenchBaselines from "./pages/BenchBaselines";
import { DebugLayout } from "./debug/DebugLayout";
import { SignalsList } from "./debug/pages/SignalsList";
import { SignalDetailPage } from "./debug/pages/SignalDetail";
import { ThinkRunsList, ThinkRunDetail } from "./debug/pages/ThinkRuns";
import { ModelsList, ModelDetail } from "./debug/pages/Models";
import { Acts } from "./debug/pages/Acts";
import { Renders } from "./debug/pages/Renders";
import { Cache } from "./debug/pages/Cache";
import "./index.css";

// Router: CEO view at /, inspector at /debug/*.
const root = document.getElementById("root");
if (!root) throw new Error("root element missing");

ReactDOM.createRoot(root).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<DemoLanding />} />
        <Route path="/demo" element={<DemoPicker />} />
        <Route path="/structure" element={<Structure />} />
        <Route path="/history" element={<History />} />
        <Route path="/mind" element={<MyMind />} />
        <Route path="/bench" element={<Bench />} />
        <Route path="/bench/new" element={<BenchNew />} />
        <Route path="/bench/runs/:runId" element={<BenchRun />} />
        <Route
          path="/bench/runs/:runId/profile/:kind"
          element={<BenchProfile />}
        />
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
