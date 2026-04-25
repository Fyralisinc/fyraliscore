import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import App from "./App";
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
        <Route path="/" element={<App />} />
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
