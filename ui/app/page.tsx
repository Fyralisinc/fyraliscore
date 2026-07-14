import type { Metadata } from "next";

import { HomePage } from "@/features/platform/components/home-page";

export const metadata: Metadata = {
  title: "Fyralis",
  description: "Customer-facing Fyralis landing page and BYOC setup entry."
};

export default function Page() {
  return <HomePage />;
}
