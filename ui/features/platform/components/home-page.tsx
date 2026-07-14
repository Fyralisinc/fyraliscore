import { ArrowRight, ShieldCheck } from "lucide-react";
import Link from "next/link";

export function HomePage() {
  return (
    <main className="min-h-screen bg-background px-5 py-6 text-foreground md:px-7">
      <header className="mx-auto flex max-w-6xl items-center justify-between gap-4">
        <Link href="/" className="inline-flex items-center gap-3">
          <span className="grid h-10 w-10 place-items-center rounded-lg bg-primary text-lg font-black text-primary-foreground">
            F
          </span>
          <span>
            <strong className="block text-sm">Fyralis</strong>
            <span className="text-xs font-medium text-muted-foreground">
              Customer-cloud AI workspace
            </span>
          </span>
        </Link>
        <Link
          href="/onboarding/get-fyralis"
          className="inline-flex min-h-10 items-center gap-2 rounded-md bg-primary px-4 text-sm font-semibold text-primary-foreground transition hover:bg-primary/90"
        >
          Get Fyralis
          <ArrowRight className="h-4 w-4" aria-hidden="true" />
        </Link>
      </header>

      <section className="mx-auto grid max-w-6xl gap-8 pb-16 pt-24 md:pt-32">
        <div className="max-w-3xl">
          <div className="inline-flex items-center gap-2 rounded-full border border-success/30 bg-success/10 px-3 py-1 text-sm font-semibold text-success">
            <ShieldCheck className="h-4 w-4" aria-hidden="true" />
            Bring your own cloud
          </div>
          <h1 className="mt-6 text-balance text-5xl font-semibold tracking-normal md:text-7xl">
            Fyralis
          </h1>
          <p className="mt-5 max-w-2xl text-lg leading-8 text-muted-foreground">
            Fyralis helps teams run an AI workspace in their own cloud, connect approved company sources, and keep source credentials and customer data inside their boundary.
          </p>
          <div className="mt-8 flex flex-wrap gap-3">
            <Link
              href="/onboarding/get-fyralis"
              className="inline-flex min-h-11 items-center gap-2 rounded-md bg-primary px-5 text-sm font-semibold text-primary-foreground transition hover:bg-primary/90"
            >
              Get Fyralis
              <ArrowRight className="h-4 w-4" aria-hidden="true" />
            </Link>
            <span className="inline-flex min-h-11 items-center rounded-md border border-border bg-card px-5 text-sm font-semibold text-muted-foreground">
              BYOC setup starts with no secrets
            </span>
          </div>
        </div>
      </section>
    </main>
  );
}
