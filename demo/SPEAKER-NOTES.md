# Demo Speaker Notes

Three rehearsable scripts, one per company, each ~5 minutes. Read these
before any pitch day. Run `python -m demo.demo_doctor` (TODO Session 6
proper) to verify the demo paths still work — at minimum, walk each
flow once before going live.

The notes interleave **what to say** (italicized lines you can read
verbatim) with **what to click** (bracketed actions). The "why this
beat lands" is in plain text so you can adjust if the room is reading
differently than you expected.

---

## Truss — "founder at full cognitive load"

**Open at `/demo`. Click the Truss card.**

> *"This is Truss. 40 people, just closed a Series A, building developer
> infrastructure. The founder — Maya — is operating at full cognitive
> load. Too many customers to track in her head, too many parallel
> workstreams. We're going to look at the world through her eyes."*

**[Wait ~10 seconds for the snapshot to load. Land on action list.]**

> *"This is what Maya sees when she opens her laptop. Six things flagged
> for her attention. None of them are crises. They're the things she'd
> have forgotten about by lunch."*

**Drill into the customer-pressure recommendation (3 design partners
asked about SSO).**

> *"Notice the system isn't saying 'build SSO.' It's saying: three
> design partners asked about it in the last 60 days, $280K of ARR is
> exposed, and here are the conversations it cited as evidence."*

This is the moment that lands the value proposition: the system has
*synthesized* across customer conversations. Maya didn't track this in
her head; the system did.

**[Open the simulator panel. Click the suggested signal "Linear just
asked us about SSO too".]**

> *"Now I'm playing Maya's AE on Slack. He just got off a call. Watch
> the action list."*

**[Send. Wait ~5–8 seconds. The recommendation updates with revised
revenue exposure.]**

> *"Same recommendation, updated impact, the system added the new
> conversation as evidence. Maya didn't have to do anything except
> notice that the number went up."*

**Closing point:**

> *"What you're seeing: the work of synthesizing customer signal that
> a founder normally does in their head, but at the throughput a 40-
> person AI-native company actually operates at."*

---

## Northwind Software — "normal Tuesday at a Series B"

**Open at `/demo`. Click the Northwind card.**

> *"Series B SaaS, 180 people, $14M ARR, growing 80%. Most things are
> working. The CEO — Jordan — past the founder-overload stage. The
> action list helps stay ahead of the small fires before they become
> big ones."*

**[Land on action list.]**

> *"Six recommendations. Notice they're substantive but not alarming.
> A capacity reallocation, a 14-month-old architecture decision worth
> revisiting, a manager that's gone 6 weeks without 1:1s, customer
> pressure on SAML SSO."*

The demo's job here is to show the product earning its keep on a normal
day. Don't oversell — let it look like calm, useful work.

**Drill into the SAML SSO recommendation.**

> *"Three customers asked, $410K ARR exposed, here are the calendar
> meetings and Slack threads where it came up. The system isn't
> guessing — it's pointing at specific evidence."*

**[Open the simulator. Click the suggested signal "Acme is asking about
SAML again".]**

**[Send. Wait. The recommendation updates.]**

> *"Same recommendation, fresh evidence, fresh impact estimate. Jordan
> can keep running the company; the action list keeps up."*

**Closing point:**

> *"This is the product on a Tuesday. Not in a crisis, not in a
> celebration. The boring, important work."*

---

## Meridian Industrial — "$4.2M customer escalating"

**Open at `/demo`. Click the Meridian card.**

> *"Series C, 1100 people, $85M ARR, supply chain optimization for
> heavy industry. They have a problem. A $4.2M customer — Industrium —
> is escalating about a missed feature commitment. Watch."*

**[Land on action list.]**

> *"Top of the list: the Industrium escalation. Four other items below
> it about the same thread — capacity, executive engagement, decision
> revisit, pattern observation."*

**Drill into the top recommendation.**

> *"It's pulling from the customer escalation thread, the slipping
> commitments, and the original commitment that grew 3x in scope. The
> CEO doesn't have to assemble this picture. It's already assembled."*

**[Open the simulator. Click "Industrium gives 2-week extension".]**

**[Send. Wait. New recommendations land — restructuring the commitment,
specific milestone proposal.]**

> *"The CEO didn't ask for this. They sent a single signal — 'they're
> giving us 2 more weeks if we commit to a milestone' — and the system
> reorganized its recommendations around the new constraint."*

**Closing point:**

> *"This is the product on a real day at a real company. Not curated,
> not pre-rendered. The system's reasoning over the substrate, in the
> moment."*

---

## Failure modes — what to do if something breaks live

| What you see | What to say | What to do |
|---|---|---|
| "Demo limit reached" message in action list | "Let me reset and we'll re-run that beat." | Click Reset, wait ~10s, retry. |
| Action list empty after start | "One sec — picking up the snapshot." | Refresh once. If still empty, switch demo company. |
| Simulator returns 500 | Don't acknowledge — the audience didn't see it. | Wait 3s, click another suggested signal. |
| LLM call slow (>15s after a signal) | "Here's the substrate at work — what you're seeing is the system reasoning across 9 months of accumulated signal." | Buy time. The recommendation should land. If not, click Reset. |
| Picker page shows blank | Backend is down. Stop the pitch and reschedule. | Don't try to recover — call the engineering team. |

## Pre-pitch checklist (do before the meeting)

- [ ] Open `/demo` in a fresh browser. Pick each company. Confirm action
      list loads in <30s.
- [ ] Send the headline suggested signal for each company. Confirm the
      action list updates within 10s.
- [ ] Verify the cost counter is tracking — should show ~$0.10–0.30
      after a single signal.
- [ ] End each session. Confirm Reset works on at least one.

## Branding

The picker page calls these "simulated companies based on common
organizational patterns." Don't claim they're real. If asked: *"These
are curated scenarios. Real customer trials are a separate enterprise
process — we never demo with another customer's data."*
