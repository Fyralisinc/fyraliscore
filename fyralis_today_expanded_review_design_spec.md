# Fyralis Today Page Redesign Spec

## Expanded Proposed Change Review State

**Status:** Locked direction  
**Primary surface:** Today  
**Core object:** Proposed Change, internally a Decision Delta  
**Last updated:** May 2026

---

## 1. Purpose of This Document

This document describes the required changes needed to transform the current expanded Proposed Change card on the Today page into the intended premium, focused, in-page review experience.

A developer should be able to use this document to build the page and component behavior with full clarity.

The design intent is based on the latest locked direction:

> Today is not a task queue. Today is an executive judgment surface. It should feel like Fyralis reviewed the company, absorbed the noise, and brought forward only the few changes that need human judgment.

The expanded Proposed Change should not feel like a generic accordion, a table record, a modal, or a separate page. It should feel like a **focused in-page review sheet** that temporarily becomes the dominant object in the Today stream while keeping surrounding items visible above and below.

---

## 2. Product Role of Today

### 2.1 What Today Answers

Today answers:

```text
What needs my judgment right now?
```

It does not answer:

```text
What is true across the company?        → Model
What is forming or likely?              → Forecasts
What happened and resolved?             → Ledger
```

### 2.2 Emotional Goal

When a user lands on Today, they should feel:

1. **Relief**  
   Fyralis handled most of the noise.

2. **Clarity**  
   Only a few items need judgment.

3. **Control**  
   Every proposed change is reviewable, correctable, delegatable, and grounded.

4. **Trust**  
   Fyralis shows evidence, missing context, and consequences before asking the user to act.

5. **Momentum**  
   The user can move through items quickly without leaving the page.

---

## 3. Current Problems to Fix

The screenshot implementation still feels cheap and low-value because it treats the expanded Proposed Change as an inline admin record.

### 3.1 Structural Problems

| Problem | Why It Hurts |
|---|---|
| Expanded card behaves like a large accordion | It does not create enough review-mode focus. |
| The content is too table-like | It feels bureaucratic, not premium or intelligent. |
| The card is too horizontally spread | The user has to scan sideways instead of being guided through a judgment. |
| `Why this matters` repeats the title | The card fails to explain consequence. |
| `Evidence quality · 0 signals` conflicts with confidence | It damages trust. |
| `If you accept` is system-centric | It describes backend outcomes instead of operational value. |
| Ask Fyralis feels bolted on | The most powerful primitive appears as a generic input block. |
| Typography feels generic | The card lacks editorial composition and hierarchy. |
| Too many boxes, borders, and uppercase labels | The product feels like internal enterprise software. |
| The action bar feels like a generic toolbar | It does not feel like a decision control surface. |

### 3.2 Correct Direction

Replace the current expanded accordion style with:

```text
In-page Focused Review Card
```

The card remains in the Today stream, but when expanded it occupies most of the viewport and becomes the clear focal object.

---

## 4. Page-Level Architecture

### 4.1 Layout Regions

The Today page has these regions:

```text
┌───────────────────────────────────────────────────────────────┐
│ Left Sidebar                                                   │
├───────────────────────────────────────────────────────────────┤
│ Main Scroll Area                                               │
│                                                               │
│ Page Header                                                    │
│ Compact Briefing                                               │
│                                                               │
│ Compact item above selected card, if any                       │
│                                                               │
│ Expanded Focused Review Card                                   │
│                                                               │
│ Compact item below selected card, if any                       │
│ Handled summary / queue footer                                 │
└───────────────────────────────────────────────────────────────┘
```

### 4.2 Do Not Open a New Page

When a user clicks a Proposed Change:

- Do **not** navigate to a new route.
- Do **not** open a modal.
- Do **not** open a right-side inspector.
- Do **not** replace the whole Today page.

Instead:

```text
The selected card expands in place and becomes the primary review surface.
```

Other cards remain above and below in compact form. The user can scroll through the same page and click another item to switch focus.

---

## 5. Global Visual Style

### 5.1 Overall Mood

The expanded Today view should feel:

```text
calm
premium
editorial
trustworthy
alive
focused
review-oriented
```

It should not feel:

```text
dashboardsy
form-heavy
admin-like
boxy
spreadsheet-like
chatbot-like
```

### 5.2 Main Surface

Use a warm luminous background, not flat white and not dull beige.

Recommended tokens:

```css
--surface-page: #F7F2EA;
--surface-card: #FFFCF6;
--surface-soft: #F3EEE5;
--border-soft: #DED7C8;
--border-strong: #CFC3B3;
--text-primary: #101A16;
--text-secondary: #66726A;
--text-muted: #8B958C;
```

### 5.3 Brand / Sidebar Tokens

```css
--sidebar-bg: #071713;
--sidebar-bg-soft: #102820;
--sidebar-active: #173C2E;
--accent-moss: #2F6F4E;
--accent-moss-bright: #4F8A6A;
```

### 5.4 Semantic Colors

```css
--state-authority: #B9342A;      /* use for needs-authority risk accent */
--state-review: #C96A56;         /* warm review / correction */
--state-delegate: #C98A2E;       /* delegatable / authority handoff */
--state-monitor: #2F6F4E;        /* monitoring / stable */
--state-evidence: #315A7A;       /* evidence and trace links */
--state-forecast: #6D678B;       /* forecasts / uncertainty references */
```

### 5.5 Color Usage Rules

- Do not flood cards with red, green, or amber.
- Use color as small rails, chips, icons, or values.
- The expanded card may use one subtle top rail or side rail for state.
- The primary action should generally use deep forest green, not red, even for critical items. Red communicates risk; green communicates accepted action.

---

## 6. Typography

### 6.1 Recommended Type Roles

Use a refined serif for major page and review titles. Use a clean sans-serif for body, metadata, controls, and labels.

Do not overuse uppercase labels.

### 6.2 Page Title

```text
Today
```

- Serif
- 36–44 px desktop
- Weight: regular or medium
- Color: `--text-primary`

### 6.3 Expanded Card Title

Raw machine phrasing should be rewritten into a more human judgment statement.

Bad:

```text
Conversation-AI commitment predates 4 customer requests for ICP scoring — re-scope
```

Better:

```text
Re-scope the Conversation-AI commitment
```

Subheadline:

```text
4 customer requests for ICP scoring now exceed current scope.
```

Title style:

- Serif
- 32–40 px desktop
- Line height 1.12–1.18
- Max width: 680–760 px

### 6.4 Section Headings

Use sentence case.

Good:

```text
What changes
Why this matters
Evidence
What may be missing
If accepted
Ask Fyralis about this change
```

Avoid excessive all caps:

```text
CURRENT → PROPOSED
EVIDENCE QUALITY
WHAT MAY BE MISSING
```

Small uppercase can be used sparingly for object type labels only:

```text
PROPOSED CHANGE
```

---

## 7. Today Page Default State

### 7.1 Header

The page header should be briefing-first.

Example:

```text
Today
Fyralis reviewed the company since your last session.
98 signals processed · 94 absorbed · 4 need your judgment
```

The goal is to show that Fyralis protected attention.

### 7.2 Header Counters

Counters should be compact. Avoid giant dashboard cards.

Recommended values:

```text
2 need authority
1 delegatable
1 monitoring
12 model updates
$2.04M exposed
```

### 7.3 Default Stream

Default Today stream contains:

1. Primary expanded or semi-expanded judgment item.
2. Other compact judgment items.
3. Handled-without-you summary.

The default view may have the primary judgment partially expanded. When a user clicks it or another card, that item enters full focused review state.

---

## 8. Proposed Change Compact Card

### 8.1 Purpose

Compact cards exist to let users scan other judgment items quickly while one item may be expanded elsewhere on the page.

### 8.2 Compact Card Structure

```text
[Icon] Title                                      [Status chip] [Chevron]
       Current → Proposed summary
       Key chips: impact · affected entities · signals · confidence
```

Example:

```text
Assign owner for pricing model decision          Needs review
Unowned → CFO · Decision due in 5 business days
$720K opportunity · 2 commitments blocked · 9 signals · 66% confidence
```

### 8.3 Compact Card Visual Specs

```css
.compact-card {
  background: var(--surface-card);
  border: 1px solid var(--border-soft);
  border-radius: 16px;
  padding: 18px 22px;
  min-height: 84px;
}
```

- Left icon or state rail: 4–6 px accent
- No full evidence list
- No action bar
- No large internal boxes
- No Current vs Proposed table

### 8.4 Compact Card Click Behavior

On click:

1. Current expanded card collapses to compact form.
2. Clicked card expands in place.
3. Page scrolls the expanded card into comfortable viewport position.
4. Focus remains within Today page.

---

## 9. Expanded Focused Review Card

### 9.1 Purpose

The expanded card is the main judgment surface.

It should answer:

```text
What is being proposed?
What changes?
Why does it matter?
Can I trust it?
What might be missing?
What happens if I accept?
What can I do now?
```

### 9.2 Size and Placement

The expanded card should feel substantial but not modal.

Recommended desktop behavior:

```text
Width: 980–1120 px preferred reading width
Max width: 1180 px
Min height: 70vh
Target height: 75–88vh depending on content
```

If the viewport is wider than content max width, center the expanded card in the main area.

The user should still see compact cards above or below when scrolling.

### 9.3 Outer Shell

```css
.review-card {
  background: var(--surface-card);
  border: 1px solid var(--border-strong);
  border-radius: 22px;
  box-shadow: 0 18px 48px rgba(16, 26, 22, 0.08);
  overflow: hidden;
}
```

### 9.4 State Accent

Use one subtle accent only:

- top rail or side rail, not both
- state chip in upper-right
- no loud red card border unless truly critical

Example:

```css
.review-card[data-state="authority"] {
  border-color: rgba(185, 52, 42, 0.28);
}
```

---

## 10. Expanded Card Content Architecture

### 10.1 Top Utility Row

```text
Reviewing 4 of 7                                      Collapse review
```

Requirements:

- Keep small and quiet.
- `Collapse review` should not be styled like a primary action.
- Include optional previous / next controls if queue navigation is supported.

### 10.2 Header Block

Structure:

```text
Proposed change                                      Needs your authority

Re-scope the Conversation-AI commitment
4 customer requests for ICP scoring now exceed current scope.

From Customers & Revenue · Proposed by Fyralis · Created 11d ago · Moderate confidence
Grounded in existing model items
```

Requirements:

- The title must be human-written or transformed from raw model text.
- Do not use raw alert strings as display headlines.
- The subtitle should explain the condition or consequence.
- Use one confidence phrase, not a floating generic confidence pill only.

### 10.3 Metadata Strip

Metadata should sit below title in one line or two calm rows.

Example:

```text
From Customers & Revenue · Proposed by Fyralis · Created 11d ago · Moderate confidence
Grounded in existing model items
```

Avoid scattering these metadata fields around the card.

---

## 11. Current vs Proposed Diff

### 11.1 Purpose

The diff block shows exactly what the user is authorizing.

This is one of the most important objects on the page.

### 11.2 Do Not Use a Generic Table

Avoid:

```text
FIELD | CURRENT | PROPOSED
CURRENT | At watch | Critical
```

This feels cheap and unclear.

### 11.3 Recommended Visual Structure

Use two comparison panels connected by a subtle center axis.

```text
Current                                      Proposed
State          At watch                      State          Critical
Scope          ICP scoring for Conv-AI       Scope          ICP scoring for Conv-AI + 4 customer requests
Owner          Revenue Operations            Owner          Revenue Operations
Re-evaluation  30 days                       Re-evaluation  7 days
```

### 11.4 Diff Layout Specs

Desktop:

```css
.diff-block {
  display: grid;
  grid-template-columns: 1fr 80px 1fr;
  gap: 24px;
}
```

Middle axis:

- small icons or change markers
- thin connector lines
- not too decorative

Mobile / narrow width:

```text
Current panel
↓
Proposed panel
```

### 11.5 Highlighting Changed Values

Changed values should be subtly emphasized.

Examples:

- `Critical` in risk color
- `7 days` in warm attention color
- owner changes in moss or gold

Do not highlight unchanged values.

---

## 12. Review Body

### 12.1 Reading Order

The review body should be more vertical than the current screenshot.

Preferred order:

```text
Why this matters
Evidence
What may be missing
If accepted
Ask Fyralis
Actions
```

On desktop, some sections may appear in a light grid, but the visual flow should still read top-to-bottom.

### 12.2 Why This Matters

This is the intellectual center of the card.

Bad:

```text
Conversation-AI commitment predates 4 customer requests for ICP scoring — re-scope
```

Good:

```text
Four enterprise customers have formally requested ICP scoring from Conversation-AI in the last 10 days. Continuing at watch status risks delays to revenue expectations and customer trust. Human judgment is needed to rebalance scope, capacity, and risk.
```

Requirements:

- Explain consequence.
- Explain why now.
- Explain why human judgment is needed.
- Never simply repeat the title.

### 12.3 Evidence Section

Evidence should build trust without overwhelming.

Example:

```text
Evidence
Grounded in existing model items and recent updates.

Support tickets        4
CRM account notes      3
Planning notes         2
Model history          3 changes
```

If there are zero new signals, say that clearly:

```text
No new signals since the last evaluation.
This proposed change is grounded in existing model items and historical context.
```

Never show:

```text
78% confidence
Evidence quality · 0 signals
```

without explanation.

### 12.4 What May Be Missing

Required on all expanded Proposed Changes.

Example:

```text
What may be missing
- Capacity impact on current roadmap
- Customer prioritization rationale
- Competitive commitments or risks
```

If no gaps are known:

```text
No major context gaps identified.
```

But avoid making the system sound omniscient. Prefer:

```text
No major context gaps identified from connected sources.
```

### 12.5 If Accepted

This section should show operational outcomes, not backend bookkeeping.

Bad:

```text
Record ledger event for audit trail
```

Good:

```text
If accepted
- Reclassify to Critical and notify owners
- Shorten re-evaluation cadence to 7 days
- Allocate analysis resources this sprint
- Update customer communications plan
```

Rules:

- Show user-visible consequences first.
- Backend events can happen silently.
- If ledger recording matters, mention only in secondary metadata or success confirmation.

---

## 13. Ask Fyralis Primitive

### 13.1 Role

Ask Fyralis is not a generic chatbot. It is a contextual reasoning layer over the selected Proposed Change.

The Ask section should let users interrogate the current review without leaving the card.

### 13.2 Placement

Place Ask Fyralis **after the review body and before the action bar**.

This order is intentional:

```text
Read the case → Ask follow-up questions → Act
```

### 13.3 Ask Strip Layout

```text
Ask Fyralis about this change
[Why now?] [What if I wait?] [Who should own this?] [What evidence is weakest?]
[Ask anything about this change...                         ↵]
```

### 13.4 Suggested Prompts

Prompts should be generated based on the selected item.

Default examples:

```text
Why now?
What if I wait?
Who should own this?
What evidence is weakest?
What would make this wrong?
What changes if I accept?
```

### 13.5 Ask Response Behavior

Answers should appear inline inside the card, directly below the Ask input.

Response cards should be typed, not generic chat bubbles.

Possible response types:

```text
Explanation
Evidence summary
Scenario
Owner recommendation
Weakness / missing context
Action preview
Model link
```

Example response to `What if I wait?`:

```text
If you wait 7 days
- Customer requests remain unresolved.
- Revenue Operations keeps ownership, but scope pressure remains unclassified.
- Fyralis will re-evaluate if no new requests appear.

Recommendation
Accept re-scope or delegate ownership review before the next GTM planning checkpoint.
```

Actions:

```text
[Accept change] [Delegate review] [Open in Model]
```

### 13.6 Ask Must Respect Context

Ask knows:

```text
selected proposed change
source category
related model items
visible evidence
user role
current queue position
available actions
```

Short prompts like `Why now?` must resolve against the selected Proposed Change.

### 13.7 Ask Should Not Create Visual Clutter

If no Ask response is active, the Ask strip should remain compact.

If a response is active, expand only that response area. Do not push the entire card into chaos.

---

## 14. Action Bar

### 14.1 Placement

Actions live at the bottom of the expanded card.

They may be sticky within the card when content is long.

### 14.2 Actions

Default:

```text
Accept change
Delegate
Review evidence
Report correction
```

### 14.3 Visual Hierarchy

Primary:

```text
Accept change
```

- deep forest fill
- clear icon
- strongest visual weight

Secondary:

```text
Delegate
Review evidence
```

Tertiary / correction:

```text
Report correction
```

- warm outline
- not scary
- not hidden

### 14.4 Do Not Include Collapse as a Peer Action

Collapse belongs in top utility row.

Do not put `Collapse` beside `Accept change`.

---

## 15. Collapse and Expand Behavior

### 15.1 Expand Card

On compact card click:

1. Card expands in place.
2. Page scrolls card into view.
3. Previously expanded card collapses.
4. New card shows Focused Review content.
5. Other cards remain visible above/below in compact form.

### 15.2 Collapse Review

On `Collapse review`:

1. Expanded card collapses to compact form.
2. Page remains at same scroll region if possible.
3. No route change.
4. No modal close animation.

### 15.3 Previous / Next Navigation

Optional but recommended:

- arrow controls near `Reviewing X of Y`
- keyboard shortcuts

```text
J / ArrowDown → next item
K / ArrowUp   → previous item
Enter         → expand selected
Esc           → collapse review
A             → accept
D             → delegate
E             → review evidence
R             → report correction
```

---

## 16. Evidence Drawer

### 16.1 Trigger

`Review evidence` opens an in-page drawer or side sheet.

Do not navigate away.

### 16.2 Drawer Contents

```text
Evidence for this proposed change

Source                  Type        Trust      Time        Supports
Support ticket #482      Ticket      Strong     2h ago      Customer request
CRM note                 CRM         Strong     1d ago      ICP scoring demand
Planning note            Doc         Medium     3d ago      Scope mismatch
Model history            Change      Strong     11d ago     Watch classification
```

### 16.3 Evidence Drawer Behavior

- Opens over the right side or as a bottom drawer.
- Focus trapped for accessibility.
- ESC closes drawer.
- Underlying expanded card remains visible.

---

## 17. Delegate Flow

### 17.1 Trigger

Click `Delegate`.

### 17.2 Delegate Sheet Fields

```text
Delegate to
Due date
Message / context
Notify now
```

### 17.3 Delegate Preview

Before committing:

```text
Delegating this will:
- Notify selected owner
- Move this item to Monitoring
- Schedule follow-up in 48h
- Record the delegation in Ledger
```

### 17.4 Completion State

After delegate:

```text
Delegated to [Owner].
Fyralis will monitor for confirmation and resurface if unresolved.
```

The expanded card should briefly show this success state, then collapse or move to Monitoring.

---

## 18. Report Correction Flow

### 18.1 Trigger

Click `Report correction`.

### 18.2 Correction Options

```text
This is wrong
Missing context
Wrong owner
Already handled
Not important
Other
```

### 18.3 Correction Input

```text
Add context...
```

### 18.4 Completion State

```text
Correction submitted.
Fyralis will re-evaluate this proposed change and related model items.
```

If correction materially affects the model, create a Ledger event.

---

## 19. Accept Change Flow

### 19.1 Trigger

Click `Accept change`.

### 19.2 Pre-Accept Requirements

If any required fields are missing, show a lightweight confirmation step.

Examples:

```text
Owner is unassigned. Choose owner before accepting.
```

or:

```text
This change has weak evidence. Review evidence before accepting?
```

### 19.3 Applying State

Button state:

```text
Applying change...
```

Card may show subtle progress:

```text
Updating model
Notifying owners
Scheduling re-evaluation
```

### 19.4 Success State

After successful accept:

```text
Change accepted

Fyralis reclassified the item, notified owners, linked related model items,
and scheduled re-evaluation.

Moved to Monitoring
```

### 19.5 Post-Accept Behavior

Options:

- Collapse card after 1.5–3 seconds.
- Move it to Monitoring section.
- Keep a small confirmation row in the stream.

Do not simply delete the card instantly.

---

## 20. Data Contract

### 20.1 ProposedChange Object

```ts
export type ProposedChange = {
  id: string;
  queueIndex: number;
  queueTotal: number;

  state: "needs_authority" | "delegatable" | "monitoring" | "contested" | "accepted" | "dismissed";

  title: string;              // human-readable action title
  subtitle?: string;          // consequence / scope statement
  sourceCategory: string;     // e.g. "Customers & Revenue"
  relatedCategories: string[];

  proposedBy: "Fyralis" | string;
  createdAt: string;
  updatedAt?: string;

  confidence: {
    value: number;            // 0-1
    label: "low" | "moderate" | "high";
    explanation?: string;
  };

  current: ChangeField[];
  proposed: ChangeField[];

  whyThisMatters: string;

  evidence: {
    summary: string;
    signalCount: number;
    rows: EvidenceSummaryRow[];
  };

  missingContext: string[];

  impactIfAccepted: ImpactItem[];

  ask: {
    suggestedPrompts: string[];
    scope: "this_change";
  };

  actions: {
    accept: boolean;
    delegate: boolean;
    reviewEvidence: boolean;
    reportCorrection: boolean;
  };
};

export type ChangeField = {
  label: string;
  currentValue?: string;
  proposedValue?: string;
  changed: boolean;
  severity?: "neutral" | "watch" | "critical" | "positive";
};

export type EvidenceSummaryRow = {
  label: string;
  count?: number;
  strength: "strong" | "medium" | "partial" | "weak" | "missing";
};

export type ImpactItem = {
  label: string;
  type: "model_update" | "notification" | "link" | "reevaluation" | "workflow";
};
```

---

## 21. Component Map

Recommended React component structure:

```text
TodayPage
  Sidebar
  TodayHeader
  BriefingSummary
  TodayStream
    ProposedChangeCompactCard
    ProposedChangeReviewCard
      ReviewUtilityRow
      ReviewHeader
      ChangeDiff
      ReviewBody
        WhyThisMatters
        EvidenceQuality
        MissingContext
        ImpactIfAccepted
      AskFyralisStrip
      ReviewActionBar
    HandledSummary
  EvidenceDrawer
  DelegateSheet
  CorrectionSheet
  AskResponseCard
```

---

## 22. Responsive Behavior

### 22.1 Desktop

- Sidebar fixed.
- Main content scrolls.
- Expanded card max width 1120–1180 px.
- Diff block uses two-column comparison.
- Review body may use 2–3 columns if space allows, but maintain top-to-bottom reading order.

### 22.2 Tablet

- Sidebar may collapse.
- Expanded card width fills content area.
- Diff stacks if width below 900 px.
- Ask strip remains inline.

### 22.3 Mobile

- Sidebar hidden behind menu.
- Expanded review becomes full-width vertical sheet in page flow.
- Action bar may become sticky bottom.
- Compact cards become single-column.

---

## 23. Accessibility

### 23.1 Keyboard

- Cards must be focusable.
- `Enter` expands focused card.
- `Esc` collapses expanded card or closes active drawer.
- Buttons have visible focus states.

### 23.2 Screen Readers

Expanded card should announce:

```text
Reviewing proposed change 4 of 7: Re-scope the Conversation-AI commitment. Needs your authority.
```

### 23.3 Color Independence

Do not rely on color alone.

Use:

- labels
- icons
- text states
- ARIA labels

### 23.4 Motion Preferences

Respect `prefers-reduced-motion`.

If enabled:

- skip expansion animation
- use simple state transition

---

## 24. Animation and Motion

### 24.1 Expansion

```text
Duration: 280–420 ms
Easing: ease-out / cubic-bezier(0.2, 0.8, 0.2, 1)
```

Animation sequence:

1. Compact card grows vertically.
2. Surrounding cards reposition.
3. Header content fades in.
4. Diff block fades/slides in.
5. Review body appears.
6. Ask/action bar appears last.

### 24.2 Collapse

```text
Duration: 220–320 ms
```

Keep collapse calmer than expansion.

---

## 25. QA Checklist

### Visual

- [ ] Expanded card feels like a focused review sheet, not an accordion.
- [ ] Current vs Proposed block is not a generic table.
- [ ] Title is rewritten into human proposal language.
- [ ] Why This Matters explains consequence, not just title.
- [ ] Ask Fyralis appears integrated, not bolted on.
- [ ] Other cards remain visible above/below while scrolling.
- [ ] Bottom action bar is visually intentional.
- [ ] Sidebar and main content feel like the same brand world.

### Functional

- [ ] Clicking card expands in page.
- [ ] Clicking another card switches expansion.
- [ ] Collapse returns to compact card.
- [ ] Accept shows success state before moving item.
- [ ] Delegate opens sheet and previews consequences.
- [ ] Review evidence opens drawer.
- [ ] Report correction opens correction flow.
- [ ] Ask Fyralis answers in context.

### Trust

- [ ] No unexplained `0 signals` with high confidence.
- [ ] Missing context is always visible.
- [ ] Evidence strength is shown in plain language.
- [ ] Actions preview operational impact.

---

## 26. Implementation Priorities

### Phase 1: Visual Restructure

- Build expanded review card shell.
- Replace table diff with comparison panels.
- Rewrite title/subtitle display hierarchy.
- Stack review body vertically or semi-vertically.
- Move collapse out of action bar.

### Phase 2: Interaction

- In-page card expansion/collapse.
- Scroll-to-expanded behavior.
- Compact card switching.
- Action bar states.

### Phase 3: Trust and Ask

- Evidence quality display.
- Missing context section.
- Ask Fyralis strip and suggested prompts.
- Inline typed Ask responses.

### Phase 4: Action Flows

- Accept flow.
- Delegate flow.
- Evidence drawer.
- Correction flow.
- Monitoring/handled transition.

---

## 27. Final Product Rule

The expanded Proposed Change should feel like this:

```text
Fyralis built a case.
The user reviews the case.
The user asks questions if needed.
The user accepts, delegates, or corrects.
The model changes.
Fyralis keeps watching.
```

It should not feel like:

```text
A SaaS card expanded into a beige form.
```

That is the standard to build against.
