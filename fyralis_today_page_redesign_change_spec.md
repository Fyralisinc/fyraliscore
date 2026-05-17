# Fyralis Today Page Redesign Specification

**Scope:** Changes required to move the current Today page expanded-card implementation into the ideal Today page experience we designed.

**Primary focus:**
- The proposed-change expanded view.
- In-page focused expansion, not a new route or modal.
- Ask Fyralis as a native product primitive, not a sidebar utility.
- Structural changes required from the screenshot implementation.

**Audience:** Frontend engineer, product designer, design systems owner, UX engineer.

---

## 0. Executive Summary

The current implementation shows a proposed change as a large inline expanded card inside a queue. It is technically legible but emotionally flat, visually bureaucratic, and not delivering Fyralis' core value: delegated intelligence.

The Today page should not feel like an approval queue or admin form. It should feel like an executive re-entry surface:

> Fyralis reviewed the company, absorbed the noise, and brought forward only the few things that need human judgment.

The ideal design is an **in-page focused expansion**:

- The user remains on the Today page.
- The selected proposed change expands in place and takes over most of the viewport.
- Other proposed changes remain above/below in compact form so the user keeps flow and context.
- The expanded card becomes a focused review case: what changes, why it matters, what evidence supports it, what may be missing, what happens if accepted, and what the user can do.
- Ask Fyralis is integrated directly inside the card and across the page as a contextual reasoning layer.

This page should answer:

> What needs my judgment right now?

It should not answer:

- What is the current state of the company? That is Model.
- What is forming or likely? That is Forecasts.
- What happened and resolved? That is Ledger.

---

## 1. Core Product Principle

### 1.1 Today is an authority surface, not a dashboard

Today should not optimize for showing many things. It should optimize for showing the few things that survived Fyralis' filtering.

The emotional message:

> You do not need to read everything. Fyralis already did. These are the few things that need you.

### 1.2 Expanded cards are review cases, not data records

A proposed change should not look like a database record with fields. It should look like a case for judgment.

Bad mental model:

```text
Expanded queue card
```

Correct mental model:

```text
Focused review case inside the Today stream
```

### 1.3 The user must stay in page flow

Do **not** open a new route or full-screen modal when a proposed change is clicked.

Correct behavior:

```text
Queue -> selected card expands in place -> other cards remain compact above/below
```

This allows fast processing across multiple judgment items while still giving the selected item full attention.

---

## 2. Diagnosis of Current Screenshot

The screenshot shows an expanded proposed change inside a section titled `Other judgment items`. The card contains:

- A title.
- A status chip.
- A repeated `Why this matters` block.
- A table-like `Current -> Proposed` block.
- Evidence quality showing `0 signals`.
- A `What may be missing` area.
- Related model context chips.
- An `If you accept` system-action list.
- Actions: Accept change, Delegate, Report correction, Collapse.

### 2.1 Structural issues

#### Problem: Expanded state is still visually part of the list

The selected item expands as an accordion within a queue. This makes it feel like a record in a list, not an important judgment.

Required change:

- The selected item should expand into a **Focused Review Card** that takes over most of the page viewport.
- Adjacent cards should remain visible in compact form above/below.
- The user should still be able to scroll through the Today page.

#### Problem: No strong focal hierarchy

The expanded card has no single visual anchor. The title, status, repeated explanation, table, evidence, context, and actions all compete.

Required change:

- The expanded card should guide the eye vertically:
  1. Proposed change identity.
  2. Current -> Proposed diff.
  3. Why it matters.
  4. Evidence quality.
  5. What may be missing.
  6. Impact if accepted.
  7. Actions.

#### Problem: The card is too horizontal

The existing card spreads information across wide regions and boxy horizontal sections. It is acting like a dashboard panel.

Required change:

- Keep the **Current -> Proposed** comparison horizontal because it is naturally comparative.
- Stack all other review sections vertically or in a narrow two-column responsive stack.
- Use the vertical space of the expanded card to tell a clear judgment story.

#### Problem: The card repeats instead of explains

`Why this matters` repeats the title instead of explaining consequence.

Required change:

- `Why this matters` must explain the operational consequence.
- If the same string appears in the title and why section, the implementation should be considered wrong.

#### Problem: `Evidence quality · 0 signals` breaks trust

A card showing `78% confidence` and `0 signals` creates a contradiction.

Required change:

- Never show `0 signals` beside a high-confidence proposed change without explanation.
- If there are zero **new** signals, label it as such:

```text
0 new signals since last confirmation
Evidence from 4 existing model items
```

- If there is truly insufficient evidence, the primary action should not be `Accept change`; it should be `Request evidence` or `Ask Fyralis to substantiate`.

#### Problem: `If you accept` is system-centric

The existing list includes actions like `Record ledger event for audit trail`, which is implementation detail.

Required change:

- Show operational consequences first.
- System logging may happen silently.

Example:

```text
+ Assign decision owner
+ Notify Product and GTM owners
+ Link 2 blocked commitments
+ Schedule re-evaluation in 48h
```

#### Problem: `Collapse` is treated as a peer action

`Collapse` appears alongside Accept/Delegate/Correction. It is navigation, not a decision.

Required change:

- Move collapse/close review into the card header as `Collapse review` or `Return to queue`.
- Do not place it in the action bar.

#### Problem: Ask Fyralis is absent from the decision flow

The current implementation does not use Ask Fyralis in the expanded card. This wastes one of Fyralis' strongest primitives.

Required change:

- Add a contextual Ask strip inside the expanded card.
- Ask should answer questions about the selected proposed change in place.
- Ask responses should be typed product responses, not generic chat.

---

## 3. Target Page Architecture

The Today page has two primary states:

1. **Briefing / Default State**
2. **Focused Review State**

Additional nested states exist inside Focused Review:

- Ask answer visible.
- Evidence drawer open.
- Delegate sheet open.
- Correction sheet open.
- Accepting / Accepted confirmation.
- Error / retry.

---

## 4. Global Layout

### 4.1 Sidebar

Keep the existing Fyralis sidebar pattern:

- Dark forest / deep green shell.
- Fyralis logo.
- Primary nav:
  - Today
  - Model
  - Forecasts
  - Ledger
- Utilities:
  - Ask Fyralis
  - Sources
  - Settings
- Model live card.
- User profile.

### 4.2 Main content background

Use a warm luminous surface:

```text
Background: warm cream / Moon Paper
Cards: Porcelain Mist / soft white
Borders: subtle Stone Veil
```

Avoid making Today look like Model's relational map. Today should feel like a review surface, not a visual system map.

---

## 5. Default Today State

### 5.1 Purpose

The default state should communicate:

> Fyralis reviewed the company since your last session. Most signals were absorbed. Only a few need judgment.

### 5.2 Header copy

Recommended header:

```text
Today
Fyralis reviewed the company since your last session.
98 signals processed · 94 absorbed · 4 need your judgment.
```

Optional secondary line:

```text
You're up to date. 8:42 AM, May 15
```

### 5.3 Briefing strip

The summary should be compact and narrative-led, not a heavy dashboard.

Metrics may be shown in a slim row:

```text
98 Signals processed
94 Absorbed
12 Model updates
4 Need judgment
$2.04M Exposed
```

Design rules:

- Do not use oversized dashboard cards.
- Use icons sparingly.
- The primary emphasis should be the sentence, not the metrics.

### 5.4 Primary Judgment Preview

The most important proposed change should be partially expanded by default.

This gives the page soul immediately. The user should not need to click to understand Fyralis' value.

The preview should include:

- `PRIMARY JUDGMENT` label.
- Proposed change title.
- Current -> Proposed summary line.
- Key impact chips.
- Short Why this matters.
- Evidence quality summary.
- Primary action row.

The primary judgment preview can be less detailed than full Focused Review but more detailed than a compact row.

### 5.5 Other Changes

Other items appear as compact cards/rows.

Each compact item should show:

- Icon / category / status rail.
- Title.
- Short transition summary.
- 2-4 impact chips.
- Status chip.
- Chevron.

Example:

```text
Assign owner for pricing model decision
Unowned -> CFO · Decision due in 5 business days
$720K opportunity · 2 commitments blocked · 9 signals · 66% confidence
Needs review
```

Do not show evidence icons as the primary trust mechanism. Use concise text:

```text
Evidence medium
```

or:

```text
9 signals
```

### 5.6 Handled Without You module

Show what Fyralis handled to create relief.

Example:

```text
Handled without you
94 signals absorbed
12 model updates applied
3 items placed under monitoring
0 contested changes
```

This module can appear below Other Changes or as a side block in wider layouts.

Important: This section is not vanity. It communicates the value of attention protection.

---

## 6. Focused Review State

### 6.1 Trigger

Triggered when:

- User clicks a compact proposed change card.
- User expands the primary judgment.
- User navigates by keyboard to a proposed change and presses Enter.

### 6.2 URL behavior

Do not route to a new page.

Optional: update URL hash/query for deep linking without losing page flow:

```text
/today?review=delta_123
```

But the UI should still feel like the same Today page.

### 6.3 Layout behavior

When a card is selected:

- The selected card expands in place.
- It takes up roughly 70-90% of the viewport height, depending on content.
- The page scrolls so the expanded card sits comfortably below the Today header.
- Previous/next cards remain compact above and below.
- Other cards should visually recede but remain available.

The selected card should feel like the only thing that matters right now, while still preserving scroll continuity.

### 6.4 Expanded card dimensions

Recommended desktop dimensions:

```text
Width: fills content column, max 1180-1280px
Min-height: 70vh
Max-height: none; allow natural vertical expansion
Internal padding: 28-40px
Border radius: 18-24px
Border: subtle, color-tinted by status
```

For very large screens, avoid stretching content too wide. Keep internal content readable.

Recommended inner content max width:

```text
Main readable column: 760-920px
Diff panel can sit alongside on desktop
```

---

## 7. Focused Review Card Anatomy

The expanded card is a case for judgment. It should contain the following sections in order.

### 7.1 Card header controls

Top row:

```text
Reviewing 1 of 4        Collapse review
```

Optional next/previous controls:

```text
← previous    next →
```

Do not use `Back to Today` if it implies navigation to a different route. Prefer:

```text
Collapse review
Return to queue
```

### 7.2 Proposed change identity

Display:

```text
PROPOSED CHANGE
Needs your authority

Escalate customer risk for Salesforce sync instability
```

Metadata:

```text
From Risks & Constraints · Proposed by Fyralis · Created 21m ago
```

Design rules:

- The title should be action-oriented.
- Avoid incident-style titles that merely describe a problem.
- The label should be small; the title should dominate.

Bad:

```text
Founder-CTO sync overdue 6 weeks — data-warehouse pricing decision unowned
```

Better:

```text
Assign owner and escalate the data-warehouse pricing decision
```

### 7.3 Key impact chips

Show 3-4 chips maximum.

Example:

```text
$2.04M ARR
3 customers
12 signals
78% confidence
```

Rules:

- No more than 4 chips.
- Avoid mixing too many icon styles.
- Do not overuse colored chips.

### 7.4 Current -> Proposed diff

This is the most important section after the title.

It can be horizontal because it is comparative.

Example layout:

```text
Current                         Proposed
Risk level: Watch       ->      Risk level: Critical
Owner: Unassigned       ->      Owner: VP Engineering
Re-evaluate: 7 days     ->      Re-evaluate: 48 hours
```

For decision-owner examples:

```text
Current                         Proposed
State: Watch             ->     State: Critical
Owner: Unassigned        ->     Owner: CFO / CTO
Review: overdue 6 weeks  ->     Schedule founder-CTO sync
Re-evaluate: 7 days      ->     Re-evaluate: 48h
```

Rules:

- Never use ambiguous field names like `CURRENT` as a row label.
- Field labels must be meaningful.
- The diff must show exactly what the user is authorizing.
- If the only proposed change is a status escalation, ask whether the product has enough value. Prefer concrete ownership, evaluation, notification, or model mutation.

### 7.5 Why this matters

Should explain consequence, not repeat title.

Example:

```text
Three anchor customers are reporting recurring Salesforce sync failures. Renewal exposure is increasing as confidence in sync reliability declines.
```

For pricing decision example:

```text
The pricing decision is now blocking two downstream commitments and delaying roadmap finalization. Without a confirmed owner, the decision is unlikely to resolve before the next GTM planning checkpoint.
```

Rules:

- 1-3 short paragraphs maximum.
- Prefer concrete consequences.
- Do not repeat the title.

### 7.6 Evidence quality

Show compact evidence strength.

Example:

```text
Evidence quality       12 signals
Support tickets        Strong
CRM logs               Strong
Email & threads        Partial
Review all evidence ->
```

Rules:

- If `signals_count` is 0, show why.
- Never combine high confidence with unexplained zero evidence.
- Evidence detail opens in drawer, not inline.

Zero-signal rule:

```text
If signals_count = 0 and confidence >= 0.7:
  Must show evidence_basis_text.
  Must not show "0 signals" alone.
```

Examples:

```text
No new signals since last confirmation.
Based on 4 existing model items and one declared commitment.
```

or:

```text
Evidence insufficient. Ask Fyralis to substantiate before accepting.
```

### 7.7 What may be missing

Keep this visible in expanded view.

Example:

```text
What may be missing
- No recent Beacon call transcript
- Account owner has not confirmed severity
```

Rules:

- If no context gaps are identified, say:

```text
No major context gaps identified.
```

- Do not hide this section. It is trust-building.
- Add `Ask Fyralis: What could be missing?` as a suggestion.

### 7.8 Ask Fyralis contextual strip

This is mandatory in Focused Review.

Position options:

- After `What may be missing`, before `Impact if accepted`.
- Or as a slim strip directly above the action bar.

Recommended UI:

```text
Ask Fyralis about this change...
[Why now?] [What if I wait?] [Who should own this?] [What evidence is weakest?]
```

Expanded Ask input:

```text
Ask Fyralis about this proposed change...
```

Rules:

- Ask uses the selected proposed change as context automatically.
- Short prompts should be interpreted relative to the selected change.
- Do not open a separate chat page.
- Answer inline inside the expanded card.

See Section 13 for full Ask behavior.

### 7.9 Impact if accepted

Show concrete consequences.

Example:

```text
Impact if accepted
+ Create escalation in Risks & Constraints
+ Notify VP Engineering and account owners
+ Link 3 renewal commitments
+ Schedule re-evaluation in 48h
```

Rules:

- Operational consequences first.
- System logging / audit trail can be implicit or shown in a secondary line.
- Do not lead with database/audit actions.

Bad:

```text
Record ledger event for audit trail
```

Better:

```text
Fyralis will record this in Ledger automatically.
```

### 7.10 Related model context

Show meaningful links, not category pills only.

Bad:

```text
Customers & Revenue
Decisions
```

Better:

```text
Related model context
Risks & Constraints: Salesforce sync instability
Customers & Revenue: 3 anchor renewals exposed
Commitments: 3 renewal commitments linked
View in Model ->
```

Rules:

- Context links should explain why they matter.
- Clicking opens Model in the relevant category/relationship focus state.

### 7.11 Action bar

Bottom of expanded card.

Recommended actions:

```text
Accept change
Delegate
Review evidence
Report correction
```

Rules:

- Action bar should be sticky to the bottom of the card or viewport while reviewing.
- Primary action should be visually strongest.
- `Collapse review` should not appear in action bar.
- `Review evidence` may open evidence drawer.
- `Report correction` opens correction sheet.

Primary button color:

- Use deep forest for most actions.
- Use red/coral only if the action itself is risky/destructive.
- Risk color should belong to the risk, not necessarily the accept button.

---

## 8. Expanded Card Layout: Vertical vs Horizontal

The expanded card should not be overly horizontal.

### 8.1 Horizontal sections allowed

Only these should be horizontal on desktop:

- Identity + diff in top region.
- Current vs Proposed comparison.
- Optional evidence/missing/impact columns only if each column remains readable.

### 8.2 Vertical sequence required

Primary review flow should be vertical:

```text
Proposed Change
Current -> Proposed
Why this matters
Evidence quality
What may be missing
Ask Fyralis
Impact if accepted
Actions
```

This ensures users can read the card like a judgment brief.

### 8.3 Responsive behavior

At widths below 1100px:

- Stack all sections vertically.
- Current -> Proposed diff becomes two stacked cards or a vertical diff list.
- Action buttons may wrap into two rows.

---

## 9. Compact Card States

### 9.1 Compact row anatomy

Compact proposed change card:

```text
[status rail/icon] Title                         Status chip
Transition summary
Impact chips
Source / related context
```

Example:

```text
Assign owner for pricing model decision          Needs review
Unowned -> CFO · Decision due in 5 business days
$720K opportunity · 2 commitments blocked · 9 signals · 66% confidence
From Decisions · Related: Commitments, Finance & Capital
```

### 9.2 Visual rules

- Height: 88-140px depending on content.
- Do not include full evidence lists.
- Do not include action buttons.
- Do not include `Why this matters`.
- Use a chevron or expand icon.

### 9.3 When one card is expanded

Other cards remain compact.

If a compact card is immediately above/below the expanded card, it should remain visible enough to maintain flow.

---

## 10. Scrolling and In-Page Focus

### 10.1 Card expansion behavior

On click:

1. Expand selected card in place.
2. Smooth-scroll card to a comfortable viewport position.
3. Do not navigate away.
4. Collapse any previously expanded card.
5. Keep other cards in compact form.

Recommended animation:

```text
Duration: 240-360ms
Easing: ease-out
```

Avoid bounce or dramatic motion.

### 10.2 Scroll behavior

When expanded:

- User can scroll up to previous compact cards.
- User can scroll down to next compact cards.
- The Today page remains one continuous document.

### 10.3 Sticky action bar

If expanded content is tall, action bar should stick within the bottom of the viewport while the card is active.

Rules:

- Sticky only inside expanded card boundaries.
- Do not overlap content.
- On mobile, use bottom sticky action bar.

---

## 11. State Machine

### 11.1 Page-level states

```text
TODAY_DEFAULT
FOCUSED_REVIEW
EMPTY_QUEUE
LOADING
ERROR
```

### 11.2 Focused review sub-states

```text
REVIEW_IDLE
ASK_OPEN
ASK_ANSWER_VISIBLE
EVIDENCE_DRAWER_OPEN
DELEGATE_SHEET_OPEN
CORRECTION_SHEET_OPEN
ACCEPTING
ACCEPTED_CONFIRMATION
ACTION_ERROR
```

### 11.3 State transitions

#### Default -> Focused Review

Trigger:

- Click compact card.
- Keyboard Enter on focused card.
- Direct URL with `review=id`.

Effects:

- Expand selected card.
- Scroll into view.
- Set `selected_delta_id`.

#### Focused Review -> Default

Trigger:

- Click Collapse review.
- Press Escape.
- Click same card header if currently expanded.

Effects:

- Collapse selected card.
- Preserve scroll position where possible.

#### Focused Review -> Different Focused Review

Trigger:

- Click another compact card.
- Keyboard next/previous.

Effects:

- Collapse current.
- Expand new card.
- Scroll new card into review position.

#### Review -> Accepting

Trigger:

- Click Accept change.

Effects:

- Disable action buttons.
- Show spinner or applying state.
- Submit accept mutation.

#### Accepting -> Accepted Confirmation

Trigger:

- Mutation success.

Effects:

- Show confirmation inside expanded card.
- Move item to Monitoring / Handled after delay or user action.
- Update header counts.
- Add Ledger event.

#### Accepting -> Action Error

Trigger:

- Mutation failure.

Effects:

- Show inline error.
- Re-enable actions.
- Offer retry.

---

## 12. Accepted Confirmation State

After accepting, do not instantly remove the card.

Show:

```text
Change accepted

Fyralis created the escalation, notified VP Engineering, linked 3 renewal commitments, and scheduled re-evaluation in 48h.

Moved to Monitoring
```

Actions:

```text
View in Model
View Ledger Event
Next change
```

After 2-4 seconds, the card may collapse into Monitoring if the user does not interact.

---

## 13. Ask Fyralis Primitive

Ask Fyralis is not a generic chat window. It is a context-aware command and reasoning layer over the company model.

### 13.1 Global Ask

Global top input or command palette:

```text
Ask Fyralis...
```

Context-aware placeholder on Today:

```text
Ask about today’s judgment items...
```

Shortcut:

```text
Cmd/Ctrl + K
```

### 13.2 Contextual Ask inside expanded proposed change

The Focused Review Card must include a contextual Ask primitive.

UI:

```text
Ask Fyralis about this change...
[Why now?] [What if I wait?] [Who should own this?] [What evidence is weakest?]
```

### 13.3 Context passed to Ask

Ask request payload should include:

```ts
type AskContext = {
  page: "today";
  selected_delta_id: string;
  selected_model_item_ids: string[];
  related_category_ids: string[];
  user_role: string;
  visible_state: "focused_review" | "default";
  time_window?: string;
};
```

### 13.4 Ask response types

Ask should return typed product responses, not just prose.

Supported response types:

```ts
type AskResponseType =
  | "explanation"
  | "evidence_summary"
  | "what_if_scenario"
  | "owner_recommendation"
  | "wait_analysis"
  | "model_context_link"
  | "action_preview"
  | "correction_prompt"
  | "unsupported_answer";
```

### 13.5 Required response structure

Every substantive Ask answer should include:

```ts
type AskAnswer = {
  type: AskResponseType;
  title: string;
  body: string;
  based_on?: string[];
  may_be_missing?: string[];
  actions?: AskAction[];
};
```

Example actions:

```ts
type AskAction = {
  label: string;
  action_type:
    | "accept_delta"
    | "delegate"
    | "open_model"
    | "open_evidence"
    | "create_delta_preview"
    | "add_context"
    | "schedule_review";
  payload?: Record<string, unknown>;
};
```

### 13.6 Example Ask prompts and behavior

#### Prompt: Why now?

Expected inline answer:

```text
Why now

This surfaced because the risk moved from Watch to Critical after three anchor customers showed recurring sync failures and the renewal window is now inside the next reporting cycle.

Based on:
- Support tickets
- CRM logs
- Renewal-thread email

May be missing:
- No recent Beacon call transcript
```

Actions:

```text
[Review evidence]
[Open in Model]
```

#### Prompt: What if I wait?

Expected answer:

```text
If you wait 7 days

Renewal risk remains elevated for Beacon and Northvale. The related commitment stays blocked unless ownership is assigned. Fyralis will re-evaluate if no new sync failures appear, but current evidence still supports escalation.
```

Actions:

```text
[Delegate owner]
[Accept escalation]
[Schedule reminder]
```

#### Prompt: Who should own this?

Expected answer:

```text
Recommended owner: VP Engineering

Why:
- Owns the CRM reliability commitment
- Controls Salesforce sync stabilization
- Already connected to 2 affected commitments

Suggested co-owner: Head of Support for customer communication.
```

Actions:

```text
[Delegate to VP Engineering]
[Add co-owner]
[Review ownership in Model]
```

#### Prompt: What evidence is weakest?

Expected answer:

```text
Weakest evidence

Email evidence is partial. Fyralis has support and CRM confirmation, but no recent Beacon call transcript and no direct account-owner confirmation.
```

Actions:

```text
[Ask account owner]
[Review all evidence]
[Report missing context]
```

### 13.7 Ask answer placement

Ask answer should appear inline inside the expanded card below the Ask strip.

It should not open a separate chat panel unless the user explicitly opens full Ask.

Answer card layout:

```text
Ask Fyralis
Question: What if I wait?

[Answer body]

Based on
...

May be missing
...

Actions
[...]
```

### 13.8 Ask should reduce UI clutter

Because Ask can answer deeper questions, do not expose every possible action as permanent buttons.

Permanent actions:

```text
Accept change
Delegate
Review evidence
Report correction
```

Contextual Ask handles:

```text
Why now?
What happens if I wait?
Who should own this?
What would make this wrong?
Show related model items.
```

### 13.9 Ask trust rules

Ask must not hallucinate certainty.

Every answer should be grounded in:

- selected proposed change
- related model items
- evidence sources
- available confidence and missing context

If insufficient:

```text
I can partially answer this.
```

or:

```text
I do not have enough connected evidence to answer fully.
```

Then offer next action:

```text
[Review evidence]
[Add context]
[Ask owner]
```

---

## 14. Evidence Drawer

Triggered by:

- Review evidence button.
- Ask response action.
- Evidence quality section link.

Drawer can be right-side or centered overlay, but must not navigate away.

Contents:

```text
Evidence for this proposed change
12 signals

Source           Evidence                             Trust / Strength
Support          Beacon reported recurring failures    Strong
CRM              Northvale renewal thread mentions... Strong
Email            Conduit report blocked               Partial
```

Each evidence item should show:

- source system
- timestamp
- author if available
- trust tier
- short summary
- linked model item

---

## 15. Delegate Sheet

Triggered by `Delegate`.

Fields:

```text
Delegate to
Suggested owner
Due date
Message / context
Notify now
```

Suggested owner should be prefilled when possible.

Example:

```text
Suggested: VP Engineering
Reason: owns CRM reliability commitment and controls Salesforce sync stabilization.
```

Actions:

```text
Delegate
Cancel
```

After success:

```text
Delegated to VP Engineering.
Fyralis will monitor for confirmation.
```

Move card to Monitoring / Delegated.

---

## 16. Correction Sheet

Triggered by `Report correction`.

Options:

```text
This is wrong
Missing context
Wrong owner
Already handled
Not important
Other
```

Text area:

```text
Add context for Fyralis...
```

If correcting owner:

```text
Correct owner
```

After submit:

```text
Correction recorded.
Fyralis will re-evaluate related model items.
```

State moves to contested or re-evaluating depending on severity.

---

## 17. Data Contracts

### 17.1 Proposed Change / Decision Delta

```ts
type DecisionDelta = {
  id: string;
  title: string;
  user_facing_label: "Proposed Change";
  status: "needs_authority" | "needs_review" | "delegatable" | "monitoring" | "contested" | "accepted" | "rejected";
  priority_rank: number;
  source_category: string;
  related_categories: string[];
  proposed_by: "Fyralis" | string;
  created_at: string;
  updated_at: string;

  summary: string;
  why_this_matters: string;

  current_state: DeltaField[];
  proposed_state: DeltaField[];

  impact_chips: ImpactChip[];
  evidence_summary: EvidenceSummary;
  missing_context: string[];
  impact_if_accepted: AcceptedImpact[];
  related_model_context: RelatedContext[];

  actions: DeltaAction[];
};

type DeltaField = {
  label: string;
  value: string;
  severity?: "neutral" | "watch" | "critical" | "positive";
};

type ImpactChip = {
  label: string;
  value: string;
  icon?: string;
  severity?: "neutral" | "low" | "medium" | "high";
};

type EvidenceSummary = {
  signals_count: number;
  basis_text?: string;
  items: {
    label: string;
    strength: "strong" | "medium" | "partial" | "weak" | "missing";
    source_types?: string[];
  }[];
};

type AcceptedImpact = {
  label: string;
  type: "model_update" | "notification" | "relationship_link" | "reevaluation" | "ledger" | "delegation";
};

type RelatedContext = {
  category: string;
  label: string;
  description?: string;
  model_url?: string;
};

type DeltaAction = {
  type: "accept" | "delegate" | "review_evidence" | "report_correction";
  label: string;
  enabled: boolean;
  reason_disabled?: string;
};
```

### 17.2 Today Page Summary

```ts
type TodaySummary = {
  signals_processed: number;
  signals_absorbed: number;
  model_updates_applied: number;
  need_judgment_count: number;
  requires_authority_count: number;
  delegatable_count: number;
  monitoring_count: number;
  contested_count: number;
  exposed_amount?: string;
  last_reviewed_at?: string;
};
```

---

## 18. Accessibility

### 18.1 Keyboard

Required shortcuts:

```text
Enter: expand/collapse selected card
Escape: collapse review or close sheet
J / ArrowDown: next proposed change
K / ArrowUp: previous proposed change
A: accept selected change, with confirmation if needed
D: delegate
E: review evidence
R: report correction
Cmd/Ctrl+K: Ask Fyralis
```

### 18.2 Screen reader

- Expanded card should announce:

```text
Reviewing proposed change 1 of 4: Escalate customer risk for Salesforce sync instability.
```

- Current -> Proposed diff should be readable as paired fields.
- Evidence bars need text labels.
- Color cannot be the only indicator of state.

### 18.3 Focus management

On expansion:

- Focus moves to the expanded card header.
- Escape returns focus to the compact card.
- After action success, focus moves to confirmation state.

---

## 19. Performance and Interaction Requirements

- Expansion animation should not exceed 400ms.
- Ask responses should stream if longer than 1 second.
- Evidence drawer can lazy-load evidence details.
- Accept action should optimistically show applying state but not remove item until confirmed.
- The page should remain responsive with up to 20 proposed changes.

---

## 20. Visual Design Rules

### 20.1 What to avoid

Do not:

- Show all cards expanded.
- Use right-side inspector as the main expanded experience.
- Open a full new route for details.
- Use modal for normal review.
- Overuse red in buttons.
- Show unexplained `0 signals`.
- Repeat title inside `Why this matters`.
- Put `Collapse` in the primary action bar.
- Use generic tips like `Use filters to focus...`.

### 20.2 What to prefer

Do:

- Lead with the re-entry story.
- Show handled work.
- Spotlight one judgment at a time.
- Use vertical review flow.
- Keep Current -> Proposed diff visually strong.
- Make Ask Fyralis contextual and inline.
- Keep other cards compact and nearby.
- Show closure after action.

---

## 21. Implementation Checklist

### Phase 1: Restructure page

- [ ] Add Today briefing header copy.
- [ ] Replace heavy metric cards with slim briefing strip.
- [ ] Add Primary Judgment preview in default state.
- [ ] Add Other Changes compact cards.
- [ ] Add Handled Without You module.

### Phase 2: In-page focused expansion

- [ ] Implement selected card expansion in place.
- [ ] Keep other cards compact above/below.
- [ ] Add smooth scroll to selected card.
- [ ] Add collapse review control.
- [ ] Add keyboard navigation.

### Phase 3: Expanded card redesign

- [ ] Replace current table with meaningful Current -> Proposed diff.
- [ ] Rewrite Why this matters section to show consequence.
- [ ] Add evidence quality summary with zero-evidence safeguards.
- [ ] Add What may be missing.
- [ ] Replace system-centric `If you accept` with operational impact.
- [ ] Move collapse out of action bar.

### Phase 4: Ask Fyralis integration

- [ ] Add contextual Ask strip inside expanded card.
- [ ] Add suggested prompts based on selected delta.
- [ ] Implement typed Ask responses.
- [ ] Render answers inline.
- [ ] Add action preview support.
- [ ] Add grounding and missing-context display.

### Phase 5: Action workflows

- [ ] Accept mutation.
- [ ] Accepted confirmation state.
- [ ] Delegate sheet.
- [ ] Evidence drawer.
- [ ] Correction sheet.
- [ ] Monitoring/handled transition.

### Phase 6: QA

- [ ] Test 0 evidence scenario.
- [ ] Test long title.
- [ ] Test no missing context.
- [ ] Test 1 card, 4 cards, 20 cards.
- [ ] Test keyboard navigation.
- [ ] Test mobile stack.
- [ ] Test Ask answer latency.
- [ ] Test accept failure.

---

## 22. Final Target Experience

A user lands on Today and sees:

```text
Fyralis reviewed the company since your last session.
98 signals processed. 94 absorbed. 4 need judgment.
```

They immediately see the primary judgment.

If they click it, the card expands in place into a focused review case. It takes most of the viewport, but the user remains inside Today. Other changes stay above/below in compact form.

Inside the expanded card, the user sees:

1. What Fyralis proposes.
2. What changes from current to proposed.
3. Why it matters.
4. How strong the evidence is.
5. What may be missing.
6. What happens if accepted.
7. How to ask Fyralis follow-up questions.
8. How to accept, delegate, review evidence, or correct.

The page should feel like delegated intelligence, not queue management.

