# AI Customer Support Ticket Automation

An end-to-end, zero-cost support-ticket automation system. Incoming customer emails are ingested from a Gmail inbox, triaged by a large language model, validated against strict business rules, stored as structured tickets in Airtable, and acknowledged automatically — with a full audit trail, SLA-breach escalation, and centralized error logging.

Built on **n8n** (self-hosted, local) with **Groq** as the primary LLM provider and **Mistral** as an automatic fallback.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Technology Stack & Rationale](#2-technology-stack--rationale)
3. [The Workflows in Depth](#3-the-workflows-in-depth)
4. [AI Design: Prompting & Classification](#4-ai-design-prompting--classification)
5. [Algorithms & Key Mechanisms](#5-algorithms--key-mechanisms)
6. [Data Model (Airtable)](#6-data-model-airtable)
7. [Error Handling & Resilience](#7-error-handling--resilience)
8. [Setup & Installation](#8-setup--installation)
9. [Configuration Reference](#9-configuration-reference)
10. [Security Notes](#10-security-notes)
11. [Repository Layout](#11-repository-layout)
12. [Assumptions & Limitations](#12-assumptions--limitations)

---

## 1. Architecture Overview

```
┌─────────────┐    ┌──────────────┐   ┌──────────────┐    ┌──────────────────┐
│ Gmail Inbox │──▶│ Gmail Trigger│──▶│ Extract Meta │──▶│ AI Analysis      │
│ (support@)  │    │ (poll 1 min) │   │ + Attachments│    │ Groq ─▶ Mistral │
└─────────────┘    └──────────────┘   └──────────────┘    └────────┬─────────┘
                                                                 │
      ┌──────────────────────────────────────────────────────────┘
      ▼
┌──────────────┐    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Validate &   │──▶│ Create Ticket│──▶│ Upload       │   │ Acknowledge  │
│ Normalize    │    │ (Airtable)   │──▶│ Attachments  │   │ Customer     │
└──────────────┘    └──────────────┘   └──────────────┘   └──────┬───────┘
                                                                │
                                                     ┌──────────▼───────┐
                                                     │ Mark "Processed" │
                                                     │ (Gmail label)    │
                                                     └──────────────────┘

┌───────────────────────  WF2: Lifecycle & Audit  ─────────────────────────┐
│ Airtable "Last Updated" trigger ─▶ Snapshot diff ─▶ Append Audit Log    │
│                                  └▶ Status→Resolved: resolution email    │
└──────────────────────────────────────────────────────────────────────────┘

┌───────────────────────  WF3: SLA Monitor  ──────────────────────────────┐
│ Cron (15 min) ─▶ Query open tickets past SLA ─▶ Escalate priority      │
└─────────────────────────────────────────────────────────────────────────┘

┌───────────────────────  WF-ERR: Error Handler  ─────────────────────────┐
│ n8n Error Trigger ─▶ Flatten error payload ─▶ Log to Airtable "Errors" │
└─────────────────────────────────────────────────────────────────────────┘
```

Four independent workflows share one Airtable base:

| Workflow | Purpose | Trigger |
|---|---|---|
| **WF1 — Ticket Intake** | Ingest → AI triage → validate → create ticket → acknowledge | Gmail Trigger, 1-minute poll |
| **WF2 — Lifecycle & Audit** | Detect agent edits, write audit trail, send resolution emails | Airtable Trigger on `Last Updated` |
| **WF3 — SLA Monitor** | Auto-escalate tickets that breach first-response SLA | Schedule Trigger, every 15 min |
| **WF-ERR — Error Handler** | Central capture of any workflow failure → Airtable `Errors` table | n8n Error Trigger |

Support agents work **directly in Airtable** (grid view or Interface) — no custom agent UI is needed. WF2 converts their edits into an immutable audit trail automatically.

---

## 2. Technology Stack & Rationale

Design constraint: **$0 running cost, no Docker, no paid tiers, no local model serving.**

| Concern | Choice | Why |
|---|---|---|
| Orchestration | **n8n** self-hosted via npm (`n8n start`, UI at `localhost:5678`) | Free forever self-hosted; visual + code nodes; workflows export as portable JSON; built-in retry, error-branch, and error-workflow primitives |
| Primary LLM | **Groq** free tier — `llama-3.3-70b-versatile` via OpenAI-compatible REST | $0; extremely fast inference; supports `response_format: json_object` for schema-reliable output; free-tier limits (~30 req/min) far exceed support-inbox volume |
| Fallback LLM | **Mistral** free tier — `mistral-small-latest` | Same OpenAI-compatible request shape, so the fallback is a one-line model swap wired to Groq's error branch; a Groq rate-limit or outage never blocks intake |
| Ticket database | **Airtable** free tier | Attachment fields (1 GB/base), single-selects enforce enums at the storage layer, `Last modified time` field powers change detection, Interfaces give agents a free review UI |
| Email in/out | **Gmail** + OAuth2 (Google Cloud project, Testing mode) | Trigger node downloads attachments natively; send node replies in-thread; labels provide a second idempotency layer |
| Setup automation | **Python 3** (stdlib only — `urllib`, `json`, `re`) | `setup_airtable.py` creates the base/tables/fields via the Airtable Meta API and imports workflows via the n8n REST API; no pip installs needed |

**Why polling instead of push?** The Gmail Trigger's 1-minute poll is reliable, resumable (n8n persists the last-seen message id), and needs zero public infrastructure. At higher volume, swap in Gmail push notifications via Cloud Pub/Sub — every downstream node stays identical.

**Why not a local GPU model?** Local serving (Ollama/vLLM) adds a background server and demo fragility for no benefit at this volume — Groq's hosted free tier is both faster and simpler.

---

## 3. The Workflows in Depth

### WF1 — Support Ticket Intake (11 nodes)

1. **Gmail Trigger** — polls the support inbox every minute for messages in `INBOX` without the `processed` label. Attachment download enabled, so file binaries flow through the pipeline.
2. **Extract Metadata** (Code) — parses the sender's display name and address from the `From` header (regex with RFC-822 fallback), extracts subject and plaintext body (falling back to tag-stripped HTML), captures `messageId`, `threadId`, and ISO-normalized `receivedAt`. The body is truncated to 8,000 characters for the LLM while the full body is preserved for storage.
3. **Idempotency Check** (Airtable search) — queries `{Message ID} = '<gmail id>'`. If a ticket already exists, the run short-circuits to a No-Op node. This makes the pipeline safe against re-polls, n8n restarts, and manual re-executions.
4. **Build AI Request** (Code) — assembles the full chat-completion payload: the system prompt (classification schema + business rules, see §4), the user prompt with the email fields, `temperature: 0`, and `response_format: {type: "json_object"}`.
5. **AI Analysis — Groq** (HTTP Request) — POST to `https://api.groq.com/openai/v1/chat/completions`. Retries 3× with 2-second backoff; on final failure the **error branch** routes to:
6. **AI Fallback — Mistral** (HTTP Request) — identical payload with the model swapped to `mistral-small-latest`. Both success and failure paths continue to validation, so even a double-provider outage still produces a (flagged) ticket.
7. **Validate & Normalize** (Code) — the defensive core of the system; see §5.1.
8. **Create Ticket** (Airtable create) — writes all ~24 fields with `typecast: true` so select options are created on the fly. Retries 3×.
9. **Prep + Upload Attachments** (Code + HTTP Request) — converts each binary attachment to base64 and POSTs it to Airtable's content endpoint (`/uploadAttachment`), skipping files over Airtable's 5 MB free-tier cap.
10. **Send Acknowledgment** (Gmail send) — plain-text confirmation with the ticket ID, AI summary, and SLA-derived response estimate. **Skipped** when the sender address failed validation or the ticket was tagged `possible-spam` (never auto-reply to spam).
11. **Mark Processed** (Gmail add-label) — applies the `Processed` label; combined with the idempotency check this gives two independent layers of duplicate protection.

### WF2 — Ticket Lifecycle & Audit Trail (6 nodes)

1. **Airtable Trigger** — polls the `Last Updated` (last-modified-time) field on the Tickets table every minute; only changed records flow through.
2. **Diff Against Snapshot** (Code) — a stateful field-level differ; see §5.2.
3. **Append Audit Log** (Airtable create) — one row per changed field: ticket link, field name, old value, new value, changed-by.
4. **Status → Resolved?** (IF) — checks whether this change set the status to `Resolved` and a valid customer email exists.
5. **Send Resolution Email** (Gmail send) — notifies the customer their ticket is resolved, with a 7-day reply-to-reopen window.
6. **No-Op** — clean terminal for non-resolution changes.

### WF3 — SLA Monitor & Escalation (4 nodes)

1. **Schedule Trigger** — every 15 minutes.
2. **Find SLA-Breached Tickets** (Airtable search) — formula: `AND({Status}='Open', {Escalated}=0, DATETIME_DIFF(NOW(), {Received At}, 'hours') >= {SLA Hours})`.
3. **Compute Escalation** (Code) — bumps priority one level (`Low → Medium → High → Critical`, capped), computes ticket age, and appends a timestamped `[SLA]` note.
4. **Escalate Ticket** (Airtable update) — writes the new priority, sets `Escalated` (preventing repeat escalation), and updates internal notes.

### WF-ERR — Central Error Handler (3 nodes)

1. **Error Trigger** — fires automatically whenever WF1/WF2/WF3 fails (wired via each workflow's `errorWorkflow` setting).
2. **Format Error** (Code) — flattens n8n's error payload: workflow name, failing node, message, execution URL, and a JSON snapshot capped at 90 KB (Airtable long-text safety limit).
3. **Log to Errors Table** (Airtable create) — persistent, queryable error log with a `Resolved` checkbox for ops triage.

---

## 4. AI Design: Prompting & Classification

The LLM is used for **one job only**: converting an unstructured email into a strict JSON object. Everything decision-critical is enforced *outside* the model.

**Output schema** (all keys required; enforced by `response_format: json_object` + post-validation):

```json
{
  "customer_name":        "string | null",
  "company":              "string | null",
  "issue_summary":        "string (≤ 2 sentences)",
  "detailed_description": "string",
  "category":             "Technical Support | Billing | Sales Inquiry | Feature Request | Bug Report | Account Access | Refund Request | General Inquiry",
  "priority":             "Critical | High | Medium | Low",
  "priority_justification": "string (1 sentence citing evidence)",
  "sentiment":            "Positive | Neutral | Negative",
  "product":              "string | null",
  "suggested_department": "Technical Support | Finance | Sales | Customer Success | Product Team",
  "suggested_tags":       "string[] (2–5 lowercase kebab-case)",
  "confidence_score":     "number 0.0–1.0"
}
```

**Prompt-engineering techniques applied:**

- **Temperature 0** + JSON response format → deterministic, parseable output.
- **Disambiguation rules in the system prompt** — e.g. *Bug Report* (broken feature) vs. *Technical Support* (help using a working feature); *Refund Request* only on an explicit money-back ask.
- **Tiered priority rubric** — concrete trigger phrases per level (outage/data-loss ⇒ Critical; blocked workflow/cancellation threat ⇒ High; etc.), instructing the model to apply the *highest* matching tier.
- **Calibrated self-reporting** — the model is told to lower `confidence_score` for ambiguous, multi-issue, or translated emails; scores below 0.6 flag the ticket for human review.
- **Spam handling in-schema** — spam is not rejected but classified (`General Inquiry` / `Low` / tag `possible-spam`), which downstream logic uses to suppress the auto-acknowledgment.

---

## 5. Algorithms & Key Mechanisms

### 5.1 Validation & normalization (WF1 — the "never throw" layer)

The AI output is treated as untrusted input. The validator applies, in order:

1. **Resilient JSON extraction** — accepts an already-parsed object; strips markdown code fences; falls back to a regex scan for the outermost `{...}` block. An unparseable response yields an empty object plus `Needs Review = true` — the ticket is still created.
2. **Fuzzy enum normalization** — case-insensitive exact match, then bidirectional substring match against the allowed value list; unknown values map to a safe fallback (`General Inquiry`, `Medium`, `Neutral`) and flag review.
3. **Deterministic priority overrides** — regex business rules that the AI can never downgrade:
   - `outage | data loss | security breach | hacked | cannot process payments` ⇒ force **Critical**
   - `urgent | asap | immediately | escalate | cancel my account` ⇒ raise Medium/Low to **High**
   Every override is appended to the priority justification for transparency.
4. **Tag hygiene** — coerce string→array, lowercase, kebab-case, dedupe via `Set`, length-cap 40 chars, max 5 tags.
5. **Email validation** — regex check on the sender; an invalid address suppresses the acknowledgment step rather than failing the run.
6. **Confidence gating** — non-finite or out-of-range scores default to 0.5; anything below 0.6 sets `Needs Review`.
7. **Derived routing** — a static category→team map (`Bug Report → Technical Support`, `Refund Request → Finance`, …) and a priority→SLA-hours map (`Critical: 2h, High: 4h, Medium: 24h, Low: 48h`) are computed here, keeping routing logic in one place.
8. **Ticket ID generation** — `TKT-<base36 timestamp>-<4-char random>`: sortable by creation time, collision-safe at this volume, human-readable.

### 5.2 Change detection via snapshot diffing (WF2)

Airtable's trigger reports *that* a record changed, not *what* changed. WF2 reconstructs field-level diffs:

- A snapshot map `{recordId → {field: value}}` for the five watched fields (`Status`, `Priority`, `Category`, `Assigned Team`, `Internal Notes`) is persisted in n8n's **workflow static data** (survives restarts; active-mode only).
- On each trigger, current values are compared to the snapshot via `JSON.stringify` equality; each difference emits one audit item `{field, oldValue, newValue}`.
- First sighting of a record seeds the snapshot silently — no log spam on initial sync.
- Illegal status values (outside the allowed lifecycle) are detected and flagged in the same pass.

### 5.3 Idempotency & duplicate suppression (WF1)

Two independent layers:

1. **Database check** — Airtable lookup on the Gmail `Message ID` before any processing; existing ticket ⇒ skip.
2. **Source marking** — the `Processed` Gmail label, combined with the trigger's `-label:processed` search filter, keeps already-handled mail out of the candidate set entirely.

Either layer alone survives the failure of the other (e.g., a crash between ticket creation and labeling is caught by layer 1 on the next poll).

### 5.4 SLA escalation (WF3)

Escalation is **edge-triggered and monotonic**: the `Escalated` checkbox guarantees a ticket is escalated at most once, and the priority ladder is capped at Critical. The breach condition is evaluated *inside Airtable's formula engine* (server-side) so the workflow only ever pulls genuinely breached rows.

---

## 6. Data Model (Airtable)

Base: **Support Tickets** — three tables.

**`Tickets`** (24 fields) — the core entity. Highlights: `Category`/`Priority`/`Sentiment`/`Status`/`Assigned Team` are single-selects (storage-level enum enforcement); `Message ID` powers idempotency; `Received At` + `SLA Hours` + `Escalated` power SLA monitoring; `Last Updated` (last-modified-time) powers WF2's trigger; `Attachments` stores customer files; `Needs Review` + `Confidence Score` power the human-in-the-loop queue.

**`Audit Log`** — append-only: linked ticket, field changed, old/new value, changed-by, auto `Changed At`.

**`Errors`** — workflow, node, error message, JSON payload snapshot, auto timestamp, `Resolved` checkbox.

Status lifecycle: `Open → In Progress → Waiting for Customer → Resolved → Closed`.

---

## 7. Error Handling & Resilience

| Failure | Handling |
|---|---|
| Groq down / rate-limited | 3 retries w/ backoff → automatic Mistral fallback → same validation path |
| Both LLMs down | Validator applies safe defaults (`General Inquiry` / `Medium`), sets `Needs Review`; ticket is never lost |
| Malformed AI output | Fence-stripping + regex JSON recovery → defaults + review flag on total failure |
| Airtable write failure | 3 retries w/ 2s backoff; terminal failure → WF-ERR logs it |
| Ack email failure | `onError: continueRegularOutput` — the ticket still completes; failure logged |
| Duplicate delivery / re-poll / restart | Message-ID idempotency check + `Processed` label |
| Any unhandled node error | Workflow-level `errorWorkflow` routes to WF-ERR → Airtable `Errors` row with full payload snapshot |
| Attachment > 5 MB | Skipped gracefully (Airtable free-tier limit) |

---

## 8. Setup & Installation

Prerequisites: Node.js ≥ 18, Python 3, a Gmail account, an Airtable account, free API keys from Groq and Mistral.

```bash
# 1. Install and start n8n (no Docker)
npm install -g n8n
n8n start          # UI at http://localhost:5678

# 2. Configure secrets
#    Copy/edit .env — every required value is documented inline in the file.

# 3. Google Cloud (one-time)
#    - Create a project; enable the Gmail API
#    - OAuth consent screen: External + Testing; add the support inbox as a Test user
#    - Create an OAuth Web client; add redirect URI:
#      http://localhost:5678/rest/oauth2-credential/callback

# 4. Provision Airtable + import workflows
python setup_airtable.py
#    Creates the base/tables/fields via the Meta API, patches ids into
#    workflows/*.json, and imports them into n8n via the REST API.
#    Manual step: add a "Last Updated" field (type: Last modified time)
#    to the Tickets table — the Meta API cannot create this field type.

# 5. Credentials in n8n (Credentials → Add)
#    - Gmail OAuth2 (client id/secret from .env → Sign in with Google)
#    - Airtable Token API (the PAT)
#    - Header Auth ×3: Groq, Mistral, Airtable-Bearer (Authorization: Bearer <key>)
#    Attach them to the nodes flagged in each imported workflow.

# 6. Wire the error handler
#    WF1/WF2/WF3 → Settings → Error Workflow → "WF-ERR - Central Error Handler"

# 7. Test manually (Execute Workflow on WF1), then activate WF1, WF2, WF3.
```

---

## 9. Configuration Reference

All secrets live in `.env` (git-ignored — never commit it):

| Variable | Purpose |
|---|---|
| `GROQ_API_KEY` / `GROQ_BASE_URL` / `GROQ_MODEL` | Primary LLM (Groq, `llama-3.3-70b-versatile`) |
| `MISTRAL_API_KEY` / `MISTRAL_BASE_URL` / `MISTRAL_MODEL` | Fallback LLM (`mistral-small-latest`) |
| `AIRTABLE_PAT` | Personal Access Token — scopes: `data.records:read/write`, `schema.bases:read/write` |
| `AIRTABLE_WORKSPACE_ID` / `AIRTABLE_BASE_ID` | Target workspace (for base creation) / created base |
| `AIRTABLE_*_TABLE_ID` | Tickets, Audit Log, Errors table ids |
| `SUPPORT_EMAIL` | The monitored Gmail inbox |
| `GMAIL_OAUTH_CLIENT_ID` / `GMAIL_OAUTH_CLIENT_SECRET` | Google Cloud OAuth web client |
| `GMAIL_PROCESSED_LABEL_ID` | Gmail label id for the `Processed` label |
| `N8N_URL` / `N8N_API_KEY` | Local n8n instance + API key (enables automated import) |

---

## 10. Security Notes

- `.env` is git-ignored; n8n credentials are stored encrypted in n8n's own credential vault — workflow JSON files contain only credential *references* (ids), never secrets.
- The Google OAuth app runs in **Testing** mode: only explicitly allow-listed test users can authorize it.
- The Airtable PAT is scoped to the single workspace/base it needs.
- Customer email content is sent to Groq/Mistral for classification — both providers' free tiers should be assumed non-private; for production use, upgrade to a provider tier with a data-processing agreement, or swap in a self-hosted model (the OpenAI-compatible node makes this a URL change).
- Attachments are stored in Airtable, not forwarded to any LLM.

---

## 11. Repository Layout

```
├── README.md                        ← this file
├── DEMO.md                          ← guided demo walkthrough
├── TICKET-AUTOMATION-WORKING.md     ← full design/working document
├── .env                             ← secrets (git-ignored)
├── .gitignore
├── setup_airtable.py                ← one-shot provisioning + import script
└── workflows/
    ├── workflow-1-intake.json       ← WF1: Ticket Intake
    ├── workflow-2-lifecycle.json    ← WF2: Lifecycle & Audit Trail
    ├── workflow-3-sla.json          ← WF3: SLA Monitor & Escalation
    └── workflow-err-handler.json    ← WF-ERR: Central Error Handler
```

---

## 12. Assumptions & Limitations

- **Volume**: designed for a small support inbox (free-tier LLM limits ≈ 1k requests/day; 1-minute polling). Scale path: Gmail push via Pub/Sub + paid LLM tier — no workflow redesign needed.
- **Agents work in Airtable** — no custom agent frontend; Airtable Interfaces cover review/triage for free.
- **Slack notifications** were part of the original design and have been removed from this deployment (the design doc retains them as an optional add-on).
- **Attachments** over 5 MB are skipped (Airtable free-tier per-file cap).
- **Thread replies**: a customer reply on an existing thread carries a new message id and will create a new ticket unless duplicate-detection-by-thread is enabled; `Thread ID` is stored to support that extension.
- **WF2 snapshot state** persists only while the workflow is *active* (n8n static-data semantics); manual test runs seed the snapshot but do not diff.
