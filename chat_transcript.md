# ChatGPT - Branch · Git Notes Explained

**Source URL:** https://chatgpt.com/share/6975e6d3-8d70-8012-ad2e-21725ac49400

## User

Original custom instructions no longer available

---

## User

Explain git notes

---

## Assistant

Git notes are a way to attach extra information to a commit (or other Git object) **without changing the commit** itself.

Think of them as “sticky notes” that live **in a separate ref** (so they don’t rewrite history, don’t change commit hashes, and don’t affect merges unless you explicitly share/fetch them).

---

## What git notes are good for

- **Code review metadata** (e.g., “Reviewed-by”, links to discussions)
- **Build/test results** (“CI passed on runner X”)
- **Security / audit annotations**
- **Migration breadcrumbs** (“This commit corresponds to JIRA-123”)
- **Explaining weird commits** without amending/rebasing

---

## How they work (conceptually)

A commit is a Git object. Notes are stored separately under refs like:

- `refs/notes/commits` *(default)*

So Git can display notes *as if they were part of the commit*, but the commit object stays untouched.

---

## Core commands you’ll actually use

### Add a note to the current HEAD commit
```bash
git notes add -m "This fixes issue #123 after rollout incident."
```

### Add a note to a specific commit
```bash
git notes add -m "Reviewed-by: Thomas" <commit>
```

### Show notes for a commit
```bash
git notes show <commit>
```

### Edit a note
```bash
git notes edit <commit>
```

### Remove a note
```bash
git notes remove <commit>
```

---

## Viewing notes automatically

### Show notes in `git log`
```bash
git log --show-notes
```

Git can also be configured to show notes by default:
```bash
git config --global log.showNotes true
```

---

## Sharing notes with others (important!)

Notes are **not pushed by default**.

### Push notes
```bash
git push origin refs/notes/*:refs/notes/*
```

Or push only the default notes ref:
```bash
git push origin refs/notes/commits
```

### Fetch notes from remote
```bash
git fetch origin refs/notes/*:refs/notes/*
```

---

## Multiple note namespaces (power feature)

You can maintain separate “channels” of notes:

- `refs/notes/review`
- `refs/notes/ci`
- `refs/notes/audit`

Example:
```bash
git notes --ref=refs/notes/review add -m "LGTM" <commit>
git notes --ref=refs/notes/ci add -m "CI: passed 2026-01-24" <commit>
```

Show them:
```bash
git log --show-notes=refs/notes/review --show-notes=refs/notes/ci
```

---

## Merging notes (when pulling from others)

Notes can conflict too. Git supports a strategy:

```bash
git config --global notes.mergeStrategy cat_sort_uniq
```

That means “combine notes, sort and deduplicate lines” (nice for review tags and such).

---

## Limitations / gotchas

- Notes **don’t travel unless you push/fetch them**
- Some hosting tools (GitHub/GitLab) don’t surface notes in UI by default
- They’re awesome internally, but don’t rely on them as your only permanent record unless your workflow pushes them reliably

---

## Practical mini-workflow example (team-friendly)

Attach build metadata:
```bash
git notes add -m "CI: passed on linux-x64 (job #8891)"
git push origin refs/notes/commits
```

Someone else:
```bash
git fetch origin refs/notes/commits:refs/notes/commits
git log --show-notes -1
```

---

If you want, I can suggest a clean convention for your team (notes namespaces + push/fetch aliases) so it becomes painless and consistent.

---

## User

Yes please and add the concepts of feature roadmap documents that act as a feature request document that is saved with a special naming convention. This document is to be committed alone and will be used as a reference in git notes, where we have our task descriptions based a the conventional commits categories that will act as an internal issue tracking system.

---

## Assistant

Nice — this can be a *really* clean “Git-native issue tracker” that stays lightweight and doesn’t require external tooling.

Below is a **convention + workflow** that combines:

- **Feature Roadmap Documents** (the “canonical spec / request” committed as a single file)
- **Git notes** (task/decision/audit trail attached to commits)
- **Conventional Commits** categories (your “internal issue labels”)
- Optional: a couple of **aliases** so it’s ergonomic

---

# 1) The big picture (how it fits together)

### A) Feature Roadmap Document = the “source of truth”
You create **one** document per feature request / initiative.

- It’s committed **alone** (one commit containing only that file)
- It has a **stable ID** in its filename
- That commit becomes the **anchor** you reference everywhere else

### B) Git notes = the “internal issue tracker”
You attach notes to commits like:

- task description
- acceptance criteria / test approach
- links to the roadmap doc commit
- status updates
- decisions + rationale

Notes are stored under separate namespaces, so you can keep things clean:
- `refs/notes/roadmap` (links from commits → roadmap doc)
- `refs/notes/tasks` (task descriptions)
- `refs/notes/review` (review metadata)
- `refs/notes/ops` (rollout / incident / deployment notes)

### C) Conventional Commits = your “issue categories”
Your commit message type acts like labels:
- `feat:` new capability
- `fix:` bug fix
- `refactor:` internal change
- `docs:` documentation
- `test:` test-only
- `perf:` performance
- `build:` build system
- `ci:` pipelines
- `chore:` cleanup
- `revert:` revert

The note content then holds the “issue text”.

---

# 2) Naming convention for Feature Roadmap Documents

Make a dedicated folder, so it’s discoverable and consistent:

```
roadmap/
```

### File naming format
Pick one canonical format; I suggest this because it’s grep-friendly and stable:

```
roadmap/FR-YYYY-NNN--short-slug.md
```

Example:
```
roadmap/FR-2026-001--git-native-internal-tracker.md
```

**Rules:**
- `FR` = Feature Request
- `YYYY` = year the request was created
- `NNN` = running number
- `slug` = short readable title
- Markdown only (easy diffing, linking, and review)

### The “commit alone” rule
When you create a feature request doc:
- commit **only that file**
- no unrelated changes

That makes the commit hash a durable reference point.

---

# 3) Roadmap document template (minimal but powerful)

Put this inside `roadmap/FR-2026-001--…md`:

```md
# FR-2026-001 — Git-native internal tracker

## Summary
One paragraph describing the feature request.

## Motivation / Problem
What pain does this solve?

## Goals
- [ ] Goal 1
- [ ] Goal 2

## Non-goals
- Not doing X
- Not doing Y

## Scope
### In
- Item A
- Item B

### Out
- Item C

## Proposed approach
High-level design and constraints.

## Acceptance criteria
- [ ] Observable outcome 1
- [ ] Observable outcome 2

## Risks / Tradeoffs
- Risk 1
- Tradeoff 2

## Rollout / Migration plan
Step-by-step rollout.

## References
- Links (PRs, docs, discussions)
```

---

# 4) Git notes structure (your “issues”)

## Recommended note namespaces

### `refs/notes/roadmap`
A note that links a commit to a roadmap doc.

Example note content:
```
Roadmap: FR-2026-001
Roadmap-Commit: abc1234
```

### `refs/notes/tasks`
This is your internal “ticket” note.

Example note content (based on Conventional Commits category):
```
type: feat
scope: notes
status: in-progress
roadmap: FR-2026-001
summary: Add push/fetch notes aliases + namespaces
acceptance:
- `git log --show-notes=tasks` shows task note
- notes are pushed to origin by default alias
```

### `refs/notes/review`
Example:
```
Reviewed-by: Thomas
Review: Approved
Context: checked edge cases in fetch/push
```

### `refs/notes/ops`
Example:
```
Deployed: 2026-01-24
Env: production
Rollback: not needed
Notes: monitored latency; ok
```

---

# 5) The workflow (step-by-step)

## Step 1: Create the feature request document (single-file commit)
```bash
mkdir -p roadmap
$EDITOR roadmap/FR-2026-001--git-native-internal-tracker.md

git add roadmap/FR-2026-001--git-native-internal-tracker.md
git commit -m "docs(roadmap): add FR-2026-001 git-native internal tracker"
```

Get the commit:
```bash
DOC_COMMIT=$(git rev-parse HEAD)
echo $DOC_COMMIT
```

## Step 2: Reference that doc from implementation commits (via notes)
On an implementation commit, attach a roadmap reference note:

```bash
git notes --ref=roadmap add -m "Roadmap: FR-2026-001
Roadmap-Commit: $DOC_COMMIT" HEAD
```

## Step 3: Attach a “task note” to the same commit
```bash
git notes --ref=tasks add -m "type: feat
scope: notes
status: done
roadmap: FR-2026-001
summary: Add notes namespaces + sharing workflow" HEAD
```

Now your **commit** is still clean and conventional:
```bash
git commit -m "feat(notes): add notes namespaces and sharing aliases"
```

…but your “issue” lives in notes.

---

# 6) Make it ergonomic: Git config + aliases

### Always show notes in log (selected namespaces)
```bash
git config --global log.showNotes true
```

You can also do it per command with:
```bash
git log --show-notes=roadmap --show-notes=tasks
```

### Notes merge strategy (prevents pain)
```bash
git config --global notes.mergeStrategy cat_sort_uniq
```

### Aliases: add/show/push/fetch notes
```bash
git config --global alias.nadd '!f(){ ref="$1"; shift; git notes --ref="$ref" add -m "$*"; }; f'
git config --global alias.nshow '!f(){ ref="$1"; shift; git notes --ref="$ref" show "$@"; }; f'

git config --global alias.npush 'push origin refs/notes/*:refs/notes/*'
git config --global alias.nfetch 'fetch origin refs/notes/*:refs/notes/*'

git config --global alias.nlog 'log --show-notes=roadmap --show-notes=tasks --show-notes=review --show-notes=ops'
```

Usage:
```bash
git nadd tasks "type: fix
scope: parser
status: todo
roadmap: FR-2026-001
summary: handle malformed input"
```

Then:
```bash
git nlog -5
git npush
```

---

# 7) How conventional commits become your “tracking system”

### Commit message = category + scope (like labels)
Examples:
- `feat(api): add search endpoint`
- `fix(ui): avoid layout shift`
- `refactor(parser): simplify tokenizer`

### Git note = full “issue/ticket details”
That keeps commit messages short and consistent, but still gives you rich tracking info.

You can even standardize the **task note schema**:

**Task note schema**
- `type:` conventional type
- `scope:` conventional scope
- `status:` todo | in-progress | done | blocked
- `roadmap:` FR-YYYY-NNN
- `summary:`
- `acceptance:` list

---

# 8) Discoverability tricks (so it doesn’t get lost)

## Find roadmap docs quickly
```bash
ls roadmap/
```

## Find commits that reference a roadmap ID in notes
```bash
git log --show-notes=roadmap --grep="FR-2026-001"
```

## Show all tasks notes in recent history
```bash
git log --show-notes=tasks -20
```

*(Even better: your `git nlog` alias.)*

---

# 9) Team rules (lightweight but important)

**Hard rules**
1. Roadmap docs go in `roadmap/`
2. File name must match `FR-YYYY-NNN--slug.md`
3. Roadmap doc must be committed alone
4. Notes must be pushed (`git npush`) for shared visibility

**Soft rules**
- Every `feat:` commit should include a `roadmap` note entry
- Every significant change should include a `tasks` note
- Reviews add a `review` note instead of stuffing the commit message

---

If you want, I can also give you a **ready-to-drop `CONTRIBUTING.md` section** that documents this workflow (and a tiny script like `./bin/fr new "title"` to create and number FR docs automatically).

---

## User

The roadmap should be simpler. It is named based on what we use as scope in the commit message. It is a list of feature requests in several states. So it will be changed over time.

---

## Assistant

Got it — so instead of “one roadmap doc per feature”, you want **one simple roadmap file per scope**, and that file is a **living list** of feature requests in different states.

That’s a great fit for Git because it stays lightweight and keeps scopes coherent.

Here’s a clean convention that matches what you described.

---

# 1) Roadmap concept (simplified)

## Roadmap file = per *scope*
Scope is the same string you use in Conventional Commits:

- `feat(<scope>): ...`
- `fix(<scope>): ...`
- `refactor(<scope>): ...`

So the roadmap is named after that scope:

```
roadmap/<scope>.md
```

Examples:

```
roadmap/notes.md
roadmap/auth.md
roadmap/search.md
roadmap/ui.md
roadmap/build.md
```

Each file is just a **list of feature requests** for that scope in multiple states, updated over time.

✅ This means:
- fewer docs
- always easy to find “what’s next for scope X”
- scope becomes your natural “project lane”

---

# 2) Minimal roadmap format (state-based list)

Example `roadmap/notes.md`:

```md
# Roadmap: notes

## Now (in progress)
- [ ] FR-2026-001 Push/fetch notes namespaces as a team standard
- [ ] FR-2026-002 Add `git nlog` alias to show task/review/ops notes

## Next (planned)
- [ ] FR-2026-003 Define a task note schema (type/scope/status/summary)
- [ ] FR-2026-004 Add merge strategy config for notes across team

## Later (nice to have)
- [ ] FR-2026-005 CLI helper: create task note template from commit type/scope

## Done
- [x] FR-2026-000 Document the overall workflow
```

That’s it. No heavy template.

---

# 3) Feature request IDs (stable references)

Keep a simple stable ID inside each roadmap file:

**FR-YYYY-NNN**  
Example: `FR-2026-002`

Numbering can be:
- per-scope (recommended): `notes` has its own FR counter  
  → super easy, no coordination needed
- or global (one counter across repo) if you prefer

Either works.

---

# 4) How git notes reference roadmap items

You want notes to be your internal issue tracker. Perfect.

For any implementation commit (or even WIP commits), attach a **task note** that points to the roadmap scope + FR item:

### Task note example (`refs/notes/tasks`)
```
type: feat
scope: notes
fr: FR-2026-002
status: in-progress
summary: Add git nlog alias and document note namespaces
```

This is the key: **commit stays conventional + clean**, the “issue text” lives in the note.

Your commit message stays short:

```bash
git commit -m "feat(notes): add notes log alias"
```

---

# 5) Recommended states + minimal process

### States (keep it simple)
- **Now** → currently being worked on
- **Next** → planned soon
- **Later** → ideas / not committed
- **Done** → completed

### Process (super lightweight)
1. Add new item under **Next**
2. When work starts → move to **Now**
3. When merged → move to **Done**
4. Attach git notes on implementation commits referencing `fr: FR-...`

That’s enough to feel like issue tracking without becoming a system.

---

# 6) Suggested note namespaces (unchanged, still great)

- `refs/notes/tasks` → the “internal issues”
- `refs/notes/review` → review metadata
- `refs/notes/ops` → deployment / incidents
- *(optional)* `refs/notes/roadmap` → only if you want extra linking, but not required anymore

In this simpler model, you can often skip a dedicated `roadmap` note ref and just include:
- `scope: X`
- `fr: FR-...`

inside the `tasks` note.

---

# 7) Optional ergonomics: a micro schema that matches conventional commits

If you want the smallest structured note format possible, I’d standardize to exactly this:

```
type: feat|fix|refactor|docs|test|perf|ci|build|chore
scope: <scope>
fr: FR-YYYY-NNN
status: todo|in-progress|done|blocked
summary: one line
```

It stays human-readable *and* grep-able.

---

If you want, I can also give you:

- a **tiny `roadmap/README.md`** that explains the rules in 15 lines
- a couple of **git aliases** like `git task` that opens an editor prefilled with the schema and auto-targets `refs/notes/tasks` for `HEAD`

---

## User

Both sounds nice

---

## Assistant

Perfect — here are both:

1) a **tiny `roadmap/README.md`** you can drop into the repo  
2) a set of **git aliases** that make the workflow feel effortless

---

## 1) `roadmap/README.md` (copy/paste)

```md
# Roadmap

This repo uses a Git-native roadmap + issue tracking workflow.

## Roadmap files

Roadmaps live in `roadmap/<scope>.md`, where `<scope>` matches the Conventional Commit scope:

- `feat(<scope>): ...`
- `fix(<scope>): ...`
- `refactor(<scope>): ...`

Examples:
- `roadmap/auth.md`
- `roadmap/search.md`
- `roadmap/notes.md`

Each roadmap file is a living list of feature requests (FRs) in simple states.

## Feature request IDs

Feature requests use IDs like:

- `FR-YYYY-NNN`

Example:
- `FR-2026-003`

IDs are stable references used inside Git notes.

## States

Use these sections in every roadmap file:

- **Now (in progress)**
- **Next (planned)**
- **Later (nice to have)**
- **Done**

## Task tracking via Git notes

We track “issues/tasks” using Git notes (not commits, not an external tracker).

Task notes live under:

- `refs/notes/tasks`

Each task note should reference:
- `type:` Conventional Commit type (feat/fix/refactor/...)
- `scope:` scope name
- `fr:` feature request ID (FR-YYYY-NNN)
- `status:` todo | in-progress | done | blocked
- `summary:` one-liner

Example task note:

type: feat
scope: notes
fr: FR-2026-002
status: in-progress
summary: Add git aliases to push/fetch notes and show them in log

## Sharing notes

Notes are not pushed automatically. Always share them:

- `git npush` (push all notes refs)
- `git nfetch` (fetch all notes refs)

## Viewing notes

Use:

- `git nlog -20`

to show recent commits with task/review/ops notes.
```

---

## 2) Git aliases (make it ergonomic)

Paste these into your shell. They set up:

- `git task ...` to add a task note to `HEAD`
- `git tedit` to edit the task note on `HEAD`
- `git nlog` to view commits + notes
- `git npush` / `git nfetch` to share notes
- a sane merge strategy for notes

### Setup commands

```bash
# --- Notes merge strategy (prevents conflicts pain)
git config --global notes.mergeStrategy cat_sort_uniq

# --- Convenience: show all relevant note namespaces in a single log command
git config --global alias.nlog 'log --show-notes=refs/notes/tasks --show-notes=refs/notes/review --show-notes=refs/notes/ops'

# --- Push/fetch ALL notes refs (important: notes are not shared by default)
git config --global alias.npush 'push origin refs/notes/*:refs/notes/*'
git config --global alias.nfetch 'fetch origin refs/notes/*:refs/notes/*'

# --- Add/Edit/Show task notes on commits
git config --global alias.task '!f(){ git notes --ref=refs/notes/tasks add -m "$*" HEAD; }; f'
git config --global alias.tedit 'notes --ref=refs/notes/tasks edit'
git config --global alias.tshow 'notes --ref=refs/notes/tasks show'
git config --global alias.tshowc '!f(){ git notes --ref=refs/notes/tasks show "$1"; }; f'

# --- Review and ops notes (optional but handy)
git config --global alias.review '!f(){ git notes --ref=refs/notes/review add -m "$*" HEAD; }; f'
git config --global alias.ops '!f(){ git notes --ref=refs/notes/ops add -m "$*" HEAD; }; f'
```

---

## How it feels day-to-day (examples)

### Add a new item to a scope roadmap
Edit `roadmap/notes.md`:

```md
## Next (planned)
- [ ] FR-2026-004 Add standard schema for task notes
```

Commit the roadmap change (normal commit):
```bash
git add roadmap/notes.md
git commit -m "docs(roadmap): update notes FR list"
```

### Work on an item: implement + attach “issue text” via notes
Make your code changes, then:

```bash
git commit -m "feat(notes): add notes sharing aliases"

git task "type: feat
scope: notes
fr: FR-2026-004
status: in-progress
summary: Add push/fetch notes aliases and nlog shortcut"
```

Share notes:
```bash
git npush
```

### View recent work with notes included
```bash
git nlog -20
```

---

## Tiny recommended task-note format (copy this)
Use this exact block in `git task "..."`:

```
type: feat
scope: <scope>
fr: FR-YYYY-NNN
status: todo|in-progress|done|blocked
summary: <one line>
```

---

If you want one extra nicety: I can give you a `git taskp` alias that **pre-fills a template in your editor** (instead of stuffing multiline notes into a shell string). That’s the only “quality of life” upgrade I’d add after using this for a week.

---

## User

What else is out there providing the issue tracking via git native features

---

## Assistant

{"search_query":[{"q":"git native issue tracker stored in git repository notes refs git-bug","recency":30},{"q":"tools that store issues in git repository as commits or notes (e.g., git-issue git-journal)","recency":365},{"q":"git notes based code review tool git-appraise","recency":365},{"q":"Gerrit Code Review stores review metadata in git refs","recency":365}],"response_length":"medium"}

---

## Assistant

There’s a surprisingly rich ecosystem of “issue tracking *inside Git*” approaches. They fall into a few **distinct patterns**, each with different tradeoffs vs your **roadmap-per-scope + git-notes-tasks** idea.

## 1) Full issue tracker stored as Git objects (closest to “real issues in Git”)

### **git-bug**
A distributed, offline-first issue tracker where issues/comments are **stored as Git objects** in the repo and can be pushed/pulled like code. citeturn0search0  
**Why it’s interesting:** it’s basically “GitHub Issues, but Git-native and syncable”.

**Pros**
- Real issue entities (open/closed, comments, metadata)
- Works offline, distributed by design
- Doesn’t depend on a specific forge

**Cons**
- New tool + workflow to teach
- Integration into your “Conventional Commits as categories” isn’t automatic (but you can link)

### **git-issue**
A console-based issue tracker that also stores issues in the repo. citeturn0search28  
**Pros:** simple, terminal-first  
**Cons:** smaller ecosystem, less “standardized” than git-bug

✅ If you ever want “real issue objects” rather than notes-on-commits, **git-bug is the big one**.

---

## 2) “Issues as Git notes” (your approach, scaled up)

### **git notes (native Git feature)**
Notes attach metadata to commits without rewriting history — perfect for task tracking. citeturn0search5

### **git-appraise**
A distributed code review system where reviews are stored in the repo as Git objects, using **git notes as a datastore**. citeturn0search2turn0search22  
It’s “PR review comments inside Git”, not exactly issues — but the *storage model* is very relevant.

**Pros**
- Stays close to Git primitives
- Distributed review history is replicated like code

**Cons**
- Primarily review-focused, not a full “issue tracker UI”

✅ Your system (tasks in `refs/notes/tasks`) is basically the “minimal, customizable version” of this category.

---

## 3) Code review metadata stored in Git refs (industrial-strength)

### **Gerrit + NoteDB**
Gerrit stores review metadata in Git (NoteDB) *alongside the code*, including change status, votes, and comments. citeturn0search7turn0search11  

**Pros**
- Proven at massive scale
- Strong workflow and permissions model

**Cons**
- It’s a whole platform/server (not lightweight)
- More “review system” than “issue tracker”

✅ Gerrit proves the concept: **“important workflow metadata belongs in Git too.”**

---

## 4) “Issues as files in the repo” (ultra-simple, boring, effective)

This isn’t a specific tool — it’s a pattern:

- `roadmap/<scope>.md` (what you’re doing)
- or `issues/FR-2026-001.md`
- or `tickets/123-some-title.md`

Often paired with conventional commits and linking via commit footers.

**Pros**
- Dead simple; any editor works
- Easy review/diff/PR for roadmap changes

**Cons**
- No structured state machine unless you enforce it
- Can be noisy if every issue becomes a file (your per-scope roadmap avoids that)

✅ Your “living roadmap file per scope” is a *clean version* of this approach.

---

## 5) “Review/Discussion as commits” (clever but spicy)

TigerBeetle described a workflow where “code review” becomes an extra commit on top of the PR branch containing marked comments, resolved markers, etc. citeturn0search6  

**Pros**
- Everything is pure Git history
- No special storage refs required

**Cons**
- Requires discipline + sometimes force-push workflows
- Not as clean as notes for metadata

✅ This is more “discussion tracking” than issue tracking, but it’s another Git-native mindset.

---

# How these compare to *your* roadmap + notes tracker

You’re building a **minimal internal tracker** with:
- Roadmap = living lists (`roadmap/<scope>.md`)
- Tasks = git notes (`refs/notes/tasks`)
- Categories = Conventional Commits (`feat/fix/refactor/...`)

That sits nicely between:
- **“issues as files”** (simple but limited)
and
- **git-bug** (full-featured issues inside Git)

If you want to stay lightweight *and* level up later, the clean path is:

✅ **Today:** Roadmap-per-scope files + task notes  
➡️ **Later upgrade:** add a tiny CLI wrapper (still your format)  
➡️ **Full upgrade:** adopt **git-bug** if you ever need actual issue objects + queries + statuses citeturn0search0

If you want, I can propose a **1-command CLI** (shell/python) that:
- appends FR items into the right `roadmap/<scope>.md`
- auto-generates `FR-YYYY-NNN`
- adds a matching `refs/notes/tasks` note onto `HEAD` (or a chosen commit) in your schema

---

## User

We should organize the notes directory structure that would be scope, category, date/number scheme
Our branch names should use the same naming schema

---

## Assistant

Love it. If you align **git notes refs** and **branch names** under one naming scheme, your repo becomes *self-indexing*.

Below is a practical convention that works well with:

- **Conventional Commits** (`type(scope): …`)
- **Roadmap per scope** (`roadmap/<scope>.md`)
- **Git notes as internal issues**
- **Branch names that mirror everything**

---

# 1) Unified naming scheme

## Core fields
- **type** = conventional commit category (`feat|fix|refactor|docs|test|perf|ci|build|chore`)
- **scope** = your scope name (`auth|search|notes|ui|…`)
- **id** = date/number scheme (`YYYYMMDD-NNN`)
- **slug** = short description (`kebab-case`)

Example ID:
- `20260124-003`

---

# 2) Git notes “directory structure” (refs layout)

Git notes live under refs, and refs can be hierarchical ✅

### Recommended structure
```
refs/notes/<scope>/<type>
```

Examples:
```
refs/notes/notes/feat
refs/notes/notes/fix
refs/notes/auth/feat
refs/notes/search/refactor
refs/notes/ui/docs
```

### Why this is good
- You can **show only what you care about**:
  - `git log --show-notes=refs/notes/auth/feat`
- You can **push/fetch everything** easily:
  - `git push origin refs/notes/*:refs/notes/*`
- It naturally groups “issues” by the same lanes your team already uses.

---

# 3) What goes inside each note

A note is your “ticket body”, so keep it structured and grep-friendly.

### Minimal schema (recommended)
```
id: 20260124-003
status: todo|in-progress|done|blocked
roadmap: <scope>#<FR-ID>
summary: one-liner
details:
- optional bullet
- optional bullet
```

Example note content:
```
id: 20260124-003
status: in-progress
roadmap: notes#FR-2026-004
summary: Standardize notes refs and add aliases
details:
- refs/notes/<scope>/<type>
- add npush/nfetch/nlog aliases
```

> This keeps the “date/number scheme” *in the content*, while the “directory structure” lives in the ref name.

That combo is ergonomic **and** queryable.

---

# 4) Branch naming scheme (same structure)

### Recommended branch format
```
<type>/<scope>/<YYYYMMDD-NNN>-<slug>
```

Examples:
- `feat/notes/20260124-003-notes-ref-structure`
- `fix/auth/20260125-001-token-refresh-race`
- `refactor/search/20260126-002-indexing-simplification`
- `docs/ui/20260124-004-roadmap-format`

### Why this is good
- Immediately tells you **what** + **where** + **when**
- Sortable by date
- Easily ties to notes and commit messages

---

# 5) How roadmap items fit in

Since your roadmap is *per scope* and changes over time, tie tasks to it like:

`roadmap: <scope>#FR-2026-004`

Example entry inside `roadmap/notes.md`:
```md
## Now (in progress)
- [ ] FR-2026-004 Notes refs + branch naming convention alignment
```

Then your branch + notes + commit all line up:

- Roadmap: `notes#FR-2026-004`
- Branch: `feat/notes/20260124-003-notes-ref-structure`
- Commit: `feat(notes): standardize notes refs`
- Notes ref: `refs/notes/notes/feat`

---

# 6) Ergonomic aliases (optional but 🔥)

## Add a note under scope/type
This creates notes under `refs/notes/<scope>/<type>`:

```bash
git config --global alias.nadd '!f(){ scope="$1"; type="$2"; shift 2; git notes --ref="refs/notes/$scope/$type" add -m "$*" HEAD; }; f'
```

Usage:
```bash
git nadd notes feat "id: 20260124-003
status: in-progress
roadmap: notes#FR-2026-004
summary: Standardize notes refs + branch naming"
```

## Show notes for a scope/type
```bash
git config --global alias.nshow '!f(){ scope="$1"; type="$2"; shift 2; git notes --ref="refs/notes/$scope/$type" show "$@"; }; f'
```

Usage:
```bash
git nshow notes feat HEAD
```

## Push/fetch all notes
```bash
git config --global alias.npush 'push origin refs/notes/*:refs/notes/*'
git config --global alias.nfetch 'fetch origin refs/notes/*:refs/notes/*'
```

---

# 7) Quick “policy” you can enforce in the team

**Branch rule**
- Branch name must be: `<type>/<scope>/<YYYYMMDD-NNN>-<slug>`

**Commit rule**
- Must be Conventional Commits: `type(scope): summary`

**Notes rule**
- For every non-trivial commit, add a note in `refs/notes/<scope>/<type>`
- Notes must contain: `id`, `status`, `roadmap`, `summary`

---

If you want, I can also propose:
- a **single `git new` command** that creates the branch with the next `YYYYMMDD-NNN` number automatically, or
- a lightweight `pre-push` hook that warns if you forgot `git npush` (so notes don’t silently stay local).

---

## User

We no longer have need for the fears, ID status and road map in the schema

---

## Assistant

Cool — then we can make this *much* leaner.

We’ll keep:

- **directory structure** = `scope/type/…`
- **date/number scheme** = baked into **branch names** (and optionally the **note ref name**)
- **note content** = just the task description (no metadata headers)

---

## 1) New minimal note schema (no FR/id/status/roadmap)

### Note content = just the “ticket text”
Example:

```
Add aliases:
- npush / nfetch for sharing notes
- nlog to show notes refs by default

Acceptance:
- notes refs are pushed with one command
- log shows notes for scope+type
```

That’s it. No “fields”.

---

## 2) Notes “directory structure” (refs layout)

### Recommended layout
```
refs/notes/<scope>/<type>
```

Examples:
```
refs/notes/notes/feat
refs/notes/auth/fix
refs/notes/search/refactor
refs/notes/ui/docs
```

This already gives you your “scope + category” partitioning.

### Showing notes by lane
```bash
git log --show-notes=refs/notes/auth/fix -20
```

---

## 3) Where the date/number scheme lives now

Since you removed IDs from the schema, the cleanest place to put the date/number is:

✅ **Branch names** (primary)  
✅ (optional) **note ref name** (secondary)

### Branch naming (recommended)
```
<type>/<scope>/<YYYYMMDD-NNN>-<slug>
```

Example:
- `feat/notes/20260124-003-notes-ref-layout`

This becomes your “ticket id”, but it’s just the branch name.

---

## 4) Optional: put date/number in the *notes ref* too (if you really want it)

If you want notes to also be grouped by “ticket branch”, you can go one level deeper:

```
refs/notes/<scope>/<type>/<YYYYMMDD-NNN>
```

Example:
```
refs/notes/notes/feat/20260124-003
```

**Tradeoff**
- 👍 Very explicit: notes are tied to a ticket key
- 👎 More refs to manage, and `--show-notes` becomes more verbose

Most teams do **not** need this. I’d keep the simpler `refs/notes/<scope>/<type>` and rely on the branch name.

---

## 5) Minimal workflow (super clean)

### Start work
```bash
git switch -c feat/notes/20260124-003-notes-ref-layout
```

### Implement
```bash
git commit -m "feat(notes): reorganize git notes namespaces"
```

### Attach the “issue text” as a note
```bash
git notes --ref=refs/notes/notes/feat add -m "Reorganize notes namespaces

- use refs/notes/<scope>/<type>
- add aliases for sharing and viewing" HEAD
```

### Share notes
```bash
git push origin HEAD
git push origin refs/notes/*:refs/notes/*
```

---

## 6) Ergonomic aliases (updated for the minimal schema)

### Add note to `refs/notes/<scope>/<type>`
```bash
git config --global alias.nadd '!f(){ scope="$1"; type="$2"; shift 2; git notes --ref="refs/notes/$scope/$type" add -m "$*" HEAD; }; f'
```

Usage:
```bash
git nadd notes feat "Reorganize notes namespaces

- refs/notes/<scope>/<type>
- add sharing aliases"
```

### Edit/show notes for that lane
```bash
git config --global alias.nedit '!f(){ scope="$1"; type="$2"; git notes --ref="refs/notes/$scope/$type" edit HEAD; }; f'
git config --global alias.nshow '!f(){ scope="$1"; type="$2"; git notes --ref="refs/notes/$scope/$type" show HEAD; }; f'
```

### Share all notes
```bash
git config --global alias.npush 'push origin refs/notes/*:refs/notes/*'
git config --global alias.nfetch 'fetch origin refs/notes/*:refs/notes/*'
```

### Log with notes (you can show multiple lanes)
```bash
git config --global alias.nlog 'log --show-notes --show-notes=refs/notes/notes/feat --show-notes=refs/notes/notes/fix --show-notes=refs/notes/auth/feat --show-notes=refs/notes/auth/fix'
```

(Or keep it manual per scope/type so it doesn’t get noisy.)

---

## 7) Final “policy” (simple enough to remember)

**Branch name**
- `<type>/<scope>/<YYYYMMDD-NNN>-<slug>`

**Commit message**
- `type(scope): summary`

**Notes**
- live in `refs/notes/<scope>/<type>`
- content is freeform task text (no required headers)

**Sharing**
- `git npush` whenever you push code branches

---

If you want, I can also give you a **`git new` alias** that automatically creates today’s `YYYYMMDD-001` style branch name (and increments if it already exists locally).

---

## User

We want to use the same naming scheme for both the branches and the notes. They are scope type and feature request ID. We also want the automatic creation script for branches

---

## Assistant

Perfect — then we’ll make **branches and notes use the *exact same key***:

✅ **scope / type / feature-request-id**

…and we’ll add an **auto branch creator** that generates the next FR id and creates the branch for you.

---

# 1) Single naming scheme (branches + notes)

## Canonical key
```
<scope>/<type>/<FR-ID>
```

Examples:
```
notes/feat/FR-2026-004
auth/fix/FR-2026-012
search/refactor/FR-2026-003
```

### Branch name (same scheme, plus optional slug)
I recommend keeping the key **unchanged** and appending a readable slug:

```
<scope>/<type>/<FR-ID>--<slug>
```

Example:
```
notes/feat/FR-2026-004--notes-ref-layout
```

*(The part before `--` is the stable ID. The slug is just for humans.)*

---

# 2) Notes ref structure (same scheme)

Git notes refs can be hierarchical, so we mirror the exact same key under `refs/notes/`:

```
refs/notes/<scope>/<type>/<FR-ID>
```

Examples:
```
refs/notes/notes/feat/FR-2026-004
refs/notes/auth/fix/FR-2026-012
```

### Adding a note for the current commit
```bash
git notes --ref=refs/notes/notes/feat/FR-2026-004 add -m "Task description..." HEAD
```

### Showing it
```bash
git notes --ref=refs/notes/notes/feat/FR-2026-004 show HEAD
```

### Showing it in log
```bash
git log --show-notes=refs/notes/notes/feat/FR-2026-004 -10
```

---

# 3) The automatic branch creation script

This script:

- generates the next `FR-<year>-NNN` **per scope/type**
- creates a branch named:
  ```
  <scope>/<type>/<FR-ID>--<slug>
  ```
- prints the notes ref path:
  ```
  refs/notes/<scope>/<type>/<FR-ID>
  ```

### Install as a git alias: `git fr-new <scope> <type> <slug...>`

```bash
git config --global alias.fr-new '!f() {
  scope="$1"; type="$2"; shift 2;
  slug="$*";

  if [ -z "$scope" ] || [ -z "$type" ] || [ -z "$slug" ]; then
    echo "Usage: git fr-new <scope> <type> <slug...>";
    echo "Example: git fr-new notes feat notes ref layout";
    return 2;
  fi

  # slugify (lowercase, spaces -> -, strip non [a-z0-9-])
  slug="$(printf "%s" "$slug" | tr "[:upper:]" "[:lower:]" | sed -E "s/[^a-z0-9]+/-/g; s/^-+//; s/-+$//")"

  year="$(date +%Y)"

  # Find max NNN for existing local branches matching <scope>/<type>/FR-<year>-NNN*
  max="$(git for-each-ref --format="%(refname:short)" refs/heads \
    | grep -E "^${scope}/${type}/FR-${year}-[0-9]{3}" \
    | sed -E "s#^${scope}/${type}/FR-${year}-([0-9]{3}).*#\\1#" \
    | sort -n \
    | tail -n 1)"

  if [ -z "$max" ]; then
    next="001"
  else
    next="$(printf "%03d" $((10#$max + 1)))"
  fi

  fr_id="FR-${year}-${next}"
  branch="${scope}/${type}/${fr_id}--${slug}"

  git switch -c "$branch" || return $?

  echo "Created branch: $branch"
  echo "Matching notes ref: refs/notes/${scope}/${type}/${fr_id}"
}; f'
```

### Example usage
```bash
git fr-new notes feat "notes ref layout"
```

Creates:
- branch: `notes/feat/FR-2026-001--notes-ref-layout`
- notes ref: `refs/notes/notes/feat/FR-2026-001`

---

# 4) Convenience aliases for notes (matching the scheme)

### Add / edit / show notes for the “current FR”
You’ll pass the same triplet: `scope type FR-ID`.

```bash
git config --global alias.fr-note '!f(){ scope="$1"; type="$2"; fr="$3"; shift 3;
  git notes --ref="refs/notes/$scope/$type/$fr" add -m "$*" HEAD;
}; f'

git config --global alias.fr-edit '!f(){ scope="$1"; type="$2"; fr="$3";
  git notes --ref="refs/notes/$scope/$type/$fr" edit HEAD;
}; f'

git config --global alias.fr-show '!f(){ scope="$1"; type="$2"; fr="$3";
  git notes --ref="refs/notes/$scope/$type/$fr" show HEAD;
}; f'
```

Usage:
```bash
git fr-note notes feat FR-2026-004 "Implement notes ref layout + aliases"
```

---

# 5) Sharing notes (must-do)

Notes don’t travel unless pushed.

```bash
git config --global alias.npush 'push origin refs/notes/*:refs/notes/*'
git config --global alias.nfetch 'fetch origin refs/notes/*:refs/notes/*'
```

---

## Result: everything lines up cleanly

For a given work item:

- **Branch:** `notes/feat/FR-2026-004--notes-ref-layout`
- **Notes ref:** `refs/notes/notes/feat/FR-2026-004`
- **Commit:** `feat(notes): ...` *(still conventional)*

…and your “issue tracker” is Git-native and grep-friendly.

If you want, I can extend `git fr-new` to also **append an entry into `roadmap/<scope>.md`** automatically (under “Next” or “Now”).

---

## User

The notes IDs should also have the slug appended

---

## Assistant

Perfect — then we make the **notes ref name identical to the branch key**, including the slug.

## ✅ Canonical “work item key” (used everywhere)

```
<scope>/<type>/<FR-ID>--<slug>
```

Examples:
- `notes/feat/FR-2026-004--notes-ref-layout`
- `auth/fix/FR-2026-012--token-refresh-race`
- `search/refactor/FR-2026-003--index-simplification`

### Branch name
Exactly that string.

### Notes ref
Exactly that string under `refs/notes/…`:

```
refs/notes/<scope>/<type>/<FR-ID>--<slug>
```

Example:
- `refs/notes/notes/feat/FR-2026-004--notes-ref-layout`

---

# 1) Updated auto branch creator: `git fr-new`

Creates the branch **and prints the exact matching notes ref** (with slug appended).

```bash
git config --global alias.fr-new '!f() {
  scope="$1"; type="$2"; shift 2;
  slug="$*";

  if [ -z "$scope" ] || [ -z "$type" ] || [ -z "$slug" ]; then
    echo "Usage: git fr-new <scope> <type> <slug...>";
    echo "Example: git fr-new notes feat notes ref layout";
    return 2;
  fi

  # slugify (lowercase, spaces->-, strip non [a-z0-9-])
  slug="$(printf "%s" "$slug" | tr "[:upper:]" "[:lower:]" | sed -E "s/[^a-z0-9]+/-/g; s/^-+//; s/-+$//")"

  year="$(date +%Y)"

  # next FR number per scope/type/year from local branches
  max="$(git for-each-ref --format="%(refname:short)" refs/heads \
    | grep -E "^${scope}/${type}/FR-${year}-[0-9]{3}" \
    | sed -E "s#^${scope}/${type}/FR-${year}-([0-9]{3}).*#\\1#" \
    | sort -n \
    | tail -n 1)"

  if [ -z "$max" ]; then
    next="001"
  else
    next="$(printf "%03d" $((10#$max + 1)))"
  fi

  fr_id="FR-${year}-${next}"
  key="${scope}/${type}/${fr_id}--${slug}"

  git switch -c "$key" || return $?

  echo "Created branch: $key"
  echo "Matching notes ref: refs/notes/$key"
}; f'
```

### Example
```bash
git fr-new notes feat "notes ref layout"
```

Creates:
- branch: `notes/feat/FR-2026-001--notes-ref-layout`
- notes ref: `refs/notes/notes/feat/FR-2026-001--notes-ref-layout`

---

# 2) Notes helpers that use the same key

## A) Add a note (you pass the key once)
```bash
git config --global alias.fr-note '!f(){ scope="$1"; type="$2"; fr="$3"; shift 3;
  git notes --ref="refs/notes/$scope/$type/$fr" add -m "$*" HEAD;
}; f'
```

Usage:
```bash
git fr-note notes feat "FR-2026-001--notes-ref-layout" "Implement ref layout + aliases"
```

✅ Notice: `fr` includes the slug now (`FR-…--slug`).

---

## B) Even nicer: automatically use the current branch as key

This is the one you’ll actually love day-to-day.

### Add note for current branch
```bash
git config --global alias.fr-note-here '!f(){ 
  key="$(git branch --show-current)";
  if [ -z "$key" ]; then echo "Not on a branch."; return 2; fi
  git notes --ref="refs/notes/$key" add -m "$*" HEAD;
}; f'
```

Usage:
```bash
git fr-note-here "Task: implement notes refs + sharing aliases"
```

### Edit note for current branch
```bash
git config --global alias.fr-edit-here '!f(){
  key="$(git branch --show-current)";
  if [ -z "$key" ]; then echo "Not on a branch."; return 2; fi
  git notes --ref="refs/notes/$key" edit HEAD;
}; f'
```

### Show note for current branch
```bash
git config --global alias.fr-show-here '!f(){
  key="$(git branch --show-current)";
  if [ -z "$key" ]; then echo "Not on a branch."; return 2; fi
  git notes --ref="refs/notes/$key" show HEAD;
}; f'
```

---

# 3) Sharing notes (still required)

```bash
git config --global alias.npush 'push origin refs/notes/*:refs/notes/*'
git config --global alias.nfetch 'fetch origin refs/notes/*:refs/notes/*'
```

---

# ✅ Final result (everything matches perfectly)

For a work item:

- **Branch:** `notes/feat/FR-2026-001--notes-ref-layout`
- **Notes ref:** `refs/notes/notes/feat/FR-2026-001--notes-ref-layout`
- **Commit message:** `feat(notes): reorganize notes namespaces`

…and you can add the task note with:

```bash
git fr-note-here "…"
```

If you want next: I can also give you a `git fr-log-here` that runs `git log --show-notes=refs/notes/<current-branch>` automatically.

---

## User

The git hooks would be useful

---

## Assistant

Absolutely — hooks are the glue that makes this feel “native” and *automatic* instead of “remember to do X”.

Below is a **small, practical hook set** that matches your scheme:

✅ **Branch name == notes ref name**  
`<scope>/<type>/FR-YYYY-NNN--<slug>`

✅ Notes live at:  
`refs/notes/<scope>/<type>/FR-YYYY-NNN--<slug>`

✅ Conventional commits still apply:  
`type(scope): summary`

---

# Recommended hook setup

## 0) Put hooks into the repo (portable)
Create:

```
.githooks/
  pre-commit
  commit-msg
  pre-push
```

Then tell Git to use them:

```bash
git config core.hooksPath .githooks
```

Commit the `.githooks/` directory so the team gets the hooks.

---

# 1) `pre-commit`: validate branch naming scheme

This prevents “random” branch names from entering your workflow.

```bash
#!/usr/bin/env bash
set -euo pipefail

branch="$(git branch --show-current || true)"

# Allow detached HEAD (e.g. CI, rebase)
if [[ -z "$branch" ]]; then
  exit 0
fi

# Allow mainline branches
case "$branch" in
  main|master|develop|release/*|hotfix/*)
    exit 0
    ;;
esac

# Expect: <scope>/<type>/FR-YYYY-NNN--<slug>
pattern='^[a-z0-9][a-z0-9-]*/(feat|fix|refactor|docs|test|perf|ci|build|chore)/FR-[0-9]{4}-[0-9]{3}--[a-z0-9][a-z0-9-]*$'

if [[ ! "$branch" =~ $pattern ]]; then
  echo "❌ Invalid branch name:"
  echo "    $branch"
  echo ""
  echo "Expected format:"
  echo "    <scope>/<type>/FR-YYYY-NNN--<slug>"
  echo ""
  echo "Examples:"
  echo "    notes/feat/FR-2026-004--notes-ref-layout"
  echo "    auth/fix/FR-2026-012--token-refresh-race"
  exit 1
fi
```

Make it executable:
```bash
chmod +x .githooks/pre-commit
```

---

# 2) `commit-msg`: enforce Conventional Commits *and* alignment

This ensures your commit message matches:

- `type(scope): ...`
- `type` aligns with branch type
- `scope` aligns with branch scope

```bash
#!/usr/bin/env bash
set -euo pipefail

msg_file="$1"
subject="$(head -n 1 "$msg_file" | tr -d '\r')"

# Allow merge commits + reverts
if [[ "$subject" =~ ^Merge\  ]] || [[ "$subject" =~ ^Revert\ \" ]]; then
  exit 0
fi

branch="$(git branch --show-current || true)"
if [[ -z "$branch" ]]; then
  # Detached HEAD - don't block
  exit 0
fi

# Parse branch: <scope>/<type>/FR-...--slug
scope_branch="$(echo "$branch" | cut -d/ -f1)"
type_branch="$(echo "$branch" | cut -d/ -f2)"

# Conventional Commits: type(scope): subject
pattern='^(feat|fix|refactor|docs|test|perf|ci|build|chore)(\(([a-z0-9][a-z0-9-]*)\))?: .+'

if [[ ! "$subject" =~ $pattern ]]; then
  echo "❌ Commit message is not Conventional Commits:"
  echo "    $subject"
  echo ""
  echo "Expected:"
  echo "    type(scope): summary"
  echo ""
  echo "Examples:"
  echo "    feat(notes): reorganize notes refs"
  echo "    fix(auth): handle token refresh race"
  exit 1
fi

type_commit="$(echo "$subject" | sed -E 's/^([a-z]+).*/\1/')"
scope_commit="$(echo "$subject" | sed -E 's/^[a-z]+\(([^)]+)\):.*/\1/' || true)"

# If scope omitted, we allow it — but recommend it
if [[ "$subject" =~ ^[a-z]+\([^)]+\): ]]; then
  if [[ "$scope_commit" != "$scope_branch" ]]; then
    echo "❌ Commit scope does not match branch scope:"
    echo "    branch scope: $scope_branch"
    echo "    commit scope:  $scope_commit"
    exit 1
  fi
else
  echo "⚠️  Commit scope missing. Recommended:"
  echo "    $type_branch($scope_branch): ..."
fi

if [[ "$type_commit" != "$type_branch" ]]; then
  echo "❌ Commit type does not match branch type:"
  echo "    branch type: $type_branch"
  echo "    commit type:  $type_commit"
  exit 1
fi
```

Make it executable:
```bash
chmod +x .githooks/commit-msg
```

---

# 3) `pre-push`: automatically push notes for the current branch key

This is the **killer feature**: whenever you push your branch, it pushes the matching notes ref too.

```bash
#!/usr/bin/env bash
set -euo pipefail

remote_name="${1:-origin}"
remote_url="${2:-}"

branch="$(git branch --show-current || true)"
if [[ -z "$branch" ]]; then
  exit 0
fi

# Only for your work-item branches:
# <scope>/<type>/FR-YYYY-NNN--<slug>
pattern='^[a-z0-9][a-z0-9-]*/(feat|fix|refactor|docs|test|perf|ci|build|chore)/FR-[0-9]{4}-[0-9]{3}--[a-z0-9][a-z0-9-]*$'
if [[ ! "$branch" =~ $pattern ]]; then
  exit 0
fi

notes_ref="refs/notes/$branch"

# Push only the relevant notes ref for this branch (not all notes)
if git show-ref --verify --quiet "$notes_ref"; then
  echo "🔎 Found notes ref: $notes_ref"
  echo "⬆️  Pushing notes ref to $remote_name …"
  git push "$remote_name" "$notes_ref:$notes_ref" >/dev/null
  echo "✅ Notes pushed."
else
  echo "⚠️  No notes found for this branch."
  echo "    Expected notes ref: $notes_ref"
  echo ""
  echo "Tip: add one with:"
  echo "    git notes --ref=$notes_ref add -m \"<task description>\" HEAD"
fi
```

Make it executable:
```bash
chmod +x .githooks/pre-push
```

---

# Optional: “require at least one note” (strict mode)

If you *really* want to enforce that a work item always has a note, change the `pre-push` warning to an error:

Replace the `else` block with:

```bash
else
  echo "❌ Missing notes for this branch."
  echo "    Expected: $notes_ref"
  echo ""
  echo "Add one with:"
  echo "    git notes --ref=$notes_ref add -m \"<task description>\" HEAD"
  exit 1
fi
```

---

# How it feels now (workflow)

1) Create branch (your `git fr-new` alias)  
2) Commit normally  
3) Add notes sometimes  
4) Push branch → **notes auto-push**

---

If you want, I can add a **`post-checkout` hook** that prints the active notes ref when you switch branches (super nice for “what am I doing here again?”), *without being noisy*.

---

## User

That would be very nice

---

## Assistant

Nice — the **post-checkout** hook is perfect for this workflow. It gives you a tiny “context banner” when you switch branches, and it can even show the task note if one exists.

Below is a clean version that:

- only triggers on **your work-item branches**
- uses the **exact same key** (branch name == notes ref name)
- prints the **notes ref path**
- optionally shows the **note content** (first ~15 lines) if present

---

# `.githooks/post-checkout`

Create the file:

```bash
mkdir -p .githooks
$EDITOR .githooks/post-checkout
chmod +x .githooks/post-checkout
```

Paste this:

```bash
#!/usr/bin/env bash
set -euo pipefail

# post-checkout args:
#   $1 = previous HEAD
#   $2 = new HEAD
#   $3 = 1 if branch checkout, 0 if file checkout
is_branch_checkout="${3:-0}"

# Only show on actual branch switches
if [[ "$is_branch_checkout" != "1" ]]; then
  exit 0
fi

branch="$(git branch --show-current || true)"
if [[ -z "$branch" ]]; then
  exit 0
fi

# Allow mainline branches without noise
case "$branch" in
  main|master|develop|release/*|hotfix/*)
    exit 0
    ;;
esac

# Our scheme: <scope>/<type>/FR-YYYY-NNN--<slug>
pattern='^[a-z0-9][a-z0-9-]*/(feat|fix|refactor|docs|test|perf|ci|build|chore)/FR-[0-9]{4}-[0-9]{3}--[a-z0-9][a-z0-9-]*$'
if [[ ! "$branch" =~ $pattern ]]; then
  exit 0
fi

notes_ref="refs/notes/$branch"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📌 Work item:  $branch"
echo "🗒️  Notes ref:  $notes_ref"

if git show-ref --verify --quiet "$notes_ref"; then
  echo ""
  echo "📝 Task note (preview):"
  # Show first 15 lines of note for HEAD (or show full note if short)
  git notes --ref="$notes_ref" show HEAD 2>/dev/null | sed -n '1,15p' || true
  echo ""
  echo "Tip: edit with  git notes --ref=$notes_ref edit HEAD"
else
  echo ""
  echo "⚠️  No note yet for this work item."
  echo "Tip: add one with  git notes --ref=$notes_ref add -m \"<task description>\" HEAD"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
```

---

# Enable hooks for the repo

If you haven’t already:

```bash
git config core.hooksPath .githooks
```

Commit the hooks:

```bash
git add .githooks
git commit -m "chore(dev): add git hooks for branch/notes workflow"
```

---

# What you’ll see when switching branches

When you do:

```bash
git switch notes/feat/FR-2026-004--notes-ref-layout
```

You’ll get:

- ✅ branch key
- ✅ notes ref path
- ✅ note preview (or a reminder to create one)

No spam on `main`, `release/*`, etc.

---

# Optional refinement: show note for *latest commit on branch*
If you prefer showing the note for the tip commit of the branch, replace:

```bash
git notes --ref="$notes_ref" show HEAD
```

with:

```bash
git notes --ref="$notes_ref" show "$(git rev-parse HEAD)"
```

(Usually the same, but this makes it explicit.)

---

If you want the *ultimate* polish: I can add a matching **post-commit hook** that automatically reminds you to add a note if the branch has none yet (non-blocking, just a friendly nudge).