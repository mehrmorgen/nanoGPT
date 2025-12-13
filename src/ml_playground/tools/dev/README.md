# Development Tools

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.

</details>

## Purpose

Tools for developer productivity, review automation, and repo hygiene.

## Usage

```bash
# List review comments
uv run tools dev review-list <pr>

# Bulk reply to reviews
uv run tools dev review-bulk-reply <pr> --replies replies.json

# List recent GitHub Actions runs (requires GitHub CLI: `gh`)
uv run tools dev gha --limit 10

# Inspect the latest run and show failed logs
uv run tools dev gha --latest --log-failed

# Check workflow status
uv run tools dev workflow-status

# Setup AI guidelines
uv run tools dev setup-ai-guidelines <tool>
```

## Structure

```bash
src/ml_playground/tools/dev/
├── README.md            # package documentation (this file)
├── ai_guidelines.py     # AI guideline setup
├── batch_review.py      # batch review helpers
├── dev.py               # main DevTools class
├── github_actions.py    # GitHub Actions helper (wraps `gh run list/view`)
├── hygiene.py           # cleanup tools
├── review.py            # GitHub review automation
├── status.py            # simple status checks
└── workflow_status.py   # comprehensive workflow reporting
```
