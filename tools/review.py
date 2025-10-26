#!/usr/bin/env python3
"""Utilities for inspecting and replying to GitHub PR review comments.

This CLI shells out to `gh api graphql` to keep implementation simple while
staying consistent with the repository's GitHub tooling. It supports:

* Listing review threads for a pull request, optionally filtered to unresolved
  threads or threads without a reply from the current user.
* Bulk replying to matching threads with a shared response body.

Run commands through UV, for example:

```bash
uv run python tools/review.py list --pr 86 --unreplied --unresolved
uv run python tools/review.py bulk-reply --pr 86 --body "Thanks, addressed!"
```
"""

from __future__ import annotations

import json
import shlex
import subprocess
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import Annotated, cast

import typer

app = typer.Typer(add_completion=False)


def _ensure_mapping(value: object, context: str) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return cast(Mapping[str, object], value)
    raise ReviewToolError(f"{context} must be a mapping, got {type(value).__name__}")


def _ensure_list(value: object, context: str) -> list[object]:
    if isinstance(value, list):
        return [cast(object, item) for item in value]
    raise ReviewToolError(f"{context} must be a list, got {type(value).__name__}")


def _require_str(mapping: Mapping[str, object], key: str, *, context: str) -> str:
    value = mapping.get(key)
    if isinstance(value, str):
        return value
    raise ReviewToolError(f"{context} missing string field '{key}'")


def _require_bool(mapping: Mapping[str, object], key: str, *, context: str) -> bool:
    value = mapping.get(key)
    if isinstance(value, bool):
        return value
    raise ReviewToolError(f"{context} missing boolean field '{key}'")


def _optional_str(mapping: Mapping[str, object], key: str) -> str | None:
    value = mapping.get(key)
    return value if isinstance(value, str) else None


def _optional_int(mapping: Mapping[str, object], key: str) -> int | None:
    value = mapping.get(key)
    return value if isinstance(value, int) else None


PRNumberOption = Annotated[
    int,
    typer.Option(
        ...,
        "--pr",
        help="Pull request number to inspect.",
    ),
]

RemoteOption = Annotated[
    str,
    typer.Option(
        "origin",
        "--remote",
        help="Git remote name used to infer owner/repo (default: origin).",
    ),
]

UnrepliedFilterOption = Annotated[
    bool,
    typer.Option(
        False,
        "--unreplied",
        help="Only show threads without a viewer reply.",
    ),
]

UnresolvedFilterOption = Annotated[
    bool,
    typer.Option(
        False,
        "--unresolved",
        help="Only show unresolved threads.",
    ),
]

UnrepliedToggleOption = Annotated[
    bool,
    typer.Option(
        True,
        "--unreplied/--all",
        help="Reply only to threads without a viewer reply (default: true).",
    ),
]

RepliesFileOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--replies",
        help="Path to JSON file mapping comment URLs or IDs to reply strings.",
    ),
]

CommentsFileOption = Annotated[
    Path,
    typer.Option(
        ...,
        "--comments",
        help="Path to file listing comment URLs, node IDs, or database IDs to delete.",
    ),
]

ReplyDryRunOption = Annotated[
    bool,
    typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Preview replies without publishing them.",
    ),
]

DeleteDryRunOption = Annotated[
    bool,
    typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Preview deletions without removing comments.",
    ),
]


class ReviewToolError(RuntimeError):
    """Raised when an invocation fails."""


@dataclass
class ReviewComment:
    """Represents a single review comment inside a thread."""

    id: str
    url: str
    body: str
    author: str
    viewer_did_author: bool
    created_at: str
    database_id: int | None


@dataclass
class ReviewThread:
    """Aggregated review thread information used for filtering and replies."""

    id: str
    is_resolved: bool
    comments: list[ReviewComment]

    def needs_reply_from(self, _viewer: str) -> bool:
        """Return True when the viewer has not replied anywhere in the thread."""
        has_viewer_comment = any(comment.viewer_did_author for comment in self.comments)
        if has_viewer_comment:
            return False
        return bool(self.comments)

    def latest_non_viewer_comment(self, viewer: str) -> ReviewComment | None:
        """Return the most recent comment authored by someone other than viewer."""
        for comment in reversed(self.comments):
            if comment.author != viewer:
                return comment
        return None


def _run(command: list[str]) -> CompletedProcess[str]:
    typer.echo(f"$ {' '.join(shlex.quote(arg) for arg in command)}", err=True)
    return subprocess.run(command, check=True, capture_output=True, text=True)


def _ensure_repo_root() -> Path:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise ReviewToolError("This tool must be run inside the repository.") from exc
    return Path(result.stdout.strip())


def _infer_repo(remote: str = "origin") -> tuple[str, str]:
    """Infer the GitHub <owner>, <repo> pair from the configured remote."""
    result = subprocess.run(
        ["git", "remote", "get-url", remote],
        check=True,
        capture_output=True,
        text=True,
    )
    url = result.stdout.strip()
    if url.endswith(".git"):
        url = url[:-4]

    if url.startswith("git@github.com:"):
        path = url.split(":", 1)[1]
    elif url.startswith("https://github.com/"):
        path = url.split("github.com/", 1)[1]
    else:
        raise ReviewToolError(
            f"Unsupported remote URL format for {remote!r}: {result.stdout.strip()}"
        )

    owner, _, repo = path.partition("/")
    if not owner or not repo:
        raise ReviewToolError(f"Unable to parse GitHub owner/repo from {url!r}")
    return owner, repo


def _gh_graphql(query: str, *, variables: dict[str, object]) -> Mapping[str, object]:
    """Execute `gh api graphql` with the provided query and variables."""
    command: list[str] = ["gh", "api", "graphql", "-f", f"query={query}"]
    for key, value in variables.items():
        if isinstance(value, (int, float)):
            command.extend(["--field", f"{key}={value}"])
        else:
            command.extend(["--raw-field", f"{key}={value}"])
    try:
        result = _run(command)
    except subprocess.CalledProcessError as exc:
        stderr_obj = cast(str | None, getattr(exc, "stderr", None))
        message = (stderr_obj or "").strip()
        raise ReviewToolError(message or "gh api graphql failed") from exc

    stdout = result.stdout
    try:
        response_raw = cast(object, json.loads(stdout))
    except json.JSONDecodeError as exc:
        raise ReviewToolError("Failed to parse GitHub API response as JSON") from exc

    response = _ensure_mapping(response_raw, "GitHub response")

    errors_obj = response.get("errors")
    if isinstance(errors_obj, list) and errors_obj:
        errors_list = cast(list[object], errors_obj)
        first_error: object = errors_list[0]
        if isinstance(first_error, Mapping):
            error_mapping = cast(Mapping[str, object], first_error)
            message = _optional_str(error_mapping, "message") or str(error_mapping)
        else:
            message = str(first_error)
        raise ReviewToolError(f"GitHub GraphQL error: {message}")
    if errors_obj and not isinstance(errors_obj, list):
        raise ReviewToolError("GitHub GraphQL error payload must be a list")

    data_obj = response.get("data")
    if data_obj is None:
        raise ReviewToolError("GitHub GraphQL response missing 'data' payload")
    return _ensure_mapping(data_obj, "GitHub response data")


REVIEW_QUERY = """
query($owner:String!, $name:String!, $number:Int!, $pageSize:Int = 50, $cursor:String) {
  viewer { login }
  repository(owner:$owner, name:$name) {
    pullRequest(number:$number) {
      id
      reviewThreads(first:$pageSize, after:$cursor) {
        nodes {
          id
          isResolved
          comments(first: 50) {
            nodes {
              id
              body
              url
              createdAt
              databaseId
              viewerDidAuthor
              author {
                login
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}
""".strip()


REPLY_MUTATION = """
mutation($pr:ID!, $parent:ID!, $body:String!) {
  addPullRequestReviewComment(input:{pullRequestId:$pr, body:$body, inReplyTo:$parent}) {
    comment {
      id
      url
    }
  }
}
""".strip()


DELETE_COMMENT_MUTATION = """
mutation($id:ID!) {
  deletePullRequestReviewComment(input:{id:$id}) {
    clientMutationId
  }
}
""".strip()


@dataclass
class FetchResult:
    pull_request_id: str
    viewer: str
    threads: list[ReviewThread]


def fetch_review_threads(owner: str, repo: str, number: int) -> FetchResult:
    """Retrieve all review threads for a pull request."""
    from_cursor: str | None = None
    viewer_login: str | None = None
    pull_request_id: str | None = None
    threads: list[ReviewThread] = []

    while True:
        variables: dict[str, object] = {
            "owner": owner,
            "name": repo,
            "number": number,
            "pageSize": 50,
        }
        if from_cursor:
            variables["cursor"] = from_cursor
        data = _gh_graphql(REVIEW_QUERY, variables=variables)

        repo_data = _ensure_mapping(data.get("repository"), "repository")
        if viewer_login is None:
            viewer_mapping = _ensure_mapping(data.get("viewer"), "viewer")
            viewer_login = _require_str(viewer_mapping, "login", context="viewer")
        pr_data_obj = repo_data.get("pullRequest")
        if pr_data_obj is None:
            raise ReviewToolError(f"Pull request #{number} not found in {owner}/{repo}")
        pr_data = _ensure_mapping(pr_data_obj, "pullRequest")

        if pull_request_id is None:
            pull_request_id = _require_str(pr_data, "id", context="pullRequest")

        thread_connection = _ensure_mapping(
            pr_data.get("reviewThreads"), "pullRequest.reviewThreads"
        )
        node_items = _ensure_list(
            thread_connection.get("nodes"), "pullRequest.reviewThreads.nodes"
        )
        for node_obj in node_items:
            node = _ensure_mapping(node_obj, "reviewThreads.nodes[]")
            comments_mapping = _ensure_mapping(
                node.get("comments"), "reviewThreads.comments"
            )
            comment_items = _ensure_list(
                comments_mapping.get("nodes"), "reviewThreads.comments.nodes"
            )
            comments: list[ReviewComment] = []
            for comment_obj in comment_items:
                comment_mapping = _ensure_mapping(comment_obj, "comment")
                author_login = "<unknown>"
                author_obj = comment_mapping.get("author")
                if isinstance(author_obj, Mapping):
                    login = _optional_str(
                        cast(Mapping[str, object], author_obj), "login"
                    )
                    if login:
                        author_login = login
                comments.append(
                    ReviewComment(
                        id=_require_str(comment_mapping, "id", context="comment"),
                        url=_require_str(comment_mapping, "url", context="comment"),
                        body=_require_str(comment_mapping, "body", context="comment"),
                        author=author_login,
                        viewer_did_author=_require_bool(
                            comment_mapping, "viewerDidAuthor", context="comment"
                        ),
                        created_at=_require_str(
                            comment_mapping, "createdAt", context="comment"
                        ),
                        database_id=_optional_int(comment_mapping, "databaseId"),
                    )
                )
            threads.append(
                ReviewThread(
                    id=_require_str(node, "id", context="reviewThread"),
                    is_resolved=_require_bool(
                        node, "isResolved", context="reviewThread"
                    ),
                    comments=comments,
                )
            )

        page_info = _ensure_mapping(thread_connection.get("pageInfo"), "pageInfo")
        if not _require_bool(page_info, "hasNextPage", context="pageInfo"):
            break
        from_cursor = _optional_str(page_info, "endCursor")

    assert viewer_login is not None
    assert pull_request_id is not None
    return FetchResult(
        pull_request_id=pull_request_id,
        viewer=viewer_login,
        threads=threads,
    )


def apply_filters(
    threads: Iterable[ReviewThread],
    *,
    viewer: str,
    unreplied: bool,
    unresolved: bool,
) -> list[ReviewThread]:
    filtered: list[ReviewThread] = []
    for thread in threads:
        if unreplied and not thread.needs_reply_from(viewer):
            continue
        if unresolved and thread.is_resolved:
            continue
        filtered.append(thread)
    return filtered


def _print_threads(threads: Iterable[ReviewThread], *, viewer: str) -> None:
    found_any = False
    for thread in threads:
        if not thread.comments:
            continue
        found_any = True
        head = thread.comments[0]
        status = "resolved" if thread.is_resolved else "unresolved"
        replied = "replied" if not thread.needs_reply_from(viewer) else "no-reply"
        typer.echo(f"- {head.url} ({status}, {replied})")
        typer.echo(f"  by {head.author} at {head.created_at}")
        preview = head.body.strip().splitlines()
        for line in preview[:3]:
            typer.echo(f"    {line}")
        if len(preview) > 3:
            typer.echo("    …")
    if not found_any:
        typer.echo("No review threads match the selected filters.")


def _bulk_reply(
    *,
    fetch: FetchResult,
    targets: Iterable[ReviewThread],
    replies: dict[str, str],
    dry_run: bool,
) -> None:
    threads = list(targets)
    if not threads:
        typer.echo("No threads to reply to; exiting.")
        return

    for thread in threads:
        target_comment = thread.latest_non_viewer_comment(fetch.viewer)
        if not target_comment:
            typer.echo(
                f"Skipping thread {thread.id}: could not find a non-viewer comment.",
                err=True,
            )
            continue

        body = (
            replies.get(target_comment.url)
            or replies.get(target_comment.id)
            or replies.get(thread.id)
        )
        if body is None:
            typer.echo(
                f"Skipping {target_comment.url}: no reply text found in mapping.",
                err=True,
            )
            continue

        if dry_run:
            typer.echo(f"[dry-run] Would reply to {target_comment.url} with:\n{body}\n")
            continue

        variables = {
            "pr": fetch.pull_request_id,
            "parent": target_comment.id,
        }
        try:
            _ = _gh_graphql(
                REPLY_MUTATION,
                variables={**variables, "body": body},
            )
            typer.echo(f"Replied to {target_comment.url}")
        except ReviewToolError as exc:
            typer.echo(
                f"Failed to reply to {target_comment.url}: {exc}",
                err=True,
            )


def _load_replies(path: Path) -> dict[str, str]:
    if not path.exists():
        raise ReviewToolError(f"Replies file not found: {path}")
    try:
        payload = path.read_text()
    except OSError as exc:  # pragma: no cover - filesystem error
        raise ReviewToolError(f"Failed to read replies file: {exc}") from exc

    try:
        data_raw = cast(object, json.loads(payload))
    except json.JSONDecodeError as exc:
        raise ReviewToolError(f"Failed to parse replies JSON: {exc}") from exc

    if not isinstance(data_raw, dict):
        raise ReviewToolError("Replies JSON must map comment identifiers to strings.")

    mapping = cast(dict[str, object], data_raw)
    replies: dict[str, str] = {}
    for key_obj, value_obj in mapping.items():
        if not isinstance(value_obj, str):
            raise ReviewToolError(
                "Replies JSON must use string keys with string values."
            )
        replies[key_obj] = value_obj
    return replies


def _load_comment_targets(path: Path) -> list[str]:
    if not path.exists():
        raise ReviewToolError(f"Comments file not found: {path}")
    try:
        payload = path.read_text()
    except OSError as exc:  # pragma: no cover - filesystem error
        raise ReviewToolError(f"Failed to read comments file: {exc}") from exc

    try:
        data_raw = cast(object, json.loads(payload))
    except json.JSONDecodeError:
        items = [line.strip() for line in payload.splitlines() if line.strip()]
        if not items:
            raise ReviewToolError("Comments file is empty.")
        return items

    if isinstance(data_raw, list):
        items_raw = cast(list[object], data_raw)
        items = [item for item in items_raw if isinstance(item, str) and item.strip()]
        if not items:
            raise ReviewToolError("Comments JSON list must contain non-empty strings.")
        return items

    if isinstance(data_raw, dict):
        mapping = cast(dict[str, object], data_raw)
        items = [key for key in mapping.keys() if key.strip()]
        if not items:
            raise ReviewToolError("Comments JSON object must have string keys.")
        return items

    raise ReviewToolError(
        "Unsupported comments file format; use JSON list/object or newline-separated text."
    )


def _comment_lookup(fetch: FetchResult) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for thread in fetch.threads:
        for comment in thread.comments:
            _ = mapping.setdefault(comment.id, comment.id)
            _ = mapping.setdefault(comment.url, comment.id)
            if "#" in comment.url:
                _ = mapping.setdefault(comment.url.split("#")[-1], comment.id)
            if comment.database_id is not None:
                _ = mapping.setdefault(str(comment.database_id), comment.id)
    return mapping


@app.command("list")
def cmd_list(
    pr: PRNumberOption,
    unreplied: UnrepliedFilterOption = False,
    unresolved: UnresolvedFilterOption = False,
    remote: RemoteOption = "origin",
) -> None:
    """List review threads for a pull request."""
    _ = _ensure_repo_root()
    owner, repo = _infer_repo(remote)
    fetch = fetch_review_threads(owner, repo, pr)
    filtered = apply_filters(
        fetch.threads,
        viewer=fetch.viewer,
        unreplied=unreplied,
        unresolved=unresolved,
    )
    _print_threads(filtered, viewer=fetch.viewer)


@app.command("bulk-reply")
def cmd_bulk_reply(
    pr: PRNumberOption,
    replies_file: RepliesFileOption,
    unreplied: UnrepliedToggleOption = True,
    unresolved: UnresolvedFilterOption = False,
    remote: RemoteOption = "origin",
    dry_run: ReplyDryRunOption = False,
) -> None:
    """Reply to multiple review threads using per-comment messages from file."""
    _ensure_repo_root()
    owner, repo = _infer_repo(remote)
    fetch = fetch_review_threads(owner, repo, pr)
    targets = apply_filters(
        fetch.threads,
        viewer=fetch.viewer,
        unreplied=unreplied,
        unresolved=unresolved,
    )
    replies = _load_replies(replies_file)
    _bulk_reply(fetch=fetch, targets=targets, replies=replies, dry_run=dry_run)


@app.command("delete")
def cmd_delete(
    pr: PRNumberOption,
    comments_file: CommentsFileOption,
    remote: RemoteOption = "origin",
    dry_run: DeleteDryRunOption = False,
) -> None:
    """Delete one or more review comments identified in a file."""
    _ensure_repo_root()
    owner, repo = _infer_repo(remote)
    fetch = fetch_review_threads(owner, repo, pr)
    lookup = _comment_lookup(fetch)
    targets = _load_comment_targets(comments_file)

    resolved: list[tuple[str, str]] = []
    for identifier in targets:
        key = identifier.strip()
        comment_id = lookup.get(key)
        if comment_id is None and key.startswith("http"):
            comment_id = lookup.get(key.split("#")[-1])
        if comment_id is None:
            typer.echo(
                f"Skipping {identifier}: no matching comment found in PR.", err=True
            )
            continue
        resolved.append((identifier, comment_id))

    if not resolved:
        typer.echo("No comments matched the provided identifiers; exiting.")
        return

    for original, comment_id in resolved:
        if dry_run:
            typer.echo(f"[dry-run] Would delete comment {original} (id: {comment_id})")
            continue
        try:
            _ = _gh_graphql(
                DELETE_COMMENT_MUTATION,
                variables={"id": comment_id},
            )
            typer.echo(f"Deleted comment {original}")
        except ReviewToolError as exc:
            typer.echo(f"Failed to delete {original}: {exc}", err=True)


def main(argv: list[str] | None = None) -> None:
    try:
        app(prog_name="uv run python tools/review.py", args=argv)
    except ReviewToolError as exc:
        typer.secho(f"Error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    main()
