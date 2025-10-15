# GitHub ↔ GitLab Mirroring

## Overview

This guide explains how to keep a GitHub repository and a GitLab project in
near-real-time sync. The flow uses GitLab's built-in mirroring so that:

- Commits pushed to GitHub automatically appear in GitLab (pull mirroring).
- Changes made in GitLab are immediately pushed back to GitHub (push mirroring).

The procedure assumes GitHub remains the canonical remote and GitLab acts as a
continuously updated mirror that can also serve as a contribution surface when required.

## Prerequisites

- Maintainer access to the GitHub repository you want to mirror.
- Maintainer access to a GitLab project (create a new **blank project** with no README
  if it does not yet exist).
- Ability to create Personal Access Tokens (PAT) in both GitHub and GitLab.
- SSH keys or deploy tokens are optional; this guide uses HTTPS with PATs for
  simplicity.

## 1. Prepare credentials

### 1.1 GitHub Personal Access Token

1. Visit <https://github.com/settings/tokens?type=beta> and create a **Fine-grained
   PAT** scoped to the repository.
1. Enable the following permissions:
   - **Repository permissions → Contents:** Read & write
   - **Repository permissions → Metadata:** Read-only (auto-enabled)
   - **Repository permissions → Webhooks:** Read & write (needed for webhook creation in
     step 4)
   - Enable access to Git submodules and Git LFS if you use them.
1. Copy the generated token. It will be used for both pull and push mirroring
   configurations in GitLab.

### 1.2 GitLab Personal Access Token (for webhook-triggered pulls)

1. Navigate to **User Settings → Access Tokens** in GitLab.
1. Create a token with the `api` scope. This lets GitHub trigger a "pull mirror" on
   GitLab via API when new commits land on GitHub.
1. Record the token securely; you will add it to a GitHub webhook later.

## 2. Configure GitLab pull mirroring (GitHub → GitLab)

1. In the GitLab project, go to **Settings → Repository → Mirroring repositories**.
1. Click **Mirror a repository**.
1. Set **URL** to the GitHub repository's HTTPS URL (e.g.
   `https://github.com/ORG/REPO.git`).
1. Under **Mirror direction**, choose **Pull**.
1. For **Authentication method**, choose **Password**.
1. Use your GitHub username (or a machine account) as the **Username** and paste the
   GitHub PAT as the **Password**.
1. Leave **Mirror repository** enabled and save. GitLab will immediately perform an
   initial fetch from GitHub.
1. Optional: enable **Only mirror protected branches** if you do not want feature
   branches mirrored.

GitLab will now poll GitHub every few minutes. We will accelerate this in step 4.

## 3. Configure GitLab push mirroring (GitLab → GitHub)

1. In the same **Mirroring repositories** panel, add a second mirror entry.
1. Set **URL** to the GitHub repository's HTTPS URL.
1. Choose **Push** as the mirror direction.
1. Reuse the GitHub PAT as the **Password** (the same token can push provided it has
   write access).
1. Enable **Mirror only protected branches** if you want to prevent accidental
   force-pushes from GitLab.
1. Save the mirror. GitLab will now push every commit created in GitLab to GitHub almost
   immediately.

When using push mirroring, avoid creating divergent history on GitHub that omits GitLab
commits. Force-pushes should be coordinated carefully across both hosting providers.

## 4. Trigger immediate pulls from GitHub

The default GitLab schedule polls GitHub roughly every 5 minutes. To get near-instant
mirroring, configure a GitHub webhook that tells GitLab to pull as soon as new commits
arrive.

1. Determine the GitLab project ID from the project home page (**Project information →
   Details**). It looks like `namespace%2Fproject` or a numeric ID.
1. Construct the pull URL:
   `https://gitlab.com/api/v4/projects/<PROJECT_ID>/mirror/pull`.
1. In the GitHub repository, open **Settings → Webhooks → Add webhook**.
1. Set **Payload URL** to the GitLab pull URL.
1. Choose **Content type: application/json**.
1. For **Secret**, use the GitLab PAT created in step 1.2. GitLab expects this token in
   the `X-Gitlab-Token` header.
1. Select **Just the push event** and create the webhook.

Whenever GitHub receives a push, the webhook fires and GitLab immediately imports the
new commits.

## 5. Validate the mirror

1. Make a test commit on GitHub and push it.
1. Confirm the commit appears in GitLab within a minute (or immediately if the webhook
   is configured).
1. Use the GitLab Web IDE or push to GitLab via `git push gitlab main` to add another
   commit.
1. Verify the change shows up on GitHub after the push mirror runs (usually seconds).
1. Check that issues, merge requests, and wiki content are not mirrored—only Git data is
   synchronized.

## 6. Operational best practices

- **Choose a primary remote for day-to-day pushes.** Even with two-way mirroring, pick
  either GitHub or GitLab as the default upstream for developers to minimize race
  conditions.
- **Disable automatic pipelines on mirrors** if you do not want double CI runs.
  Configure pipeline triggers carefully.
- **Align branch protection rules** on both platforms so protected branches behave
  consistently.
- **Avoid force-pushes** to protected branches. If one occurs, manually run the mirror
  jobs or temporarily disable the opposite mirror to prevent conflicts.
- **Synchronize tags and releases**: GitLab mirrors tags by default. Releases/Artifacts
  must be recreated manually.
- **Monitor mirror status**: GitLab surfaces mirror errors in the same settings page and
  in project audit events. Address authentication failures promptly.

Following these steps keeps both hosting providers in sync while retaining the strengths
of each platform.

## 7. Free feature comparison for open source projects

| Category                   | GitHub Free (public repositories)                                                                                                                                                                                                                            | GitLab for Open Source Program                                                                                                                                                                                                                                                                                                                      | nanoGPT impact                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Repository & collaboration | Unlimited collaborators on public repos, branch protection, Dependabot alerts, and community tooling ([GitHub plans](https://docs.github.com/en/get-started/learning-about-github/githubs-plans)).                                                           | Approved namespaces receive the full **Ultimate** tier at no cost, unlocking portfolio, planning, and review tooling once OSI licensing and public visibility criteria are met ([GitLab community programs](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/subscriptions/community_programs.md)).                                           | ⭐ Governance parity keeps issue and merge request triage aligned across hosts.              |
| CI/CD & compute            | GitHub-hosted runners provide unlimited Actions minutes for public repositories and can be upgraded to larger machine types on request ([Actions billing](https://docs.github.com/en/actions/learn-github-actions/usage-limits-billing-and-administration)). | The program grants 50,000 shared-runner CI minutes per namespace each month and allows attaching additional runners if we exhaust the quota ([GitLab community programs](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/subscriptions/community_programs.md)).                                                                              | ⭐ Dual CI capacity keeps lint, typing, and training smoke tests affordable on both mirrors. |
| Packages & artifacts       | GitHub Packages storage and bandwidth are free for public packages, covering containers, models, and datasets ([GitHub Packages](https://docs.github.com/en/packages/learn-github-packages/about-github-packages)).                                          | GitLab’s built-in Container Registry is available to every project, and Ultimate adds package metadata insights plus cleanup policies ([GitLab container registry](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/user/packages/container_registry/_index.md)).                                                                             | ⭐ Enables mirrored checkpoints and Docker images so contributors can use either host.       |
| Developer experience       | GitHub includes 120 Codespaces core-hours and 15 GB of storage per month plus optional Copilot Chat access for public repos through the Sponsors program ([GitHub plans](https://docs.github.com/en/get-started/learning-about-github/githubs-plans)).       | GitLab Web IDE, merge request reviewer workflows, and Ultimate planning tools (roadmaps, OKRs, and portfolios) unlock after approval ([GitLab community programs](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/subscriptions/community_programs.md)).                                                                                     | ⭐ Browser-based IDE parity lowers onboarding friction for occasional contributors.          |
| Security & compliance      | Secret scanning, Dependabot alerts, and deployment protection rules ship with public repositories ([GitHub plans](https://docs.github.com/en/get-started/learning-about-github/githubs-plans)).                                                              | Ultimate adds advanced SAST, dependency, container, and secret scanning with compliance frameworks ([GitLab SAST](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/user/application_security/sast/_index.md); [GitLab compliance](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/security/compliance_security_policy_management.md)). | ⭐ Balanced security gates keep mirrored pipelines under consistent guardrails.              |

### Why the ⭐ impact areas matter for nanoGPT

- **Unified collaboration surface**: Unlimited contributors on GitHub and Ultimate
  planning features on GitLab mean we can triage issues or merge requests in either
  ecosystem while preserving governance parity
  ([GitHub plans](https://docs.github.com/en/get-started/learning-about-github/githubs-plans);
  [GitLab community programs](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/subscriptions/community_programs.md)).
- **Sustainable CI throughput**: The combination of unlimited GitHub Actions minutes for
  public repos and 50,000 GitLab CI minutes per month keeps our lint, typing, mutation,
  and training smoke tests affordable during heavy experimentation
  ([Actions billing](https://docs.github.com/en/actions/learn-github-actions/usage-limits-billing-and-administration);
  [GitLab community programs](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/subscriptions/community_programs.md)).
- **Multi-host artifact delivery**: Both registries support free distribution, so we can
  publish pretrained checkpoints or Docker toolchains once and mirror them, reducing
  contributor friction
  ([GitHub Packages](https://docs.github.com/en/packages/learn-github-packages/about-github-packages);
  [GitLab container registry](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/user/packages/container_registry/_index.md)).
- **Accessible development environments**: GitHub Codespaces allowances plus GitLab Web
  IDE let casual contributors test patches without local setup, improving community
  onboarding
  ([GitHub plans](https://docs.github.com/en/get-started/learning-about-github/githubs-plans);
  [GitLab community programs](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/subscriptions/community_programs.md)).
- **Balanced security posture**: Dependabot, secret scanning, and deployment protections
  on GitHub pair with GitLab’s Ultimate-grade SAST and compliance tooling to keep
  mirrored pipelines aligned with our security requirements
  ([GitHub plans](https://docs.github.com/en/get-started/learning-about-github/githubs-plans);
  [GitLab SAST](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/user/application_security/sast/_index.md);
  [GitLab compliance](https://gitlab.com/gitlab-org/gitlab/-/blob/master/doc/security/compliance_security_policy_management.md)).
