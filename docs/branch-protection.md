# Branch protection and release promotion

`CODEOWNERS` only requests reviewers. It does not enforce approval by itself.
Configure a GitHub ruleset for `main` with the following repository settings:

1. Require a pull request and one approval from Code Owners.
2. Require the `static` and `core-integration` checks from `Kest CI`.
3. Dismiss stale approvals and require resolution of review conversations.
4. Block force pushes and deletion of `main`.
5. Restrict bypass to the break-glass repository administrator account.
6. Require linear history or squash merge so every promoted revision maps to one reviewed change.

The current implementation branch is `codex/production-oss-platform`. Do not change
`origin/main`, `legacy`, `legacy_v2`, or `legacy-v2-final` during implementation. After
acceptance, promote this branch through a reviewed pull request. Retain the previous main
tip through the existing `legacy_v2` branch and `legacy-v2-final` tag before changing the
default branch.

For a release, record the tested commit SHA and the exact command results in
`docs/implementation-status.md`. A green CODEOWNERS review without the ruleset above is
not an enforcement boundary.
