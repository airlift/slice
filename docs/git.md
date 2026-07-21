# Git

This document contains expectations and guidelines for using Git.

For the philosophy behind these practices, see [Development Philosophy](development-philosophy.md).

## Branch Naming Convention

When creating branches, use the following format: `user/<username>/<short-description>`. `user` is a
literal prefix, `<username>` is either your GitHub username or a preferred name, and
`<short-description>` is a brief summary of the work being done in the branch and should be less
than 30 characters. Use hyphens to separate words in the description.

## Commit Message Standards

Reference: https://cbea.ms/git-commit/

Follow these seven rules for writing effective commit messages:

1. **Separate subject from body with a blank line** — Helps Git tools parse messages correctly.

2. **Limit the subject line to 50 characters** — Forces clarity and ensures readability in logs.

3. **Capitalize the subject line** — Begin with a capital letter.

4. **Do not end the subject line with a period** — Conserves space and maintains consistency.

5. **Use the imperative mood in the subject line** — Write as a command (e.g., "Add feature" not
   "Added feature"). A good test: the subject should complete the sentence "If applied, this
   commit will _____."

6. **Wrap the body at 72 characters** — Allows room for Git's indentation while staying under 80
   characters total.

7. **Use the body to explain what and why, not how** — The code shows how; the commit message
   should provide context and reasoning for the change.

## Build and Testing Requirements

- Every commit MUST pass all tests AND build successfully before being merged into the main branch.
- Commits should be ordered in a logical manner.
- Each commit should represent a single logical change.
- Avoid large commits that encompass multiple changes or features.
- Keep commits focused. A commit that significantly changes both production code and tests
  is often doing multiple things (e.g., refactoring + adding a feature). Split these into
  smaller, reviewable units.
- Always keep refactoring in separate commits from feature work or bug fixes.
- Commits should reflect human authorship only. Do not include AI tools as co-authors.
- When committing edits in the current branch, update the appropriate commit rather
  than creating a new commit for unrelated changes. There are several approaches:
  - **Amend**: Use `git commit --amend` when updating the most recent commit.
  - **Fixup + autosquash**: For updating an older commit, create a fixup commit
    targeting it with `git commit --fixup=<sha>`, then run
    `git rebase --autosquash main` to automatically fold fixups into their target
    commits.
  - **Interactive rebase**: Use `git rebase -i main` to reorder, squash, edit, or
    drop commits. This is the most flexible approach for restructuring branch
    history. Note: this requires an interactive terminal. In non-interactive
    contexts (CI, AI agents), use fixup + autosquash instead.
  Contributors should be comfortable force-pushing feature branches after rewriting
  history.

## Pull Requests

- **Title**: Keep it short but descriptive. It should clearly convey what the PR accomplishes.
  For single-commit PRs, GitHub automatically populates the title from the commit subject.
  For multi-commit PRs, use the main commit's subject as the title.
- **Description**: Provide context for reviewers — what changed, why, and any relevant background.
  Aim for 10-20 lines; enough detail to understand the change without reading every line of code.
  For single-commit PRs, GitHub populates this from the commit body. For multi-commit PRs,
  consider using the main commit's body as the description.
- **Code Review**: After creating a PR, add a comment with `/code-review` to trigger the code
  review bot.
