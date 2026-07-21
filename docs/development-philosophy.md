# Development Philosophy

**Status:** Active

**Context:**

Code is read far more often than it is written. We write for the humans who will encounter our work later: the developer debugging at 2 AM, the new hire learning our domain, the reviewer building confidence before approving a change.

The review process is a trust-building mechanism. When a reviewer can understand the change, verify correctness through tests, and see that cases have been considered, they approve with confidence. This confidence compounds into organizational trust and velocity.

**Forces:**

- Cognitive load is expensive: clarity saves time across dozens of future interactions
- Review efficiency multiplies team effectiveness
- Trust enables velocity
- Tests are documentation that cannot lie
- Simple code ages better than clever code
- Abstractions have a cost that must be justified
- Explicit is better than implicit

# Decision

We write code optimized for **the future reader and the present reviewer**, choosing clarity and completeness over cleverness and brevity.

## Write for the Reader

Code, documentation, and git history are all written once but read many times—optimize for the reader, not the writer.

**In code:** Follow Clean Code principles: reveal intent through naming, keep functions focused, mirror domain understanding in structure. Verbosity that aids comprehension is preferable to terseness that obscures meaning. A longer, clearer name beats a short, cryptic one.

**In documentation:** Explain context, decisions, and trade-offs that aren't obvious from code alone.

**In git history:** The developer investigating a bug months from now needs to understand not just what changed, but why. Follow these practices (from [A Note About Git Commit Messages](https://cbea.ms/git-commit)):
- **Structure:** Start with a capitalized, short summary (50 chars or less), followed by a blank line, then detailed explanation wrapped at 72 characters
- **Voice:** Use imperative mood ("Fix bug" not "Fixed bug")—matches git-generated messages
- **Subject line importance:** Tools like `git log --oneline`, `git rebase --interactive`, `gitk`, and GitHub all prominently display the summary. Make it count.
- **Body content:** Explain what and why, not how. The diff shows how. The message should answer: "Why was this change necessary?" and "What problem does it solve?"

The subject/body distinction isn't pedantic—it's what makes `git log`, `git rebase`, and `git blame` actually useful when investigating issues.

**2. Resist Unnecessary Complexity**

Simplicity is a hard-won achievement. Fight complexity at three critical points:

- **Premature abstraction:** When choosing between DRY abstraction and duplication, choose understandability. Duplication is easier to understand and easier to refactor later when patterns become clear. The "Rule of Three" guides us: duplicate twice, _consider_ an abstraction on the third copy.

- **Speculative features:** Add only what's required to solve the current problem. Avoid "future-proofing" abstractions, unused parameters, or speculative flexibility. Every line of code not written is code that doesn't need to be reviewed, tested, maintained, or understood. YAGNI (You Aren't Gonna Need It) keeps us focused on real requirements.

- **Clever solutions:** Straightforward solutions demonstrate wisdom. If code requires deep language knowledge or intricate abstractions to understand, it's too clever. Boring code is good code.

**3. Make the Implicit Explicit**

Avoid hidden behavior, magical conventions, and "action at a distance." If something important happens, it should be visible in the code. Configuration over convention when it aids clarity. Side effects should be obvious. Dependencies should be explicit, not reached for globally.

**4. Keep Functions and Classes Small and Focused**

Small units are easier to understand, test, and modify. Each function should do one thing well (Single Responsibility Principle). When reviewing code, small focused units allow reviewers to understand each piece independently without holding too much context in their head.

**5. Build Trust Through Effective Reviews**

Reviews are trust-building mechanisms. Every pull request contains what reviewers need to build confidence:
- Comprehensive tests demonstrating intended behavior
- Edge cases explicitly handled
- Clear context on why this change exists
- Tests as executable specifications

When reviewers consistently find thorough tests, considered edge cases, and understandable approaches, trust builds. This creates a virtuous cycle: efficient reviews → increased velocity → confidence in the codebase.

**Consequences:**

This investment in clarity and testing pays compounding returns:
- Reviews become collaborative and efficient
- New team members contribute faster
- Incidents are easier to debug
- Refactoring becomes safer when patterns are clear
- Velocity increases as trust builds

We accept longer initial development time as the cost of long-term maintainability, celebrating simplicity as a hard-won achievement.

**Notes:**

This is a bias toward simplicity, not a ban on sophisticated patterns. Advanced features and abstractions are welcome when they solve genuinely complex problems that exist today—but the burden of proof lies with complexity to demonstrate its value.

These principles draw from: Clean Code (Martin), A Philosophy of Software Design (Ousterhout), The Pragmatic Programmer (Hunt & Thomas), and Refactoring (Fowler). The common thread: optimize for human comprehension.

The goal: a codebase that any team member can confidently modify, reviewers can confidently approve, and we can all confidently deploy.
