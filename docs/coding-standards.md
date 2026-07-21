# Coding Standards

For the philosophy behind these practices,
see [Development Philosophy](development-philosophy.md).

## Code Style

This project
uses [Airlift's code style](https://github.com/airlift/codestyle/blob/master/IntelliJIdea2019/Airlift.xml).
Import this into IntelliJ to ensure consistent formatting.

## Key Patterns

- **Tests**: See [testing.md](testing.md) for testing patterns and conventions.

## Build & Test

This project uses Maven with the Maven Daemon (`mvnd`) for faster builds.

```
mvnd clean install                                        # full build + tests
mvnd test -pl <module>                                    # test one module
mvnd test -pl <module> -Dtest=TestClassName                # test one class
mvnd test -pl <module> -Dtest=TestClassName#testMethod     # test one method
mvnd clean install -DskipTests                            # build without tests
```

## Code Requirements

- All code should be as easy to review as possible.
- We hold ourselves to a high standard — code should be well written, simple, and maintainable.
- We expect tests to be written for all new functionality and bug fixes. These tests should be
  meaningful and cover both the happy path, plus all edge and corner cases.
- Follow existing code patterns unless there is a good reason to introduce a new one.
- Document any non-obvious design decisions in code comments or design docs.

## Comments

For detailed guidance on comment quality, required empty-catch comments, and when to remove vs keep
comments in complex code, see [comments.md](comments.md).

## Naming

For detailed guidance on variable/method/type naming, abbreviation rules, boolean naming, and
scope-based name clarity, see [naming.md](naming.md).

### Java Specific Standards

- Ban `Optional.get()`. Always use `Optional.orElseThrow()` instead. This makes failure
  cases explicit and provides a clear stack trace when the Optional is empty.
- Use `requireNonNull` for all injected constructor parameters:
  `this.roleApi = requireNonNull(roleApi, "roleApi is null")`.
- Configuration classes use Airlift's `@Config("dot.separated.kebab-case-key")` with fluent setters
  that return `this`. Mark sensitive values with `@ConfigSecuritySensitive`.

This list will grow as patterns are codified across the codebase.
