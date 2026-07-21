# Testing

This document outlines the testing guidelines.

## Tooling

- **JUnit 5**: Primary testing framework for unit and integration tests.
- **AssertJ**: Fluent assertions for improved readability in tests.
- **Testcontainers**: For integration tests requiring ephemeral databases and services.

## Best Practices

- **Comprehensive Coverage**: Ensure all new features and bug fixes have corresponding tests.
- **Edge Cases**: Tests should cover not only the happy path but also edge and corner cases.
- **Naming Conventions**: Follow consistent naming conventions for test classes and methods to
  enhance readability. To that end, test classes all should be named `Test*.java`, abstract tests
  should be named `Abstract*Test.java`, and testing helper classes should be named `Testing*.java`.
  Test methods should use descriptive CamelCase names (NO UNDERSCORES) that indicate the
  scenario being tested.

  Examples from the codebase:
  - `TestArgMaxMinFunctions` — test class
  - `testUpsertScheduleConfigurationWithNewConfiguration` — descriptive test method
  - `testArgMaxMinWithAllNulls` — edge case test method

See also [coding-standards.md](coding-standards.md) and [git.md](git.md) for additional guidelines.
