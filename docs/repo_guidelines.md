# GitHub Repository Working Guidelines

## 1. Branch Restrictions

- **Only read and modify the `dev` branch.**
- Do not access, checkout, modify, merge, or push changes to other branches, including `main` and `master`.
- Before starting any work, verify that the active branch is `dev`.
- If the `dev` branch is unavailable, stop and request further instructions.
- Never create or switch to another branch without explicit authorization.

## 2. Understand Before Modifying

- Read the relevant documentation, particularly `AGENTS.md` and `README.md`, before making changes.
- Examine the existing implementation, dependencies, and tests related to the requested task.
- Follow established project conventions, architecture, and coding guidelines.
- Preserve existing functionality unless modifications are explicitly requested.
- Avoid making assumptions when requirements or implementation details are unclear.

## 3. Small and Incremental Changes

- **Make changes incrementally, keeping each modification small and focused.**
- Break complex tasks into manageable steps.
- Complete and verify one logical change before proceeding to the next.
- Avoid large-scale refactoring unless explicitly requested.
- Do not modify unrelated files or functionality.
- Prefer simple solutions that follow the principles of DRY, KISS, and YAGNI.

## 4. Testing and Verification

- Run relevant tests after each logical change.
- Start with targeted tests before running broader test suites.
- Verify that existing functionality remains intact.
- Address test failures introduced by the changes before proceeding.
- Do not modify or remove existing tests merely to make an implementation pass.
- If testing cannot be completed, clearly document the limitations.

## 5. Commit Discipline

- **Commit each small, completed, and verified logical change separately.**
- Keep commits focused on a single purpose.
- Review the diff before committing.
- Stage only files related to the current change.
- Use clear, descriptive commit messages in English.
- Do not combine unrelated changes into a single commit.
- Do not commit temporary files, credentials, secrets, generated artifacts, or unrelated modifications.
- Never amend, squash, or rewrite existing commits without explicit authorization.

## 6. Push and Remote Safety

- Push changes only to the remote `dev` branch.
- Verify the target branch before pushing.
- Never use force push.
- Do not merge branches or create pull requests unless explicitly requested.
- Do not discard or overwrite existing local or remote changes.
- If local and remote histories conflict, stop and report the situation before taking corrective action.

## 7. Documentation and Code Quality

- Use English for source code, comments, docstrings, tests, and commit messages.
- Update relevant documentation whenever behavior, configuration, or architecture changes.
- Keep documentation consistent with the actual implementation.
- Avoid unnecessary abstractions, dependencies, and code duplication.
- Follow existing naming conventions and directory structures.
- Prioritize readability, maintainability, and backward compatibility.

## 8. Working with Existing Changes

- Check the working tree before modifying files.
- Preserve existing uncommitted changes.
- Never assume that modifications made by another developer can be discarded.
- Avoid staging or committing files unrelated to the current task.
- If existing changes conflict with the requested work, stop and request clarification.

## 9. Recommended Workflow

For every requested task:

1. Verify that the active branch is `dev`.
2. Check the working tree and repository status.
3. Read relevant documentation and source code.
4. Identify the smallest logical change required.
5. Implement the change.
6. Run relevant tests and verify the results.
7. Review the diff for unintended modifications.
8. Stage only the relevant files.
9. Create a descriptive commit.
10. Repeat the process for the next logical change.
11. Push to `dev` only when explicitly authorized.
12. Summarize the completed changes, test results, and commit hashes.

## 10. General Principles

- Never sacrifice repository stability for implementation speed.
- Prefer incremental improvements over extensive rewrites.
- Keep every change traceable, testable, and reversible.
- Respect the existing architecture and project decisions.
- Stop and ask for clarification whenever an operation may cause unintended data loss or affect restricted branches.

**Core Rule: Work exclusively on `dev`, make small changes, verify each change, and commit incrementally.**