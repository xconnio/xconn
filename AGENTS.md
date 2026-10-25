# AGENTS.md

## Working principles
- Understand the relevant code before making changes.
- Make the smallest correct change.
- Follow existing architecture, naming, style, and patterns.
- Avoid unrelated refactors or formatting changes.
- Prefer simple solutions over clever abstractions.
- Add dependencies only when clearly justified.
- Preserve existing behavior unless the task requires changing it.

## Validation
Before finishing:
1. Run relevant tests.
2. Run lint/type checks if configured.
3. Review the diff for regressions and unnecessary changes.

If you cannot run a check, say so.

## Communication
When done, summarize:
- what changed
- what you verified
- any remaining risks or assumptions
