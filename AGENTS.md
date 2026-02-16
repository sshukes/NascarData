# Codex Working Rules

- Make small focused changes and avoid unrelated file edits.
- Keep script interfaces stable after adding/changing options.
- Print clear status lines (`[STATUS]`, `[OK]`, `[WARN]`, `[ERROR]`).
- Exit non-zero on failures for validation/training orchestration steps.
- Prefer simple, readable code over clever code.
- Ensure scikit-only environments work end-to-end without optional dependencies.
- Before finishing, verify acceptance checklist commands run and outputs are generated.
