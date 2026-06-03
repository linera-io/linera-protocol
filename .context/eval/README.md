# Context-system evaluation harness

The ship-gate for `.context/` + `company-context`. Inherited from issue **#6216**: we
measure precision, not vibes. **Imprecise retrieval is worse than no retrieval.**

## Procedure
Each task in [`tasks.md`](./tasks.md) is run twice by a fresh agent:
- **OFF:** no `.context/`, no `company-context` MCP (today's baseline).
- **ON:** with the `CLAUDE.md` hint + `company-context` available.

Metrics per task:
- `tokens_to_first_useful_action` (orientation cost)
- `total_session_tokens`
- `correctness` (did it produce a correct, mergeable change / answer?)

## Ship-gate (do not ship if unmet)
- **≥20% total-session-token reduction on ≥6/10 tasks**, AND
- **no correctness regression** on any task.

Calibrated against the AGENTS.md empirical study (#6216 references): observed 17–29%;
20% is "real but modest," 30%+ is "convincing." If we miss the gate, we **diagnose the
content** before adding any layer (vectors, graph) on top.

## Important
Run the ON arm *with* the `CLAUDE.md` hint that points at `company-context`. Otherwise we
measure whether agents *can* use the system, not whether they *do* (Claude Code defaults
to grep-and-read).
