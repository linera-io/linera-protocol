# Spec: `ChainHistory` — separating sparse-chain bookkeeping from checkpoint logic

Baseline: `main` @ `003d925a00` (includes the full checkpoint series through #6609, #6613, #6616).
Open PRs mapped by this design: #6599, #6614.
Revision 2 — incorporates the findings of the adversarial spec review (`spec-review-findings.json`).

## 1. Problem

Checkpoints changed a foundational invariant. A chain's local knowledge always had two
regimes: the *executed* range `[0, tip_state.next_block_height)`, which was dense —
every height in it was processed by this node — and the *preprocessed* set above the
tip, which was always sparse (a tracking client fetches and preprocesses only sender
blocks, leaving gaps — and since #6328 it preprocesses sender blocks even when they
are contiguous, so the executed range on such chains simply never grows). Code below
the tip could therefore assume density: every anchor resolved, and "height below tip"
meant "already seen". A checkpoint-bootstrapped node breaks exactly that half: its
state is a snapshot plus a few vouched-for blocks, so heights below the tip may be
entirely unknown — the below-tip range is now sparse too. And once #6599 lands, *every*
node that executes a checkpoint prunes its below-tip history, so below-tip sparseness
becomes the norm on all checkpointing chains, not a bootstrap special case.

That *sparseness* has no owner in the code. Each consumer of the old invariant was
patched individually, and each unpatched consumer is a latent bug. The recent review
cycle on #6599/#6614 found the same defect wearing five costumes:

- `process_confirmed_block` treated below-tip as already-processed and silently
  discarded certificates a bootstrapped node had never seen (fixed ad hoc in #6614's
  `258e1379`, which itself left `preprocess_block`'s below-tip guard as a silent no-op
  — the pushed block's hash was never recorded, wedging later anchor resolution;
  demonstrated by the pushed-expired-blob review test, see section 8).
- `previous_message_blocks` anchor resolution errors with `CorruptedChainState` when
  the anchor's height is legitimately absent on a sparse chain (#6599 prevents this
  *write-side* by pruning fully-acknowledged anchors at checkpoint time; the read
  sites keep their hard error — see section 3 for why that is correct).
- `process_outgoing_messages` needed a missing-entry tolerance (#6599), trading away a
  genuine corruption tripwire because the code cannot distinguish "forgotten because
  sparse" from "missing because corrupt".
- `handle_revert_confirm` walks body links through `block_hashes` and hard-errors on
  the first pruned/unknown height.
- `linera chain show-block` indexes `[0]` into a lookup that is empty for any height a
  sparse chain does not know.

Root cause in one sentence: **"do we know block N?" is answered by ad-hoc tip
comparisons scattered across two crates, and tip comparisons stopped answering that
question when checkpoints landed.**

## 2. Design overview

Give sparseness a single authority and a vocabulary, so checkpoint-agnostic code never
mentions checkpoints and checkpoint code lives in named modules.

Four pieces:

| Piece | Crate | Replaces |
|---|---|---|
| `ChainHistory` facade | linera-chain | scattered `block_hashes` / tip reasoning |
| `Ingest` classification + `record_history` | linera-core worker | the gap × `starts_with_checkpoint` legs of the dispatch (mode stays a caller input) |
| `chain_worker/bootstrap.rs` | linera-core | checkpoint-restore logic inline in `state.rs` |
| checkpoint hooks module + `Recency` type | linera-execution | rotation/pruning calls inline in generic code |

## 3. `ChainHistory` (linera-chain)

A logical facade owning the question "what does this node know about this chain's
blocks", over the existing fields — `block_hashes`,
`next_height_to_preprocess` (chain.rs:335), `latest_checkpoint_height` (chain.rs:340),
`pre_checkpoint_block_trust` (chain.rs:349) — **without physically re-nesting the
views** (re-nesting would change key prefixes, which `linera-views-derive` assigns by
field position, and force a state migration; the facade is an API boundary, not a
storage change).

### Shape (a real design decision, not a detail)

`ChainHistory` cannot be a field-owning struct: `execute_block_inner` exists precisely
to split-borrow `&self.block_hashes` away from `&mut self.execution_state`, and other
hot paths do the same. The facade is therefore two things:

1. an `impl` block of methods on `ChainStateView` for the common `&mut self` callers,
   and
2. a borrow-projection struct (`ChainHistoryRef<'a>`), constructed per call from
   exactly the fields it needs (`&block_hashes`, `&tip_state`,
   `&latest_checkpoint_height`, `&pre_checkpoint_block_trust`), passed where
   `execute_block_inner`-style split borrows are required.

"Sole write path" is a convention, not a compiler guarantee, while the underlying
fields remain `pub` for GraphQL/state inspection; step 1 narrows visibility where
possible (`pub(crate)` + read-only accessors for GraphQL) and the remaining direct
writes are review-banned.

### API

Methods are `async` and the hot paths are batched — the sketch shows the logical
contract, not final signatures (`get_many(heights) -> Vec<Lookup>` mirrors today's
`multi_get`):

```rust
pub enum Lookup {
    Known(CryptoHash), // we have this block (executed or preprocessed, any height)
    Pruned,            // absent below lowest(): legitimate on any checkpointed chain
    Missing,           // absent inside [lowest(), tip): guaranteed range, must exist
    Future,            // absent at or above tip: normal lookahead sparseness
}

impl ChainHistory {
    /// Sole write path (kills the direct-insert bypass in the bootstrap path).
    async fn store(&mut self, height, hash) -> Result<()>;
    async fn get(&self, height) -> Lookup;
    async fn get_many(&self, heights) -> Vec<Lookup>;
    /// Floor of the guaranteed-known executed range:
    /// `latest_checkpoint_height` if the chain has ever applied a checkpoint
    /// (whether it executed it from genesis or bootstrapped from it), else 0.
    /// The SAME rule for every node type — once #6599 lands, every node prunes
    /// at every checkpoint, so a genesis-executed chain's floor rises exactly
    /// like a bootstrapped one's.
    fn lowest(&self) -> BlockHeight;
    /// Trust-mark: this below-lowest hash is vouched for by a checkpoint.
    /// A mark is consumed (cleared) when the certificate it names is stored —
    /// i.e. inside `record_history` — mirroring today's removal at the top of
    /// `process_confirmed_block`.
    fn expect(&mut self, hash);
    fn is_expected(&self, hash) -> bool;
}
```

Two deliberate properties of the partition:

- **`Known` below `lowest()` is legal and node-divergent.** #6599's retention keeps
  vouched-for hashes (consensus data) *and* locally-queued outbox heights (node-local)
  below the checkpoint; `block_hashes` is not consensus-hashed, so honest nodes
  legitimately disagree about which below-floor heights are `Known`. Only the
  *guarantee boundary* (`lowest()`) is uniform.
- **`Pruned` and `Missing` split what is one `None` today.** Every caller either
  trusts blindly (`[0]` panic in the CLI) or distrusts blindly
  (`CorruptedChainState`). With the split, each consumer states a policy — and the
  policy must cover the `Future` arm too, which is the live arm on tracking clients
  whose tip never advances.

Per-consumer policies:

- **Block-end anchor / `previous_event_blocks` resolution (`execute_block_inner`):
  `Pruned` and `Missing` are BOTH hard errors.** This site writes consensus-visible
  block-body content, so it cannot take node-local tolerance decisions. It never
  needs to: #6599 maintains the invariant *write-side* — `prune_message_anchors`
  drops fully-acknowledged anchors at checkpoint time and every surviving anchor
  points at a vouched (hence `Known`) height, and `previous_event_blocks` is cleared
  outright at each checkpoint. A `Pruned` result here means the invariant is broken,
  which is exactly what a tripwire is for. (Revision note: the previous draft
  wrongly prescribed tolerance here and attributed it to the PRs.)
- `process_outgoing_messages` predecessor lookup (the one site #6599 actually
  relaxed): `Pruned` → schedule with no predecessor; `Missing` → corruption error.
  Strictly better than #6599's unconditional tolerance — the tripwire survives
  inside `[lowest(), tip)`.
- `handle_revert_confirm`: `Pruned` terminates the walk gracefully (everything below
  is acknowledged history) instead of aborting retransmission; `Missing` → error.
- `collect_unfinalized_block_hashes` (checkpoint construction): **never tolerant** —
  every unfinalized height must be `Known` by the vouching invariant; anything else
  is a hard error on all arms.
- Certificate-by-height serving and the CLI: `Pruned` *and* `Future` → clean "not
  available here" (the CLI panic reproduces via `Future` on tracking clients, not
  only via `Pruned`); `Missing` → error. Note the simple-protocol proxy fix this
  enables is *not* one-line: it needs a response that distinguishes "unavailable"
  from "short list" plus requester-side handling (`RemoteNode` currently converts
  short responses into `MissingCertificatesByHeights`) — an RPC-semantics change
  scoped in step 2.

## 4. Worker ingest classification (linera-core)

`process_confirmed_block` (state.rs:935) currently interleaves trust-marks, a tip
comparison patched with two containment checks, blob provisioning, and a
mode × gap × `starts_with_checkpoint` dispatch. The refactor replaces the *history*
legs of that dispatch. **`ProcessConfirmedBlockMode` remains a caller input**: whether
a contiguous block is executed or preprocessed is caller intent (a tracking client
must never execute; an `Execute` caller relies on `InvalidBlockChaining` for gaps) and
cannot be derived from history. The classification is `f(mode, history)`:

```rust
enum Ingest {
    AlreadyKnown,          // history.get(h) == Known
    NextAtTip,             // contiguous: execute (Auto/Execute) or preprocess (Preprocess)
    OutOfBand,             // below tip, not Known (incl. trust-marked): record_history
    GapAhead,              // above tip + gap: preprocess; bootstrap if checkpoint (Auto);
                           //   InvalidBlockChaining (Execute)
}
```

`AlreadyKnown` is decided by **history alone**. A certificate present in storage whose
height is *not* `Known` to history (pre-fix data, or a crash between the storage write
and the chain save) must still go through `record_history` — which is idempotent — or
the `258e1379` wedge resurrects through the storage-only state. (Revision note: the
previous draft's "or cert in storage" disjunct is exactly that bug.)

`OutOfBand` maps to a new first-class operation:

```text
record_history(cert):
    consume trust-mark if present (history.expect -> cleared)
    verify certificate            (committee_for_epoch resolves even revoked epochs
                                   via the admin event stream, so vouched
                                   pre-checkpoint certs from superseded epochs verify)
    write blobs + certificate + blob_state     (already done today)
    history.store(height, hash)                (the step 258e1379 misses)
    NO outbox scheduling, NO event bookkeeping
```

Today this operation exists only as an accidental subset of `preprocess_block`
(chain.rs:1531), whose below-tip early-return makes it a silent no-op — the root cause
of the wedge found in review. Naming it makes the invariant structural:
*recording history ≠ replaying it.* `preprocess_block` reverts to its original meaning
(at-or-above-tip lookahead only); its below-tip guard becomes a **logged no-op that
still performs `history.store`** — not a `debug_assert`, which would turn a missed
route into release-mode double-processing.

`GapAhead` with a checkpoint block delegates to a new `chain_worker/bootstrap.rs`,
built around two named concepts:

- **`Snapshot`** — the certified content of a checkpoint: the execution-state dump
  blobs, the vouched-for block hashes, the live `used_blobs` list, and the inbox
  cursors. One type, constructed from the checkpoint's oracle response; today these
  travel as loose fields.
- **Seeding** — deriving the node-local structures from a `Snapshot`: outboxes from
  `unfinalized_message_blocks` (chain.rs:1403), inbox `restored_cursor`s, trust marks
  (`history.expect`), and the history `lowest()`.

Below-tip state arises in exactly two ways without execution — Seeding (derived
bookkeeping) and `record_history` (certified content plus the height→hash fact,
never derived bookkeeping). No third way may exist.

The module owns everything restore-related that is currently inline in `state.rs`
(`execute_block_with_checkpoint_restore` and its helpers): it installs the `Snapshot`,
seeds, and re-enters the generic pipeline as a plain `NextAtTip`. `state.rs` keeps
zero checkpoint knowledge.

## 5. Execution-side hooks (linera-execution)

`prepare_checkpoint` / `apply_checkpoint` are already fairly contained; finish the
job by moving the maintenance they trigger into one `execution/checkpoint.rs`:

- `used_blobs` generation rotation (#6614's `start_checkpoint_generation`),
- anchor pruning (#6599's `prune_message_anchors`),
- `previous_event_blocks` clearing / event summarization glue.

Generic code keeps only its own invariant, parameterized so it never names
checkpoints. Concretely for #6614: a small `Recency` value type owns
`is_recent(last_used, current)` and `retain_recent(...)` in one place (removing the
`checked_add` vs `saturating_add` duplication), fed by a generation counter it does
not interpret.

### Checkpoint-purposed state and who maintains it

An audit of the state structs (baseline `main@003d925a00`) found exactly five fields
that exist *purely* for checkpoint tracking and restore:

- `ChainStateView::latest_checkpoint_height` and
  `ChainStateView::pre_checkpoint_block_trust` — node-local; both move behind
  `ChainHistory` (section 3).
- `InboxStateView::restored_cursor` — node-local; inert (`Cursor::default()`) on a
  never-bootstrapped chain; becomes part of Seeding's inventory (section 4).
- `SystemExecutionStateView::unfinalized_message_blocks` and
  `SystemExecutionStateView::pending_checkpoint_ack_targets` — consensus state.

The last two expose the sharpest form of the layering problem: they are
checkpoint-*purposed* but maintained by *generic* code at three sites — the block-end
hook updates `unfinalized_message_blocks` for every **non-`CheckpointAck`** send
(chain.rs:1136-1151), the block tracker inserts into `pending_checkpoint_ack_targets`
for every **non-`CheckpointAck`** receive (block_tracker.rs:325), and the
`CheckpointAck` receive handler trims `unfinalized_message_blocks` cursors
(system.rs:931-950). The `CheckpointAck` exclusions are load-bearing — they are what
terminates the ack ping-pong — so the hooks must preserve them. Every chain pays this
bookkeeping on every block whether or not it ever checkpoints. The fields cannot move
out of the hashed execution state (consensus), but their *maintenance* becomes named
duties of the checkpoint-hooks module — `checkpoint::track_send`,
`checkpoint::track_receive`, `checkpoint::track_ack` — instead of open-coded map
updates, making the cost visible and the coupling one-directional. (Feasibility
verified: both writing crates already depend on linera-execution; no dependency
cycle.)

Borderline fields, documented as such rather than moved: `StreamCounts::first_index`
(exists because checkpoints prune events — #6503 — but shares its struct with the
generic `next_index`), `chain_initialized_at` (reset-loop guard; reset usually
restores from a checkpoint but is defined without one), and `used_blobs` (generic on
`main`; checkpoint-generational once #6614 lands, at which point its rotation is
already a hook above).

## 6. Where the open PRs land

**#6599 (prune `block_hashes` at every checkpoint):**
- The pruning itself becomes `ChainHistory::forget_below(...)` — a history-owned
  operation with the retention predicate (vouched ∪ queued) passed in. It also
  raises `lowest()`; the two must update atomically.
- Its `process_outgoing_messages` tolerance is superseded by the `Pruned`/`Missing`
  policy at that site (section 3) — strictly better: keeps the corruption tripwire
  inside `[lowest(), tip)`, formalizing the review's "keep the error when
  `height >= latest_checkpoint_height`" suggestion.
- `prune_message_anchors` moves into the execution checkpoint-hooks module untouched.
  It is what keeps the block-end anchor sites' hard errors correct (section 3).
- Known open issues that reduce to `Pruned`/`Future` policy decisions: the
  revert-confirm walk and the CLI panic. The simple-proxy under-serving needs the
  RPC-semantics change scoped in section 3, not a one-line policy.

**#6614 (`used_blobs` per checkpoint generation):**
- Generation rotation moves behind the execution hooks; `Recency` owns the predicate.
- The `258e1379` below-tip fix is superseded by `OutOfBand → record_history`
  (classification by history alone — see section 4's `AlreadyKnown` note), which
  structurally includes the missing `history.store`.

**Explicitly out of scope for this refactor** (protocol-level findings from the same
reviews that `ChainHistory` does not and cannot fix; they must be resolved on their
PRs or in follow-ups, and this spec must not be read as addressing them):

- The receiver-side inbox-cursor poisoning wedge: `previous_height=None` bundles
  (routine once #6599 prunes anchors) skip the receiver's gap check; a lagging
  replaying node can irreversibly advance `next_cursor_to_add` and later hard-fail a
  confirmed block with `UnexpectedBundle`. Receiver-side logic, untouched here.
- The forgotten-blob availability contract of #6614 (proposal-time re-provisioning
  fails with `UnexpectedBlob` — proven by the review's e2e test — plus the
  `ApplicationDescription` no-republish-path, `list_applications` robustness,
  admin-chain growth, and first-checkpoint block-size items).
- Preprocess-only tracking nodes never prune: `forget_below` runs at checkpoint
  *execution*, which a preprocess-only replica never performs — the unbounded-growth
  motivation of #6599/#6614 persists on that path. Follow-up design needed
  (out of scope here because it requires deciding whether preprocessed checkpoints
  may be trusted for pruning).

## 7. Migration plan (each step lands independently)

0. **Commit the review regression tests.** The three tests cited in section 8 exist
   today only as uncommitted review-session artifacts; land them on the #6614 branch
   (they target its head) before any refactor step, so the gate below is real.
1. **Facade extraction.** Introduce the `ChainHistory` impl-block + `ChainHistoryRef`
   projection over the existing fields (section 3 "Shape"); mechanical;
   `insert_block_hash` (chain.rs:561) becomes `store`; the bootstrap path's direct
   `block_hashes.insert` goes through it; visibility narrowed where GraphQL allows.
   Behavior-identical.
2. **Lookup migration.** Move the height→hash consumers onto `get`/`get_many` with
   the per-site policies of section 3. **Requires a call-site inventory first** —
   the known consumers are: block-end anchor/event resolution (both hard-error),
   `process_outgoing_messages` predecessor, `collect_unfinalized_block_hashes`
   (never tolerant), `handle_revert_confirm`, `block_hashes_for_heights` servers
   (worker query, gRPC vs simple proxy — the RPC-semantics change is its own PR),
   `get_preprocessed_block_hashes`, `reset_and_reexecute_chain`, the CLI, and
   GraphQL exposure; the inventory must be re-derived at implementation time and
   any site not in this list treated as a finding. Behavior changes only at the
   documented-defect sites; everything else is mechanical.
3. **Worker classification.** `Ingest` as `f(mode, history)` + `record_history` +
   extraction of `bootstrap.rs`. Highest-risk step (hottest path).
4. **Execution hooks + `Recency`.** Mechanical moves.

Constraints:
- No physical re-grouping of view fields anywhere (key prefixes are positional).
- Any NEW persisted field (e.g. an explicit `lowest` register, open question 2) must
  be appended after all existing fields — prefix safety is positional.
- No behavior change intended in steps 1 and 4; steps 2–3 change behavior only where
  today's behavior is one of the documented defects.

Sequencing vs the open PRs: land #6599 and #6614 first; both are given destinations
above, and refactoring underneath two active branches multiplies rebase cost for no
benefit.

## 8. Testing

- **Regression harness (must pass unchanged after every step).** Existing on refs:
  the checkpoint client tests (`test_checkpoint_ack_cycle_terminates` — also pins
  the message-lane behavior step 2 re-touches —
  `test_proposal_pushes_checkpoint_to_lagging_validator`,
  `test_checkpoint_reset_from_latest_checkpoint`, the checkpoint-comprehensive wasm
  pair) and #6614's `test_memory_app_call_after_checkpoint_bootstrap_when_expired`.
  Existing only as review artifacts, to be committed in step 0: the data-blob
  listed/expired pair and the pushed-expired-blob-with-messages test (the one that
  caught the `258e1379` hash-recording no-op).
- **New tests demanded by this spec** (step 3 gate; the full suite + e2e matrix
  alone already missed the `258e1379` no-op once, so these are named requirements):
  1. `record_history` postcondition test: records history + blobs + cert, schedules
     no outbox entry, books no event, consumes the trust-mark.
  2. Below-tip certificate with cross-chain messages on a bootstrapped validator
     (port of the review wedge test) — proves anchor resolution heals and nothing
     double-delivers.
  3. Mode × classification matrix test: a tracking client's contiguous sender block
     stays preprocessed (never executes); an `Execute` caller with a gap still gets
     `InvalidBlockChaining`.
  4. Storage-without-history healing test: cert present in storage, height not
     `Known` → `record_history` still runs and stores the hash (the `AlreadyKnown`
     regression).
- **New unit surface:** `ChainHistory` is small enough to unit-test directly
  (genesis-executed chain pre- and post-first-checkpoint, preprocess-sparse tracking
  chain, bootstrapped chain, trust-marked hash, `forget_below` + `lowest` atomicity)
  — coverage that today requires a four-validator client test.

## 9. Open questions

1. Trust-marks: folded into `ChainHistory` here (`expect`/`is_expected`, consumed in
   `record_history`) on the view that they are history statements. Alternative: keep
   them worker-side as transport concerns. Decide at step 1.
2. Should `lowest()` be persisted explicitly instead of derived (survives the
   vouched-set pruning of #6599 more obviously)? Leaning yes — one register write at
   bootstrap/checkpoint time; must be appended last for prefix safety (section 7).
3. Does the GraphQL/service layer want `Pruned`/`Future` surfaced (e.g. in
   `chain.block_hashes` queries), or mapped to null? Product call, low stakes.
4. Naming bikeshed: `ChainHistory` / `lowest` / `Pruned` / `record_history` (vs
   `store_history`, to rhyme with `store`) are placeholders for review.
