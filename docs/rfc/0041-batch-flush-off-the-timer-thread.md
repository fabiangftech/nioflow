# RFC 0041 — Keep the batch group lock off the shared TimerWheel thread

- **Status**: ✅ Implemented — option 1 (stage the flush to a worker)
- **Target**: `core` (`BatchGroup` — the window-flush path)
- **Depends on**: RFC 0025 (cancel off the timer thread — the same "nothing slow on the shared timer thread" principle)
- **Severity**: **Low-Medium** — a lock-coupling between batch windows and *every* timeout/window in the JVM; the critical section is short, so the impact is contention, not a stall
- **Realized by**: having the timer action only *stage* a window flush (enqueue to a worker) and never acquire the `BatchGroup` lock on the timer thread — do the generation check and the ref-swap on the worker.

## The finding

`TimerWheel.fireDue` runs `timeout.action.run()` on the single shared timer thread (`TimerWheel.java:138`). For a batch window, that action is `flushWindow` (`DefaultNioEngine.java:972`), which takes `synchronized (this)` on the `BatchGroup` to check the generation and swap out the accumulated values/continuations (`:975-982`). That same lock is taken by `add()` on **boss threads** when an execution enters the batch (`:953`).

So a boss adding to a batch and the timer thread flushing its window contend on the group lock. The critical sections are short on both sides — the actual bulk dispatch (`dispatchBulk` → `workersExecutorService.execute`, `:983`/`:997`) is already **outside** the lock, which is good — but the coupling is real: the one thread that ticks **every** timeout and batch window in the JVM briefly blocks on a lock held by a boss. `TimerWheel`'s own doc insists "anything slow on it stalls all of them," and while this critical section is not *slow*, it is the one place the timer thread waits on a lock another thread class also takes.

## Why it matters

The TimerWheel is a deliberately single-threaded, O(1)-schedule structure precisely so that no per-timer work can degrade unrelated timeouts. RFC 0025 already moved *cancellation teardown* off this thread for exactly this reason. The batch-window flush is the remaining case where the timer thread can wait on contended state: under heavy batching, group-lock contention on the timer thread adds jitter to every *unrelated* stage timeout and batch window scheduled in the JVM. It is a small effect today, but it is the same class of coupling RFC 0025 judged worth removing, and the fix keeps the "the timer thread only stages, never works or waits" invariant total.

## The options

1. **Timer action only stages the flush (recommended).** On window expiry, the timer thread does the absolute minimum — enqueue a "flush group G, generation N" task to the worker pool — and acquires **no** group lock. The worker then takes `synchronized (this)`, re-checks the generation (a size flush may have already taken the batch), swaps, and dispatches the bulk. This moves the only lock the timer thread takes off the timer thread entirely; the generation check already makes a late/duplicate flush a no-op, so correctness is unchanged.

2. **Lock-free generation check (heavier).** Replace the group's `synchronized` with a CAS on a generation/state word so neither the boss `add` nor the flush needs a monitor. Removes the lock everywhere, not just off the timer thread, but it is a real rewrite of the batch coalescing state machine — more risk than the problem warrants.

3. **Do nothing but document it (minimum).** Note in the `TimerWheel`/batch docs that a window flush briefly takes the group lock on the timer thread, so extremely high batch churn can add timer jitter. Honest, but it leaves the one exception to the timer-thread invariant standing.

Recommended: **option 1** — it restores the invariant with a small, local change and no semantic risk (the generation re-check already handles the staged/duplicate case).

## Testing

- A test that a window flush produces identical results whether staged (option 1) or inline (today): each batched caller gets its own element, a size flush that beat the window is a no-op, wrong-sized bulk fails every member — all unchanged.
- A concurrency test: many boss threads `add`-ing to the same batch while windows fire; assert no lost/duplicated flush and that the timer thread never blocks on the group lock (e.g. instrument that `flushWindow`'s `synchronized` is entered only on worker threads).
- RFC 0021 gates: batch throughput/allocation flat or better; the timer thread's per-tick cost must not rise.

## Risks

- **Staging adds one worker hop per window flush.** Negligible (a window flush is already a cold, coalesced event, and the bulk dispatch was already a worker hop), and it buys the timer thread's independence.
- **Generation re-check must run on the worker now.** It already exists inside `flushWindow`; option 1 just moves *where* the lock is taken, keeping the check. Ensure the staged task carries the expected generation so a size-flush-then-window-flush race stays a no-op.
- **Shutdown interaction:** a window staged to the workers just as they shut down must still fail its parked members (the existing `RejectedExecutionException` path at `:1000` handles this) — keep that fallback on the staged path.

## Results

Shipped option 1. No hot-path change (the size-triggered flush — the benchmarked
path — is untouched; only the cold window flush gains a stage), and the batch
benchmark stayed in range.

- **The timer thread stages, and takes no group lock.** The window timer now
  schedules `() -> stageWindowFlush(gen)`, which does the minimum on the shared
  TimerWheel thread — one `workersExecutorService.execute(...)` — and acquires no
  `BatchGroup` lock. So a boss inside `add()` (which does take the group lock)
  never contends the one thread that ticks every timeout and window in the JVM.

- **The worker does the lock + swap + bulk, in one hop.** The staged
  `flushWindow` runs on a worker: it takes `synchronized (this)`, re-checks the
  generation (a size flush that beat the window is still a no-op), swaps and
  resets, and runs the bulk inline — no second dispatch hop, since it is already
  on a worker.

- **Shutdown is handled on the staged path.** If the stage `execute(...)` is
  rejected because the workers are gone (a window firing exactly at shutdown),
  `failWindow` swaps under the lock — on the timer thread, but nothing contends
  now that admission is closed — and fails the parked members, so none hangs. The
  RFC's own risk note called for keeping this fallback; it is `failWindow`.

- **Tests:** the existing `DefaultNioFlowBatchTest` (size trigger, window trigger,
  per-element downstream, bulk failure, wrong-sized result, lane pooling,
  consecutive reuse) stays green through the new staged path, and a new
  `aWindowFlushRunsOffTheSharedTimerThread` guards the invariant — a window flush
  runs its bulk (and, now, its group-lock swap) on a worker, never on
  `nio-flow-timer`. SonarLint over the main-source diff is clean.

- **Not taken: option 2** (a lock-free CAS generation word) — a real rewrite of the
  coalescing state machine for no gain once the lock is off the timer thread; the
  short critical section on the boss/worker is fine.
