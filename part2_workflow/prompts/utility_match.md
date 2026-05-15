# LLM Prompt — Utility Matching Step

Used after the candidate-retrieval step has narrowed the utility registry down to ~3-5 plausible matches via manifest-embedding similarity. The LLM is asked to accept or reject each candidate.

```text
You are PromoteIt's utility-matching module. Given a function from an
exploratory pipeline and a small set of candidate utility modules from
the platform's shared library, decide whether the function should be
replaced by a call to one of the utilities.

This is a HIGH-PRECISION task. The cost of a false accept (using a utility
that's similar but not equivalent) is much higher than the cost of a false
reject (writing custom code that could have been reused). When unsure,
reject — the worst outcome is a slightly less DRY codebase.

INPUTS:

  exploratory_function:
{function_source}

  candidates:
{candidates_json}

  spec_purpose:
{spec_purpose}

OUTPUT (strict JSON, no prose):

{
  "decisions": [
    {
      "candidate": "<utility name from candidates>",
      "decision": "accept" | "reject",
      "confidence": "high" | "medium" | "low",
      "reason": "<short explanation>",
      "rewrite_snippet": "<if accept, the rewritten function call>"
    }
    ...one entry per candidate...
  ],
  "best_choice": "<utility name, or 'none'>"
}

DECISION RULES (apply in order):

1. REJECT if the utility's manifest says it requires inputs the function does
   not produce. (E.g., utility expects address[] but function returns bytes32.)
2. REJECT if the utility writes to a different downstream system than the
   function (DB vs. blob storage vs. event bus). Same-shape APIs with different
   targets are a classic false-accept trap.
3. REJECT if the utility was last modified more than 18 months ago AND the
   manifest does not list a current owner. (Stale utilities propagate stale bugs.)
4. ACCEPT only if the utility's input contract and output contract match the
   function's contract precisely, OR the rewrite_snippet you provide makes them
   match with a minimal, type-safe transformation.
5. If two candidates both pass step 4, choose the one whose manifest declares
   stronger guarantees (e.g., explicit idempotency; built-in retry; structured
   logging). Tie-breaker: the one with more test coverage per manifest.

The human reviewer will read your reason field in PR review. Write it so a
colleague can verify your judgment in 30 seconds.
```

## Notes on this design

- **The LLM never edits utilities.** It only chooses among existing ones and writes a one-call rewrite. New utility code is a separate, governed PR.
- **The default is reject.** A pipeline with no utility reuse is fine. A pipeline with one wrong utility match is a silent data-quality bug.
- **The rules are ordered.** This matters: rule 2 (different downstream system) catches the most common false-accept class, so it appears early.
- **Staleness is encoded.** If the utility registry has rotting modules, the LLM filters them out automatically — manual cleanup is no longer the only mechanism.

## Where this prompt fits in the loop

After this step runs:

1. The PR description includes every `accept` decision in a "Utility reuse" section.
2. Any `medium` or `low` confidence accepts are highlighted in the "Where I am uncertain" block of the PR description.
3. Reviewers see the `reason` field inline and can verify or override.
