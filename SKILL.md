---
name: garmin-golf
description: Use this skill when working with Garmin golf statistics from the local garmin-golf project, especially to run the CLI, inspect round or course history, compare date windows, and identify practice priorities from stored golf data.
---

# Garmin Golf Stats

## Overview

Use this skill for questions about the local `garmin-golf` dataset and CLI. Prefer running the existing stats commands and interpreting their output before reading or changing Python code.

## When To Use It

- Analyze overall trends from the local Garmin golf dataset
- Compare rounds or courses over a date window
- Inspect one round by `round_id`
- Find practice priorities with `stats practice-focus`
- Analyze putting outcomes by starting distance with `stats putting`
- Review second-shot club usage with `stats second-shots`
- Inspect inferred and configured club labels with `stats clubs`
- Review rolling form with `stats trends`
- Break down club performance by usage context with `stats clubs --by-context`
- Explain golf metrics already exposed by the CLI

Do not start by editing the project. Only switch to source inspection when the
user asks to extend or debug the implementation itself.

## Workflow

1. Work from the current directory.
2. Start with the narrowest command that answers the question.
3. Prefer `--json` when the consumer is an AI agent or another script.
4. Use `--period` or `--from/--to` when the user asks for a time window.
5. Summarize the key trends instead of dumping raw tables back to the user.

If the CLI reports that no local rounds are available, explain that the dataset
has not been mirrored yet. Only move into the browser mirroring workflow if the
user asks for ingestion help.

## Core Commands

### Summary and trends

```bash
uv run garmin-golf stats summary --json
uv run garmin-golf stats summary --period last-12-months --json
uv run garmin-golf stats summary --from 2025-01-01 --to 2025-12-31 --json
uv run garmin-golf stats trends --window 5 --json
uv run garmin-golf stats trends --window 10 --period last-12-months --json
uv run garmin-golf stats trends --metric gir_pct --window 5 --json
```

Use `stats summary` for overall scoring, GIR, FIR/fairway hit, scrambling, putting,
penalties, and shot-distance metrics. Use `stats trends` when the user wants
rolling form over the last 5, 10, or 20 rounds, including deltas versus the
previous window. Use `stats trends --metric ... --json` when the user wants one
metric series for graphing or focused analysis. Supported metrics are
`average_to_par`, `gir_pct`, `fir_pct`, `scrambling_pct`, `three_putts_per_18`,
and `penalties_per_18`.

### Putting analysis

```bash
uv run garmin-golf stats putting --json
uv run garmin-golf stats putting --period last-12-months --json
```

Use `stats putting` when the user wants putting performance broken down like the
distance-based views. It groups holes by first-putt starting distance bucket and
reports hole counts plus one-putt, two-putt, and three-putt-or-worse rates.

### Practice priorities

```bash
uv run garmin-golf stats practice-focus --json
uv run garmin-golf stats practice-focus --period last-12-months --json
```

Use when the user asks what to practice next, where strokes are leaking, or
which area offers the best scoring return. The output ranks the top three
practice priorities and estimates strokes that could be saved per 18 holes.

### Round lookup

```bash
uv run garmin-golf stats rounds --json
uv run garmin-golf stats round --round-id 22068626916 --json
```

Run `stats rounds` first when the user does not know the round id. `stats round` now returns the round summary, a hole-by-hole table, round club usage, and par-4/par-5 second-shot breakdowns when shot data is available.

### Shot and club analysis

```bash
uv run garmin-golf stats second-shots --json
uv run garmin-golf stats second-shots --period last-12-months --json
uv run garmin-golf stats clubs --json
uv run garmin-golf stats clubs --by-context --json
uv run garmin-golf stats clubs --course "Golf National ~ Aigle" --json
uv run garmin-golf stats clubs --course "Golf National ~ Aigle" --hole 7 --by-context --json
```

Use `stats second-shots` when the user wants club usage and outcomes on second shots for par 4s and par 5s.
Use `stats clubs` when club labels look suspicious or need bag-specific overrides; it exposes observed `club_id` values, inferred names, configured names, counts, and average distances.
Use `stats clubs --by-context` when the user wants club performance split by contexts such as par-3 tee shots, par-4 tee shots, par-4 approaches, par-5 second shots, short game, recovery, and putting.
Add `--course` when the user wants club usage only on one course across all recorded rounds there. Add `--hole` to narrow further to one specific hole, optionally combined with `--by-context`.

### Course analysis

```bash
uv run garmin-golf stats courses --json
uv run garmin-golf stats course --course "Golf National ~ Aigle" --json
uv run garmin-golf stats course --course "Golf National ~ Aigle" --period last-12-months --json
```

Use `stats courses` to discover the exact course name. `stats course` returns a
course summary, next-round focus, and hole difficulty table. Use plain output only
when a human-readable table is more useful than machine-readable JSON.

## Date Filters

- Supported `--period` values: `last-12-months`, `this-year`, `last-year`
- Do not combine `--period` with `--from` or `--to`
- When the user uses relative language, prefer the matching CLI period or state
  the exact date range used in the response

## Interpretation Notes

- `average_score` and `average_to_par` are the top-level scoring anchors
- `gir_pct`, `gir_per_18`, `fir_pct`, and `fir_per_18` show tee-to-green ball
  striking
- `scrambling_pct` and `scrambles_per_18` show how often missed greens are saved
- `three_putt_pct`, `three_putts_per_18`, and `average_putts_per_18` expose
  putting leakage
- `stats putting` is the first stop when the user asks how make rates change by
  first-putt distance
- `penalty_hole_pct`, `penalties_per_round`, and `penalties_per_18` capture
  avoidable mistakes
- `practice-focus` converts those patterns into ranked training priorities
- `stats trends` is the first stop when the user asks whether their game is improving or regressing recently
- `second-shots` helps test whether long clubs are overrepresented or costly on par 4 and par 5 second shots
- `stats clubs` is the first stop when Garmin club labels look wrong for a specific bag
- `stats clubs --by-context` separates raw club inventory from on-course usage patterns
- `stats clubs --course ... --hole ...` is the right slice when the user wants to know what they actually hit on one course or one recurring problem hole
