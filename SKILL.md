---
name: garmin-golf
description: Use this skill when working with Garmin golf statistics from the local garmin-golf project, especially to run the CLI, inspect round or course history, compare date windows, identify practice priorities, and give evidence-based training or course-strategy coaching from stored data.
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
- Compare approach proximity, dispersion, and GIR by club with `stats clubs --approach-accuracy`
- Identify one current priority with `analyze player`
- Check data coverage with `analyze data-quality`
- Produce a structured post-round debrief with `analyze round`
- Identify repeated risk on a familiar course with `analyze course`
- Start, inspect, review, or cancel a measurable local experiment
- Explain golf metrics already exposed by the CLI

Do not start by editing the project. Only switch to source inspection when the
user asks to extend or debug the implementation itself.

## Workflow

1. Work from the current directory.
2. Start with the narrowest command that answers the question.
3. Prefer `--json` when the consumer is an AI agent or another script.
4. Use `--period` or `--from/--to` when the user asks for a time window.
5. Use `analyze player` first when the user asks what deserves attention most;
   use `stats` commands to investigate that selected priority.
6. Summarize the key trends instead of dumping raw tables back to the user.

If the CLI reports that no local rounds are available, explain that the dataset
has not been mirrored yet. Only move into the browser mirroring workflow if the
user asks for ingestion help.

## AI Coaching Workflow

Use the CLI as the evidence source and do the coaching interpretation after the
relevant JSON has been collected. Do not ask the user to provide raw Garmin JSON
or infer a conclusion that the output does not support.

For any coaching response:

1. State the scope: course, date window, number of rounds, and (where relevant)
   the number of shots or holes behind the observation.
2. Compare like with like. For a club decision, compare the same `context`
   (for example `tee_par_4`), not a driver tee shot against a wedge approach.
3. Use outcomes together: `avg_to_par`, `double_or_worse_pct`,
   `bogey_or_worse_pct`, penalties, GIR/FIR, and sample size. Do not choose a
   club from distance alone.
4. Make recommendations conditional rather than absolute: "based on your
   recorded rounds" and "consider" are appropriate when conditions, tee set,
   wind, or lie are unknown.
5. Separate a *strategy* observation (club/target/risk choice) from an
   *execution* observation. Scorecard data alone cannot prove why a shot missed.
6. End with one or two measurable actions, not a broad instruction such as
   "improve your short game."

Treat small samples as exploratory. A useful default is to call out fewer than
10 shots or fewer than 5 rounds explicitly; never present it as a reliable
club-selection rule. The CLI does not record weather, wind, pin position for
every shot, intended target, hazard geometry, or whether a player deliberately
chose a recovery. It therefore supports personal historical tendencies, not a
universal yardage book or swing diagnosis.

## Prioritized analysis engine

The `analyze` namespace is deterministic: it calculates coverage, evidence,
candidate scores, confidence, limitations, and a single primary priority before
rendering advice. Do not replace its priority with an untested interpretation of
raw data. Use `stats` commands for follow-up detail, not to silently override it.

```bash
uv run garmin-golf analyze data-quality --json
uv run garmin-golf analyze player --period last-12-months --json
uv run garmin-golf analyze player --last-rounds 20 --json
uv run garmin-golf analyze round --last-round --json
uv run garmin-golf analyze course --course "Golf National ~ Aigle" --json
```

- `analyze player` and `analyze data-quality` default to the last 12 months.
  They accept either date filters or `--last-rounds`, never both.
- Treat `unavailable` as no finding and `limited` as exploratory; only describe
  `usable` or `strong` results as qualified conclusions.
- `analyze player` returns at most five candidates and exactly one primary
  priority, with samples, comparison, confidence, limitations, action, and
  review metrics.
- `analyze round` contains the five required debrief sections and uses recent
  12-month history. It cannot establish swing cause, wind, target intent, or
  hazard geometry.
- `analyze course` ranks high-risk holes and makes only conditional conservative
  suggestions. Its `player_profile.tee_club_options` lists observed tee clubs
  on those holes when a club has at least five recorded tee shots, including
  rounds, average to par, double-or-worse rate, and FIR. Compare only options
  on the same hole; if one club is the only qualifying row, report that there
  is not yet evidence for a club change. It does not prove an aim line or
  universally optimal club.

Current deterministic detectors cover double-or-worse concentration, tee-shot
miss-direction cost, and approach distance-band weakness when geometry coverage
is usable. An `Unknown` Garmin club label is not a valid tee-club recommendation.

`analyze player --json` also exposes direction profiles under
`player_profile.tee_direction` and `player_profile.approach_direction`. The tee
profile uses Garmin's authoritative `HIT`, `LEFT`, and `RIGHT` fairway outcomes
for first shots on par 4s and par 5s. The approach profile uses the shot's GPS
start/end coordinates and the hole's recorded pin to classify the finish as
left, right, or on-line relative to the start-to-pin line; it includes GIR
counts and percentages for each direction. Approach classification has a
3-metre lateral tolerance. Treat these as historical tendencies, not proof of
aim, swing path, wind effect, or intent.

### Experiments

```bash
uv run garmin-golf experiment start --insight-id <id> --json
uv run garmin-golf experiment list --json
uv run garmin-golf experiment show --experiment-id <id> --json
uv run garmin-golf experiment review --experiment-id <id> --json
uv run garmin-golf experiment cancel --experiment-id <id> --json
```

Start only an insight returned by the current default `analyze player` scope.
The engine freezes its baseline and eligibility payload in local Parquet. Garmin
cannot verify whether the golfer followed an intended target or strategy, so
state that limitation when interpreting a review.

### Coaching question → data to extract

| Coaching question | CLI extraction | What the AI can responsibly advise |
| --- | --- | --- |
| What should I focus on first? | `analyze player --period last-12-months --json`; optionally `analyze data-quality --json` | Lead with the engine's one primary priority, evidence, confidence, limitations, and measurable experiment. Use raw stats only to clarify it. |
| What should I practise? | `stats practice-focus --period last-12-months --json`; `stats summary --period last-12-months --json`; `stats putting --period last-12-months --json` | Rank one or two scoring leaks, pick a distance-specific putting or approach drill, and define a next-5/10-round metric. `practice-focus` is a heuristic: its estimated strokes are overlapping opportunities, not additive strokes-gained. |
| Is recent form improving? | `stats trends --window 5 --period last-12-months --json` | Identify sustained movement in score, GIR, FIR, scrambling, three-putts, and penalties; distinguish a trend from a single round. |
| Which holes need a plan on a course? | `stats course --course "<exact course>" --period last-12-months --json` | Prioritise holes by `avg_to_par`, double-or-worse rate, penalties, FIR/GIR, and three-putts. Suggest a conservative objective such as protecting against doubles or aiming for centre-green. |
| Is there evidence for a different tee club on a risky course hole? | `analyze course --course "<exact course>" --period last-12-months --json` | Inspect `player_profile.tee_club_options` for the same hole. Compare only clubs with at least five tee shots; one qualifying club means the data supports monitoring that club, not selecting an alternative. |
| Which opening club has produced the best outcomes on one hole? | `stats clubs --course "<exact course>" --hole <n> --by-context --json` | Filter rows to `tee_par_3`, `tee_par_4`, or `tee_par_5`; compare each club's `shots`, `rounds`, `avg_to_par`, `bogey_or_worse_pct`, `double_or_worse_pct`, and distance dispersion. Recommend a *candidate* tee club only when its sample is adequate and its outcome trade-off is favourable. |
| Is a club a sound option on a recurring hole? | `stats clubs --course "<exact course>" --hole <n> --json`, then the `--by-context` form | Use the first output for the club's observed distances and dispersion, then use the context output for scoring outcomes. Do not conflate all uses of a club on a hole with its tee-shot performance. |
| How accurate is each club into greens? | `stats clubs --approach-accuracy --period last-12-months --json` | Compare clubs within the same `distance_bucket` using approaches, rounds, median proximity, dispersion, and GIR. Treat small rows as exploratory and do not compare clubs across different buckets. |
| Which club/context is costly overall? | `stats clubs --by-context --period last-12-months --json` | Find repeated contexts with high `avg_to_par` or double/bogey rates, then propose a practice or conservative strategy experiment. This is association, not proof that the club caused the score. |
| Which first-shot miss costs the most? | `stats tee-shots --period last-12-months --json` | Compare first-shot club, distance, and resulting hole outcomes for `fairway`, `miss_left`, `miss_right`, `missed_fairway`, `no_fairway`, and `unknown`. This command is global for the selected date range; it cannot isolate one course or hole. |
| Do I tend to miss left or right? | `analyze player --period last-12-months --json` | Read `player_profile.tee_direction` for par-4/5 first shots and `player_profile.approach_direction` for GPS-derived approach direction and GIR by direction. Treat small samples as exploratory and do not infer intended aim. |
| Are second shots on par 4s/5s a problem? | `stats second-shots --period last-12-months --json` | Compare club usage, distance, and hole outcomes by par type and inferred second-shot start: `fairway`, `off_fairway`, `no_fairway`, or `unknown`. This command is global for the selected date range; it cannot isolate one course or hole. |
| What happened in a bad or good round? | `stats round --round-id <id> --json` | Review each hole's score, GIR, FIR, putts, penalties, clubs, and par-4/5 second shots; identify candidate turning points while acknowledging missing intent and conditions. |

### Tee-club strategy example

For a request such as "What should I hit from the tee on Aigle hole 2?", run:

```bash
uv run garmin-golf stats course --course "Golf National ~ Aigle" --period last-12-months --json
uv run garmin-golf stats clubs --course "Golf National ~ Aigle" --hole 2 --by-context --period last-12-months --json
```

First establish why the hole matters using the course-hole output. Then retain
only `tee_par_4` rows from the club-context output and compare the tee clubs.
The preferred coaching statement has this form:

> Across N recorded tee shots on this hole, Club A had lower average to par and
> fewer doubles than Club B. If today's tee, wind, and trouble match those
> rounds, consider Club A with a conservative target; reassess when the sample
> is small or the distance leaves an unsuitable approach.

Never claim that the data proves a particular aim line, hazard avoidance plan,
or optimal club unless those inputs are separately available.

### Recommended coaching response shape

Keep a response concise and evidence-led:

1. **Evidence:** exact scope and two or three decisive metrics.
2. **Interpretation:** the most plausible, clearly qualified explanation.
3. **Next-round plan:** a club/target/risk choice for the relevant holes.
4. **Training plan:** one drill, volume or session count, and success measure.
5. **Confidence and missing context:** sample size plus conditions that could
   change the recommendation.

### Post-round debrief

When a user asks for a round debrief, start with the recorded round and add
historical context only where it changes the advice:

```bash
uv run garmin-golf analyze round --round-id <id> --json
uv run garmin-golf stats trends --window 5 --json
uv run garmin-golf stats course --course "<exact course>" --json
```

Start with `analyze round --json` for the deterministic five-section debrief.
Then use `stats round --json` for the hole table and recorded shot sequence when
the debrief needs concrete club or distance context. Do not compare one round
against an unrelated all-time aggregate when recent or course-specific data is
available.

Write the debrief in exactly these five sections:

1. **What went well** — identify one or two strengths supported by the round
   data, such as avoiding penalties, converting missed greens, a strong par-5
   result, or good lag putting. Do not use empty praise.
2. **Where the score was lost** — name the decisive holes or repeated pattern,
   quantify the cost (for example doubles, penalties, three-putts, or score
   versus the player's course-hole history), and cite the relevant club/shot
   sequence when available.
3. **The two decisions worth changing** — make two concrete, conditional
   next-round experiments: club selection, target conservatism, lay-up choice,
   or recovery discipline. Distinguish a likely strategic change from an
   execution miss; scorecard data cannot prove intent.
4. **One practice focus** — prescribe one focused drill linked to the observed
   pattern, with a volume and a measurable on-course review metric. Do not list
   several unrelated drills.
5. **One question for the golfer** — ask the single missing-context question
   most likely to change the recommendation. For example: "Was the right miss
   on hole 11 caused by wind, alignment, or a deliberate aggressive line?"

For a potentially decisive hole, describe the evidence in this order:

```text
What happened today → the relevant historical pattern → qualified next action
```

Example: "On hole 11 you hit Driver, missed right, then made double. Across
your recorded history on that hole, right misses lead to worse outcomes than
fairways; if conditions are similar, test a club or target that removes the
right-side miss and review the result over the next five rounds."

Keep the debrief concise. Include sample sizes for any historical claim, call
out small samples, and never infer wind, hazard location, rough versus bunker,
or the player's intent from an unlabelled shot.

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

Use `stats putting` when the user wants putting performance broken down by
distance. It groups holes by first-putt starting distance bucket and reports
hole counts plus one-putt, two-putt, and three-putt-or-worse rates.

`stats round --round-id ...` does not include this distance-bucket table. It does
report the selected round's `average_putt_distance_m`, and its shot breakdown
includes the recorded putts. To inspect the distance buckets for one round,
first get that round's `played_on` date from `stats rounds --json`, then use a
date range containing only that date:

```bash
uv run garmin-golf stats putting --from YYYY-MM-DD --to YYYY-MM-DD --json
```

If there are multiple rounds on the same date, the date-filtered result combines
them; explain that limitation rather than claiming the round has no
putting-distance data. An empty `stats putting` result means no usable first-putt
distance data was available for the selected date range—it does not contradict
an `average_putt_distance_m` value shown by `stats round`.

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
uv run garmin-golf stats round --last-round --json
uv run garmin-golf stats annotate-round --round-id 22068626916 --exclude-from-stats --comment "match play"
uv run garmin-golf stats annotate-round --last-round --comment "windy"
```

Run `stats rounds` first when the user does not know the round id. `stats round --json` returns the round summary, a hole-by-hole table, round club usage, the recorded shot sequence (`shots`) with club and distance on every hole, and par-4/par-5 second-shot breakdowns when shot data is available.
Use `--last-round` instead of `--round-id` to select the most recently played local round. The two selectors are mutually exclusive.
Use `stats annotate-round` to set local round metadata. `--exclude-from-stats` removes the round from aggregate multi-round stats, `--include-in-stats` restores it, `--comment` sets a freeform note, and `--clear-comment` removes the note. Single-round `stats round` remains available for excluded rounds.

### Shot and club analysis

```bash
uv run garmin-golf stats second-shots --json
uv run garmin-golf stats second-shots --period last-12-months --json
uv run garmin-golf stats tee-shots --json
uv run garmin-golf stats tee-shots --period last-12-months --json
uv run garmin-golf stats approaches --last-round --clubs 9,PW,GW,SW --by-direction --json
uv run garmin-golf stats clubs --json
uv run garmin-golf stats clubs --period last-12-months --json
uv run garmin-golf stats clubs --from 2025-01-01 --to 2025-12-31 --json
uv run garmin-golf stats clubs --by-context --json
uv run garmin-golf stats clubs --approach-accuracy --json
uv run garmin-golf stats clubs --course "Golf National ~ Aigle" --json
uv run garmin-golf stats clubs --course "Golf National ~ Aigle" --hole 7 --by-context --json
```

Use `stats second-shots` when the user wants club usage and outcomes on second shots for par 4s and par 5s. It derives `inferred_start_lie` for the second shot from the tee-shot fairway result: `fairway`, `off_fairway`, `no_fairway`, or `unknown`. These are not confirmed rough/bunker lies; they only describe the preceding tee shot's fairway result.
Use `stats tee-shots` when the user wants to analyze first-shot club choice and the cost of a fairway, left miss, or right miss. `fairway_result` comes from Garmin's tee-shot outcome; it is the authoritative place to compare miss direction. A `missed_fairway` result does not prove the ball was in rough—it may be any off-fairway location.
Use `stats approaches` for shot-by-shot approach direction. It reports hole,
club, starting distance to the recorded pin, `left`/`right`/`on_line` direction,
lateral and longitudinal target-line error, finishing proximity, and GIR. Use
`--last-round`, `--round-id`, or date filters; `--clubs` accepts comma-separated
names and the aliases `9`, `PW`, `GW`, and `SW`; `--by-direction` sorts the
detail rows by direction. The same detail rows are available as `approaches`
in `stats round --json`. `lateral_to_target_m` is positive to the left of the
start-to-pin line and negative to the right; values from -3 m through +3 m are
`on_line`. It measures final position only and cannot establish whether the
ball initially started left or right of the intended target.
Use `analyze player` when the user wants an overall left/right tendency. Its tee profile covers par-4/5 first shots; its approach profile derives lateral direction from the GPS target line and reports GIR by direction. Do not call a direction a swing fault or assume it was unintended.
Use `stats clubs` when club labels look suspicious or need bag-specific overrides; it exposes observed `club_id` values, inferred names, configured names, counts, average distances, and distance dispersion (`distance_stddev_m`) based on the same outlier-trimmed samples.
Use `stats clubs --by-context` when the user wants club performance split by contexts such as par-3 tee shots, par-4 tee shots, par-4 approaches, par-5 second shots, short game, recovery, and putting.
Use `stats clubs --approach-accuracy` when the user wants approach precision by
club. It reports geometry-derived proximity to the recorded pin and GIR by
starting-distance band; it excludes `Unknown` clubs and requires valid Garmin
coordinates. It also reports left/right/on-line counts and percentages for each
club and distance band. Large proximity values can reflect recovery intent,
wind, lie, or an intended lay-up, so do not infer a swing cause.
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
