# Golf Analysis Engine Specification

**Repository:** `flepied/garmin-golf`  
**Status:** Draft specification  
**Target release:** MVP  
**Primary audience:** maintainers and AI coding agents

## 1. Purpose

This document specifies an analysis engine that turns locally mirrored Garmin golf data into one prioritized, evidence-backed action for the golfer.

The engine must not behave like a generic dashboard or an unconstrained AI coach. Its core loop is:

> **Observe → detect a repeated pattern → estimate its cost → recommend one experiment → measure the result.**

The MVP should answer three user questions:

1. **Post-round:** What mattered most in this round, and what should I test next time?
2. **Rolling analysis:** Which repeated pattern currently offers the best improvement opportunity?
3. **Course strategy:** Which holes or decisions deserve a conservative plan on a familiar course?

## 2. Product promise

### 2.1 Primary promise

> From a golfer's recorded Garmin history, identify the recurring pattern that is most likely to be costing score, recommend one measurable change, and reassess it after several rounds.

### 2.2 Positioning

The product should not promise more statistics. It should promise prioritization:

> Garmin records the shots. The analysis engine decides what deserves attention first.

### 2.3 MVP target user

The initial target user is a golfer who:

- records rounds and shot data with a Garmin golf watch;
- has at least 5 usable rounds, preferably 10 or more;
- already sees basic Garmin statistics but does not know what to work on first;
- wants an asynchronous, evidence-based debrief rather than a swing diagnosis.

## 3. Existing repository capabilities

The current project already provides the main ingestion and analytical foundations.

### 3.1 Data flow

```text
Garmin Connect authenticated browser session
    → browser mirror raw JSON
    → normalization
    → local Parquet tables
    → deterministic statistics
    → JSON or Rich CLI output
```

Current local tables:

- `rounds.parquet`
- `holes.parquet`
- `shots.parquet`

### 3.2 Available round-level data

The normalized round model includes, when present:

- round and scorecard identifiers;
- played date and start time;
- course and tee names;
- total score and total par;
- exclusion flag and free-form comment;
- Garmin handicap-related fields;
- original summary and scorecard JSON.

### 3.3 Available hole-level data

The normalized hole model includes, when present:

- hole number and par;
- strokes and putts;
- fairway hit and Garmin fairway outcome;
- green in regulation;
- penalties;
- handicap score;
- pin latitude and longitude;
- original hole JSON.

### 3.4 Available shot-level data

The normalized shot model includes, when present:

- hole and shot sequence;
- club and club identifier;
- recorded shot distance;
- shot type, automatic shot type, lie and result;
- shot source and timestamp;
- start and end latitude/longitude;
- start and end map coordinates;
- original shot JSON.

### 3.5 Existing analytical commands

The current CLI already exposes:

- overall summaries;
- per-round detail and shot sequences;
- rolling trends;
- putting by first-putt distance;
- practice-focus heuristics;
- tee-shot outcomes;
- second-shot outcomes;
- club distance and dispersion;
- club outcomes by playing context;
- course and hole history;
- round annotations and exclusions.

The new engine should compose these capabilities instead of duplicating them.

## 4. Scope

### 4.1 In scope for MVP

- Personal historical analysis, without requiring a multi-player benchmark.
- Comparison of the golfer with their own history.
- Post-round debriefs using the current round plus relevant history.
- Detection of repeated scoring, tee-shot, approach, putting and course-hole patterns.
- Ranking of insight candidates by impact, recurrence, confidence and actionability.
- Selection of one primary priority.
- Recommendation of one measurable strategy or practice experiment.
- Tracking and reassessment of experiments after a defined number of rounds.
- Structured JSON output suitable for an AI renderer.
- Conservative language and explicit confidence levels.

### 4.2 Explicitly out of scope for MVP

- Swing diagnosis.
- Claims about technical causes of a miss.
- Universal club recommendations.
- Automatic optimal aim lines based on hazard geometry.
- Benchmark claims such as “your putting is equivalent to handicap 10.”
- Full strokes-gained calculations against an external population.
- Causal claims derived only from associations.
- Fully automated Garmin authentication.
- Medical, biomechanical or equipment-fitting advice.

## 5. Analysis modes

The engine must keep the following modes separate because they answer different questions.

### 5.1 Post-round debrief

**Question:** What happened in one round, and which one or two decisions are worth testing next time?

**Primary inputs:**

- selected round;
- last 5 to 10 rounds;
- history on the same course and holes, when available.

**Output sections:**

1. What went well.
2. Where the score was lost.
3. Two decisions worth changing.
4. One practice focus.
5. One missing-context question for the golfer.

### 5.2 Rolling player analysis

**Question:** Which repeated pattern offers the best current improvement opportunity?

**Primary inputs:**

- configurable period or last N rounds;
- all eligible rounds, holes and shots;
- current active experiments.

**Output:**

- scoring profile;
- up to five evidence-backed insight candidates;
- one primary priority;
- one recommended experiment;
- review horizon and success metrics.

### 5.3 Course strategy analysis

**Question:** On a familiar course, which holes and choices deserve a specific plan?

**Primary inputs:**

- rounds played on the selected course;
- per-hole outcomes;
- tee-shot and club context on each hole;
- current golfer tendencies.

**Output:**

- highest-risk holes;
- repeated miss patterns;
- candidate conservative decisions;
- sample-size warnings;
- no unsupported aim-line claims.

## 6. Required analysis pipeline

```text
1. Select scope
2. Run data-quality checks
3. Build derived features
4. Build player scoring profile
5. Generate deterministic insight candidates
6. Score and gate candidates
7. Select the primary priority
8. Attach a measurable experiment
9. Render structured JSON
10. Optionally use an LLM for prose only
```

## 7. Scope selection

Each analysis result must declare its scope.

Required fields:

```json
{
  "period": {
    "from": "2026-01-01",
    "to": "2026-07-31"
  },
  "rounds": 18,
  "holes": 324,
  "shots": 1420,
  "courses": 7,
  "excluded_rounds": 2,
  "player_context": {
    "handicap_index": 16.4,
    "target_handicap_index": 14.0
  }
}
```

`handicap_index` and `target_handicap_index` are optional context fields in the MVP. They must not be used to claim peer-relative performance without a benchmark dataset.

## 8. Data-quality layer

No insight may be produced before the engine evaluates data coverage.

### 8.1 Coverage metrics

The engine should calculate:

- rounds with complete hole scores;
- holes with putt counts;
- holes with GIR data;
- eligible non-par-3 holes with fairway outcomes;
- holes with pin coordinates;
- shots with club labels;
- shots with distances;
- shots with start coordinates;
- shots with end coordinates;
- approach shots with start position, end position and pin position;
- first putts with usable starting distance;
- rounds with at least 80% of expected usable shot records.

### 8.2 Data-quality statuses

Each analytical domain should receive one status:

- `unavailable`: required fields are absent;
- `limited`: some analysis is possible, but sample or coverage is weak;
- `usable`: sufficient for qualified findings;
- `strong`: sufficient for stable comparisons and trend checks.

### 8.3 Exclusions

The engine must honor `exclude_from_stats=true` for aggregate analysis.

It should also support rule-based exclusions for:

- incomplete rounds;
- rounds with too few recorded holes;
- rounds explicitly annotated as match play or non-comparable;
- domain-specific missing data.

A round can be excluded from one analytical domain without being excluded from all domains.

## 9. Derived features

Derived features should be deterministic, testable and stored or rebuilt consistently.

### 9.1 Shot geometry

When start/end coordinates and pin coordinates are available, calculate:

- `start_to_pin_m`;
- `end_to_pin_m`;
- `distance_reduction_m`;
- `recorded_vs_geodesic_distance_delta_m`;
- `pin_data_available`;
- `shot_geometry_quality`.

The implementation should use an explicitly documented geodesic calculation.

### 9.2 Distance buckets

Default approach buckets:

```text
< 50 m
50–75 m
75–100 m
100–125 m
125–150 m
150–175 m
> 175 m
```

Default first-putt buckets should continue to use the existing putting implementation unless a migration is justified.

Bucket definitions must be configuration constants, not embedded throughout detector code.

### 9.3 Context labels

Reuse or extend current context labels, including:

- par-3 tee shot;
- par-4 tee shot;
- par-5 tee shot;
- par-4 approach;
- par-5 second shot;
- short game;
- recovery;
- putting.

### 9.4 Outcome features

For each relevant event, calculate or reuse:

- score to par;
- par-or-better flag;
- bogey-or-worse flag;
- double-or-worse flag;
- penalty flag;
- GIR flag;
- fairway outcome;
- three-putt flag;
- successful scramble flag.

## 10. Player scoring profile

Before ranking individual insights, classify how the golfer's score is currently produced.

### 10.1 Example profile dimensions

- `volatile` versus `stable` scoring;
- catastrophe-driven versus accumulation-driven scoring;
- penalty-heavy versus penalty-light;
- GIR-limited versus short-game-dependent;
- putting-sensitive versus approach-proximity-sensitive;
- improving, declining or stable recent form.

### 10.2 Example classifications

#### Volatile / catastrophe-driven

Typical evidence:

- a material proportion of score-to-par comes from double bogeys or worse;
- penalties are concentrated in a limited number of holes;
- many pars coexist with several severe mistakes.

Likely analysis emphasis:

- risk management;
- tee-shot miss cost;
- recovery discipline;
- course-hole strategy.

#### Stable / accumulation-driven

Typical evidence:

- few doubles and penalties;
- many bogeys;
- consistently low GIR or approach proximity.

Likely analysis emphasis:

- approach distance bands;
- proximity and dispersion;
- conversion of recurring bogey patterns.

The profile is descriptive. It must not be presented as a permanent golfer type.

## 11. Insight candidate model

Each detector returns one or more structured candidates.

```json
{
  "id": "tee_driver_miss_right_cost",
  "category": "tee_shot",
  "title": "Right misses with Driver are linked to severe scores",
  "observation": "...",
  "scope": {
    "rounds": 12,
    "holes": 84,
    "shots": 31
  },
  "metrics": {},
  "baseline": {},
  "comparison": {},
  "impact_score": 0.82,
  "recurrence_score": 0.71,
  "confidence_score": 0.76,
  "actionability_score": 0.90,
  "priority_score": 0.79,
  "confidence": "high",
  "limitations": [],
  "candidate_actions": [],
  "success_metrics": []
}
```

## 12. Deterministic detector catalog

The MVP must use deterministic detectors. The LLM must not invent detectors from raw data.

### 12.1 Scoring-shape detectors

#### Double-or-worse concentration

Detect whether double bogeys or worse explain a disproportionate share of score above par.

Required output:

- doubles or worse per 18;
- score cost attributable to those holes;
- share of total score above par;
- concentration by course, hole, club or miss type when available.

#### Penalty concentration

Detect whether penalties are frequent, trending, or concentrated in defined contexts.

Do not infer the exact penalty cause unless recorded.

#### Par-type weakness

Compare performance on par 3s, par 4s and par 5s.

The detector should not recommend a practice area solely because one par type has the highest average score. It must identify a repeated actionable pattern inside that par type.

### 12.2 Tee-shot detectors

#### Miss-direction cost

Compare fairway, left miss, right miss and other/unknown outcomes by:

- sample size;
- score to par;
- bogey-or-worse rate;
- double-or-worse rate;
- penalties.

Candidate output example:

> Right misses with Driver are associated with a materially higher double-or-worse rate than fairways or left misses.

Guardrail:

- report association, not swing cause;
- require like-for-like context;
- do not compare a par-3 iron with a par-5 driver.

#### Tee-club trade-off

For repeated course-hole or par contexts, compare candidate tee clubs using:

- distance;
- distance dispersion;
- scoring outcomes;
- penalties;
- sample size.

A longer club must not be preferred from distance alone.

### 12.3 Approach detectors

#### Distance-band weakness

For each approach distance bucket, calculate:

- shot count and round count;
- GIR rate;
- median and mean proximity to pin;
- proximity dispersion;
- score outcome of the hole;
- club usage.

Detect buckets that are both frequent and materially worse than adjacent or historical buckets.

#### Club overlap

Detect clubs with overlapping observed distance distributions.

This is an equipment or decision observation, not an automatic equipment recommendation.

#### Club-context underperformance

Within the same context and comparable distance range, detect clubs associated with:

- higher dispersion;
- worse proximity;
- lower GIR;
- worse resulting score.

Guardrail:

- minimum sample requirements;
- no technical diagnosis;
- state possible confounding from lie, wind, target and recovery intent.

### 12.4 Short-game detectors

When lie/result data is sufficient, compare short-game outcomes by:

- starting distance;
- lie category;
- proximity;
- successful scramble;
- resulting score.

When lie data is insufficient, keep findings at the distance-band level and state the limitation.

### 12.5 Putting detectors

#### Three-putt origin

Determine whether three-putts are concentrated by first-putt distance.

Also test whether long first putts are preceded by weak approach proximity. This allows the engine to distinguish a likely lag-putting issue from an approach-proximity issue.

#### Short-putt conversion

Only run when first-putt distance coverage is sufficient.

Do not infer stroke mechanics.

### 12.6 Course-hole detectors

#### Repeated high-risk hole

Identify holes with persistent:

- high score to par;
- double-or-worse rate;
- penalty rate;
- repeated miss direction;
- weak GIR or three-putt rate.

#### Course decision candidate

When enough repetitions exist, propose a conditional test such as:

- a different tee club;
- a more conservative target;
- lay-up versus attack comparison;
- centre-green objective.

The data may support testing a decision. It does not automatically prove the optimal decision.

### 12.7 Trend detectors

Use rolling windows to detect:

- sustained improvement;
- sustained regression;
- one-round anomalies;
- a metric improvement not reflected in score;
- a score improvement driven by an unsustainably strong secondary metric.

Default comparisons:

- last 5 rounds versus previous 5;
- last 10 rounds versus previous 10 when data permits.

## 13. Candidate gating

A detector result must pass all mandatory gates before ranking.

### 13.1 Minimum sample defaults

- fewer than 5 rounds: exploratory only;
- fewer than 10 relevant shots: exploratory only;
- 10–29 relevant shots: potentially usable with medium confidence;
- 30 or more relevant shots across multiple rounds: eligible for high confidence.

These are product guardrails, not universal statistical laws.

### 13.2 Mandatory gates

A candidate must have:

- a clearly defined comparison or baseline;
- a meaningful sample;
- a material difference;
- a plausible measurable action;
- no contradiction with a stronger candidate using the same evidence;
- explicit limitations.

### 13.3 Statistical robustness

The MVP should support at least:

- median as well as mean for skewed distance and proximity data;
- dispersion reporting;
- bootstrap confidence intervals or another documented uncertainty method for key comparisons;
- sensitivity checks excluding the worst single round;
- comparison across multiple time windows when possible.

A p-value is not required for every detector. Product confidence should combine sample, effect size, stability and data quality.

## 14. Candidate scoring and ranking

Each eligible candidate receives normalized scores in `[0, 1]`.

### 14.1 Components

#### Impact

How strongly is the pattern associated with score cost or severe outcomes?

Possible signals:

- difference in average score to par;
- difference in double-or-worse rate;
- penalty frequency;
- approach frequency multiplied by outcome difference;
- estimated opportunity per 18.

#### Recurrence

How often does the pattern occur?

Possible signals:

- events per 18;
- rounds containing the pattern;
- persistence across courses or time windows.

#### Confidence

How reliable is the observation?

Possible signals:

- sample size;
- data-quality status;
- stability across windows;
- uncertainty interval;
- sensitivity to outlier removal.

#### Actionability

Can the golfer test a specific change?

Examples of high-actionability findings:

- tee-club experiment on defined holes;
- centre-green strategy from a distance band;
- distance-control practice with a measurable target;
- recovery rule after a miss.

### 14.2 Default priority formula

```text
priority_score =
    0.35 × impact
  + 0.20 × recurrence
  + 0.25 × confidence
  + 0.20 × actionability
```

Before applying the formula, mandatory gates must pass. A high weighted score must not rescue an invalid or unsupported insight.

Weights should be centralized in configuration and covered by tests.

### 14.3 Deduplication

Related candidates should be merged or suppressed.

Example:

- “high three-putt rate beyond 15 m” and
- “weak lag putting beyond 15 m”

should normally become one candidate unless they rely on materially different evidence.

## 15. Primary priority selection

The rolling analysis may display up to five candidates, but it must select only one primary priority.

The primary priority must include:

1. observation;
2. quantified evidence;
3. qualified interpretation;
4. action or experiment;
5. review horizon;
6. success metrics;
7. confidence and limitations.

Example:

```text
Observation
Approaches from 100–125 m have lower GIR and wider proximity dispersion than adjacent bands.

Evidence
64 shots across 17 rounds; 23% GIR; median proximity 17.2 m.

Interpretation
The recorded history suggests that this distance band is a frequent scoring constraint. The data does not identify a swing cause.

Experiment
For three weeks, practise 100 m, 110 m and 120 m with the two most frequently used clubs in this band.

Review
Reassess after five rounds.

Success metrics
GIR and median proximity from 100–125 m.

Confidence
High, subject to unrecorded wind, lie and target intent.
```

## 16. Experiment model

Experiments are the distinguishing feature of the engine. A recommendation must become a trackable hypothesis.

### 16.1 Experiment schema

```json
{
  "id": "exp_2026_08_driver_right_miss",
  "status": "active",
  "created_from_insight_id": "tee_driver_miss_right_cost",
  "hypothesis": "A more conservative tee decision on identified holes will reduce severe scores caused by right misses.",
  "action": "Use the selected alternative club or conservative target on the four identified holes.",
  "starts_on": "2026-08-01",
  "review_after_rounds": 5,
  "eligible_context": {},
  "baseline_metrics": {},
  "success_metrics": [
    "penalties_per_eligible_tee_shot",
    "double_or_worse_pct",
    "avg_to_par"
  ],
  "result": null
}
```

### 16.2 Experiment outcomes

- `confirmed`;
- `not_confirmed`;
- `inconclusive`;
- `insufficient_data`;
- `cancelled`.

### 16.3 Review rules

At review time, compare like-for-like eligible contexts. Do not compare all subsequent rounds if the experiment only concerns a subset of holes or distances.

The engine must preserve the baseline used when the experiment was created.

## 17. Recommendation catalog

The deterministic engine should select from a controlled recommendation catalog rather than allowing unrestricted AI-generated advice.

### 17.1 Strategy experiments

Examples:

- test an alternative tee club on specified holes;
- use a conservative target when one miss direction is costly;
- use centre-green strategy in a defined approach band;
- apply a recovery rule after an off-fairway drive;
- compare lay-up and attack outcomes on recurring par 5s.

### 17.2 Practice experiments

Examples:

- distance-control ladder at selected yardages;
- lag-putting practice from the distance band producing most three-putts;
- dispersion practice for one club/context;
- short-game practice from the highest-cost distance band.

### 17.3 Recommendation requirements

Each recommendation template must define:

- compatible detector categories;
- required data;
- action text parameters;
- practice volume or decision rule;
- review horizon;
- success metrics;
- conditions that invalidate the recommendation.

The engine should avoid detailed swing drills unless supplied by a qualified external source or coach. The MVP should focus on strategy, distance control and measurable outcomes.

## 18. LLM role and guardrails

The LLM is a narrative renderer, not the statistical engine.

### 18.1 Allowed LLM tasks

- explain a selected deterministic insight;
- summarize evidence without changing numbers;
- choose concise phrasing from approved recommendation templates;
- state limitations;
- ask one missing-context question;
- render the post-round five-section debrief.

### 18.2 Prohibited LLM behavior

- invent metrics or observations;
- infer wind, hazards, lie or target when absent;
- diagnose a swing fault;
- claim causality from association;
- rank raw data independently of the deterministic engine;
- hide small sample sizes;
- produce more than the configured number of actions;
- recommend a club solely from distance.

### 18.3 Required evidence bundle

The LLM receives only a structured evidence bundle containing:

- scope;
- selected insights;
- exact metrics;
- candidate actions from the catalog;
- limitations;
- experiment state.

It should not receive the entire raw dataset by default.

## 19. Proposed CLI

### 19.1 Rolling analysis

```bash
uv run garmin-golf analyze player --period last-12-months
uv run garmin-golf analyze player --last-rounds 20 --json
```

### 19.2 Post-round debrief

```bash
uv run garmin-golf analyze round --last-round
uv run garmin-golf analyze round --round-id 22068626916 --json
```

### 19.3 Course strategy

```bash
uv run garmin-golf analyze course --course "Golf National ~ Aigle"
uv run garmin-golf analyze course --course "Golf National ~ Aigle" --period last-12-months --json
```

### 19.4 Data quality

```bash
uv run garmin-golf analyze data-quality --period last-12-months
uv run garmin-golf analyze data-quality --json
```

### 19.5 Experiments

```bash
uv run garmin-golf experiment list
uv run garmin-golf experiment show --experiment-id <id>
uv run garmin-golf experiment start --insight-id <id>
uv run garmin-golf experiment review --experiment-id <id>
uv run garmin-golf experiment cancel --experiment-id <id>
```

The final command naming may be adjusted to match the current Typer hierarchy, but user-facing concepts should remain stable.

## 20. Proposed module architecture

```text
src/garmin_golf/
├── analysis/
│   ├── __init__.py
│   ├── models.py
│   ├── scope.py
│   ├── data_quality.py
│   ├── features.py
│   ├── profile.py
│   ├── detectors/
│   │   ├── __init__.py
│   │   ├── scoring.py
│   │   ├── tee_shots.py
│   │   ├── approaches.py
│   │   ├── short_game.py
│   │   ├── putting.py
│   │   ├── courses.py
│   │   └── trends.py
│   ├── ranking.py
│   ├── recommendations.py
│   ├── experiments.py
│   └── render.py
├── cli.py
├── stats.py
└── storage.py
```

### 20.1 Design constraints

- Keep current `stats.py` functions reusable.
- Do not move existing code only for architectural purity.
- Introduce typed Pydantic models or typed dataclasses for analysis outputs.
- Preserve JSON-only output behavior for agent consumption.
- Keep deterministic calculations independent from rendering.
- Maintain strict `mypy` compatibility.

## 21. Storage additions

### 21.1 Analysis results

Persisting every analysis is optional for the first implementation. If persisted, use a versioned schema and include:

- analysis version;
- code version or commit identifier;
- scope;
- generated candidate IDs;
- selected priority;
- creation timestamp.

### 21.2 Experiments

Experiments require durable local storage.

Recommended first option:

```text
data/parquet/experiments.parquet
```

Minimum columns:

- experiment ID;
- status;
- created timestamp;
- insight ID and category;
- hypothesis;
- action payload;
- scope and eligibility payload;
- baseline payload;
- success metrics;
- review-after-rounds;
- review result and timestamp;
- schema version.

JSON payload columns are acceptable initially when the nested structure would otherwise cause premature schema complexity.

## 22. JSON output contract

Example top-level output:

```json
{
  "analysis_version": "1.0",
  "analysis_type": "player",
  "scope": {},
  "data_quality": {},
  "player_profile": {},
  "insights": [],
  "primary_priority": {},
  "active_experiments": [],
  "limitations": []
}
```

### 22.1 Stability requirements

- JSON mode must contain no Rich formatting or prose outside JSON.
- Fields should be additive across minor versions.
- Renaming or changing field meaning requires an analysis schema version change.
- Missing data should use `null` or an explicit availability status, not misleading zero values.

## 23. Post-round rendering contract

The human-readable debrief must use exactly these sections:

1. **What went well**
2. **Where the score was lost**
3. **The two decisions worth changing**
4. **One practice focus**
5. **One question for the golfer**

Every historical claim must include its scope or sample size.

Preferred evidence order for a decisive hole:

```text
What happened in this round
→ relevant historical pattern
→ qualified next action
```

## 24. Testing strategy

### 24.1 Unit tests

Each detector requires fixtures covering:

- no data;
- insufficient samples;
- clear positive detection;
- no material effect;
- outlier sensitivity;
- missing optional columns;
- multiple comparable contexts;
- contradictory signals.

### 24.2 Ranking tests

Cover:

- mandatory gating;
- score normalization;
- weighting;
- deduplication;
- stable ordering;
- suppression of lower-quality duplicate candidates.

### 24.3 Experiment tests

Cover:

- creation from an eligible insight;
- baseline persistence;
- eligible-context filtering;
- review after enough rounds;
- insufficient-data outcome;
- cancellation;
- schema migration behavior.

### 24.4 Golden JSON tests

Maintain representative golden outputs for:

- player analysis;
- round debrief;
- course analysis;
- data-quality analysis;
- experiment review.

Golden fixtures must use synthetic or anonymized data.

### 24.5 Property and invariant tests

Useful invariants include:

- percentages stay within `[0, 100]`;
- confidence cannot be high when mandatory sample gates fail;
- the primary priority must reference an existing insight;
- an experiment must have at least one success metric;
- excluded rounds never enter aggregate analysis;
- JSON output remains valid when optional fields are absent.

## 25. Acceptance criteria for MVP

The MVP is complete when all of the following are true:

1. `analyze data-quality` reports coverage by analytical domain.
2. `analyze player` generates deterministic insight candidates from existing local data.
3. Every candidate contains evidence, sample size, limitations and normalized scores.
4. The engine selects exactly one primary priority.
5. The primary priority includes one measurable experiment.
6. `analyze round` produces the required five-section debrief.
7. `analyze course` identifies high-risk holes and only makes qualified strategy suggestions.
8. Small samples are explicitly marked and cannot produce high confidence.
9. JSON output is stable, machine-readable and tested.
10. At least one experiment can be persisted and reviewed after subsequent rounds.
11. The LLM, when used, cannot alter metrics or introduce unsupported findings.
12. `ruff check`, `ruff format --check`, `mypy` and `pytest` all pass.

## 26. Suggested implementation phases

### Phase 1 — Evidence foundation

- data-quality report;
- shot-to-pin derived geometry;
- shared analysis models;
- scope and sample metadata.

### Phase 2 — First useful analysis

Implement three high-value detectors:

1. double-or-worse concentration;
2. tee-shot miss-direction cost;
3. approach distance-band weakness.

Add ranking and one primary priority.

### Phase 3 — Post-round product

- five-section debrief;
- same-course and same-hole historical context;
- controlled recommendation catalog;
- one follow-up question.

### Phase 4 — Experiments

- local experiment persistence;
- baseline freezing;
- review after N eligible rounds;
- confirmed/inconclusive/not-confirmed outcomes.

### Phase 5 — Broader detector coverage

- putting origin analysis;
- short-game analysis;
- course strategy candidates;
- trends and regression detection;
- candidate deduplication improvements.

### Phase 6 — Optional AI renderer

- structured evidence prompt;
- exact-number preservation checks;
- prose snapshots;
- no-LLM fallback rendering.

## 27. Validation plan before productization

The engine should first be validated on the repository owner's historical data.

For each generated primary priority, assess:

- **surprise:** was the finding non-obvious?
- **robustness:** did it survive sample and sensitivity checks?
- **actionability:** did it change a real decision or practice plan?
- **measurability:** could the effect be reviewed after several rounds?
- **trust:** did the wording stay within what the data supports?

Before broadening the product, repeat the process with at least three golfers who have similar Garmin data coverage.

The critical validation signal is not that the report is interesting. It is that the golfer changes one decision, follows the experiment, and returns to see whether it worked.

## 28. Future extensions

Potential post-MVP extensions:

- benchmark datasets segmented by handicap index;
- course rating and slope normalization;
- external course geometry and hazard maps;
- weather and wind enrichment;
- coach-authored recommendation catalogs;
- multi-player and coach dashboards;
- automatic PDF or email reports;
- local-first desktop packaging;
- privacy-preserving aggregate benchmarks;
- subscription workflow after ingestion friction is solved.

## 29. Open questions

1. Where should the optional handicap index and target index be stored?
2. Should experiments use Parquet, SQLite or a versioned JSON store?
3. Which coordinate system and geodesic implementation should be canonical?
4. What constitutes an approach shot when Garmin's `shot_type` is absent or inconsistent?
5. Should distance buckets be global configuration or golfer-specific?
6. How should deliberate lay-ups and recovery shots be annotated?
7. Which recommendation templates are safe without input from a qualified golf coach?
8. Should the initial CLI namespace be `analyze`, `coach` or additional `stats` subcommands?
9. Should insights be persisted to preserve historical recommendations across algorithm versions?
10. What minimum data coverage should be required before offering a paid report?

## 30. Guiding principles

1. **Decision over dashboard.** A smaller number of useful findings is better than exhaustive output.
2. **One priority at a time.** The golfer should leave with one main action.
3. **Evidence before language.** Deterministic calculations select the finding; AI explains it.
4. **Association is not causation.** Recommendations are experiments, not declarations of truth.
5. **Compare like with like.** Club and outcome comparisons require compatible contexts.
6. **Show uncertainty.** Sample size, data coverage and limitations are part of the result.
7. **Measure the recommendation.** Every action must have a review horizon and success metric.
8. **Local-first by default.** Preserve the project's current privacy-friendly architecture.
9. **No benchmark required for V1.** Personal history is sufficient for the initial product promise.
10. **Automate only after value is demonstrated.** The engine must first prove that its primary priorities change real golfer behavior.
