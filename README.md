# SafeStart2

SafeStart2 is a clean-room redesign of the vaccination recall system in this
repository. It is built around three principles:

1. Normalize raw ImmunizeMe exports into patient and vaccination event records.
2. Compute recommendations from a versioned UK 2026 routine schedule.
3. Persist both source data and derived recall recommendations to Supabase.

## Scope

This version supports:

- CSV upload of one-row-per-vaccine-event files like `ImmunizeMe_020625.csv`
- Canonical mapping of common UK vaccine trade names to recall-relevant groups
- A dedicated `unvaccinated` pathway for patients whose only vaccine marker is
  `unknown`
- Routine child recalls and age-based adult recalls that can be inferred from
  demographics alone
- Optional Supabase persistence for surgeries, patients, events, imports, and
  recommendations

This version does not attempt to automate condition-based eligibility where the
source data does not contain the necessary clinical risk flags.

## App Structure

```text
SafeStart2/
├── streamlit_app.py
├── README.md
├── sql/
│   ├── 001_schema.sql
│   └── 002_rls.sql
└── safestart2/
    ├── __init__.py
    ├── catalog.py
    ├── config.py
    ├── models.py
    ├── parser.py
    ├── processing.py
    ├── schedule.py
    └── supabase_store.py
```

## Run Locally

From the repository root:

```bash
streamlit run SafeStart2/streamlit_app.py
```

If Supabase credentials are configured in `.streamlit/secrets.toml`, the app can
persist data. Otherwise it runs in local analysis mode only.

## Supabase Setup

Run these SQL files in order:

1. [001_schema.sql](/Users/janduplessis/code/janduplessis883/streamlit-projects/SafeStart/SafeStart2/sql/001_schema.sql)
2. [002_rls.sql](/Users/janduplessis/code/janduplessis883/streamlit-projects/SafeStart/SafeStart2/sql/002_rls.sql)

## Design Assumptions

- `unknown` means no vaccine on record and is treated as operationally
  unvaccinated.
- Adult seasonal vaccines are inferred only where age alone is enough to make a
  safe operational suggestion.
- Travel vaccines are stored in history but do not drive routine recalls.
