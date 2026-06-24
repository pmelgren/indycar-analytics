# Indycar-Analytics AGENT GUIDE

## Scope
- Applies to all files under `indycar-analytics/`.

## Project Focus
- This repo owns scraping, parsing, and cleaning of IndyCar/related raw data files.
- Keep changes focused on data collection and cleaning workflows.

## Coding Style
- Prefer minimal, targeted edits over broad refactors.
- Prefer simple, linear logic and straightforward conditionals.
- If there is ambiguity between "more robust" and "simpler", choose simpler unless explicitly asked otherwise.
- Do not add extra validation/type-checking/framework abstractions unless explicitly requested.

## Output/Data Handling
- Preserve existing filename/data-model compatibility unless the task explicitly asks for a naming change.
- When adding new sources/series, keep outputs isolated so existing downstream consumers are not impacted.
