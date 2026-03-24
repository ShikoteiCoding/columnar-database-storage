# Agent Handoff: Repository Summary

This document gives subsequent agents a compact but complete overview of the repository, its intent, its current state, and how the exercise is organized.

## Purpose

This repository is a **Python learning exercise** inspired by the DuckDB table storage internals article on table storage format.

The exercise asks a candidate to rebuild a simplified columnar storage stack with the same conceptual layers:

- `AttachedDatabase -> Catalog -> Schema -> DuckTableEntry -> DataTable`
- `DataTable -> RowGroupCollection -> RowGroup -> ColumnData -> ColumnSegment`
- ordered segment lookup by row id
- block pointers and partial block allocation
- checkpoint metadata flowing bottom-up
- delete/version metadata at row-group level
- a final demo that exercises catalog creation, writes, reads, deletes, and checkpointing

The repository is intentionally scaffold-first:

- classes exist
- methods have docstrings
- many methods intentionally raise `NotImplementedError`
- tests are staged by question so the candidate can progress incrementally

## Current state

The repo is currently a **guided scaffold**, not a finished implementation.

Important facts for future agents:

- The curriculum is already written.
- The tests are already written.
- The demo entry point is already written.
- Core package modules exist with class skeletons.
- The expected implementation path is incremental by question.
- At least one earlier validation run confirmed that imports work and tests fail for the intended reason: unfinished `NotImplementedError` methods.

## Repository layout

Top-level layout:

- `.git/` — git metadata
- `.venv/` — local virtual environment
- `Makefile` — convenience commands for setup, per-question tests, full test run, and demo
- `README.md` — human overview of the exercise
- `main.py` — end-to-end demo scenario to run once the exercise is complete
- `pyproject.toml` — package metadata and Python version requirement
- `uv.lock` — locked environment dependencies
- `columnar_storage/` — main package with exercise code
- `docs/` — written curriculum and now this handoff doc
- `tests/` — staged unit tests, one file per question

## Package layout

### `columnar_storage/`

#### `columnar_storage/__init__.py`
Exports the main exercise symbols from all modules.

#### `columnar_storage/catalog.py`
Defines catalog-layer concepts:

- `ColumnDefinition`
- `TableDefinition`
- `DuckTableEntry`
- `Schema`
- `Catalog`
- `AttachedDatabase`

Current intent:
- Question 1 focuses on this module.
- `AttachedDatabase` currently exposes its catalog via `get_catalog()`.
- `Schema` and `Catalog` still contain unimplemented methods used by tests/curriculum.

#### `columnar_storage/segment_tree.py`
Defines ordered row-range lookup structures:

- `SegmentBase`
- `SegmentTree`

Current intent:
- Question 2 implements row containment, append ordering, binary search lookup, and range listing.

#### `columnar_storage/stats.py`
Defines metadata statistics:

- `BaseStatistics`

Current intent:
- Question 3 implements min/max/null-count tracking, constant detection, merging, and serialization.

#### `columnar_storage/blocks.py`
Defines physical block abstractions:

- `BLOCK_SIZE` (256 KB)
- `BlockPointer`
- `DataBlock`
- `BlockManager`
- `PartialBlockAllocation`
- `PartialBlockManager`

Current intent:
- Questions 3 and 4 cover pointer serialization and block allocation behavior.

#### `columnar_storage/storage.py`
Defines the main storage hierarchy:

- `DataPointer`
- `VersionInfo`
- `ColumnSegment`
- `ColumnData`
- `RowGroupPointer`
- `RowGroup`
- `RowGroupCollection`
- `DataTable`

Current intent:
- Questions 3 through 8 progressively implement this module.
- This is the most important module in the exercise.

#### `columnar_storage/checkpoint.py`
Defines simplified checkpoint flow objects:

- `ColumnCheckpointState`
- `RowGroupWriteData`
- `CollectionCheckpointState`
- `MetadataWriter`
- `SingleFileTableDataWriter`

Current intent:
- Question 8 implements metadata writing and final table payload construction.

#### `columnar_storage/database.py`
Defines the facade:

- `MiniDatabaseEngine`

Current intent:
- Question 9 wires together catalog objects and physical table storage.

## Documentation layout

### `docs/curriculum.md`
This is the canonical staged exercise plan.

It defines 10 questions:

1. Catalog hierarchy
2. Segment tree lookup
3. Statistics and pointers
4. Block allocation
5. Column segments
6. Column data and row groups
7. Table append and scan
8. Checkpoint state flow
9. Database facade
10. Final integration demo

Each question includes:
- goal
- guidance
- target files
- matching test file

### `docs/agent_handoff.md`
This document. Intended for future agents.

## Test layout

Each question has a dedicated test file under `tests/`:

- `test_question_01_catalog.py`
- `test_question_02_segment_tree.py`
- `test_question_03_statistics_and_pointers.py`
- `test_question_04_blocks.py`
- `test_question_05_column_segments.py`
- `test_question_06_row_groups.py`
- `test_question_07_data_table.py`
- `test_question_08_checkpoint.py`
- `test_question_09_database_facade.py`
- `test_question_10_main_demo.py`

Test design notes:

- Tests are intentionally progressive.
- They validate both behavior and exercise shape.
- Later tests assume earlier questions are solved.
- The demo test expects an end-to-end visible result with deletes respected and checkpoint metadata returned.

## Demo behavior expected by `main.py`

Once the exercise is finished, `main.py` should:

1. Create a `MiniDatabaseEngine` named `exercise_db`
2. Create schema `analytics`
3. Create table `events`
4. Insert rows with columns:
   - `event_id`
   - `category`
   - `value`
5. Delete one row through the first row group
6. Checkpoint the table
7. Scan rows back
8. Return a payload containing:
   - database name
   - schema name
   - table name
   - checkpoint metadata
   - visible rows after deletion filtering

## Tooling and commands

### Python

- Python target: `>=3.13,<3.14`
- Package manager / runner workflow assumes `uv`

### Main commands from `Makefile`

- `make setup` or `make sync` — install dependencies via `uv sync`
- `make test` — run all tests
- `make demo` — run `main.py`
- `make q1` through `make q10` — run individual question tests

Underlying pattern:
- `uv run python -m unittest ...`

## Implementation expectations

This is an educational mini-engine, not a production database.

Future agents should preserve these constraints unless the user asks otherwise:

- keep it pure Python
- keep it dependency free where practical
- favor clarity over optimization
- preserve the staged-question structure
- do not collapse the exercise into one monolithic implementation without keeping pedagogical steps clear

## Conceptual mapping to the DuckDB article

This repo mirrors these ideas from the article in simplified form:

- table storage hierarchy
- row groups and column segments
- segment trees for row-range lookup
- 256-KB block-oriented persistence
- block pointers and segment metadata
- version info for deleted rows
- bottom-up checkpoint state and final catalog-facing metadata

It intentionally does **not** attempt to fully match DuckDB internals.

## Important caveats for future agents

1. The repository may evolve question by question. Re-check the current file contents before implementing anything.
2. `catalog.py` has already been adjusted once so that `AttachedDatabase` now exposes `get_catalog()` directly. Do not assume the earlier scaffold shape without rereading the file.
3. If implementing solutions, prefer satisfying the staged tests in order:
   - Q1 first
   - then Q2
   - continue sequentially
4. Avoid large refactors that break the educational progression.
5. Keep docstrings and comments instructional.

## Recommended next actions for a future implementation agent

If asked to continue the exercise implementation, the safest sequence is:

1. read `docs/curriculum.md`
2. read the target module for the next unfinished question
3. read the matching test file
4. implement only the minimal behavior needed for that question
5. run the matching test target
6. continue to the next question

## Quick orientation checklist

If you are a future agent opening this repo, start here:

- overview: `README.md`
- staged plan: `docs/curriculum.md`
- current handoff: `docs/agent_handoff.md`
- package entrypoints: `columnar_storage/`
- validation targets: `tests/`
- end-to-end target: `main.py`

## Status summary

The repository is ready for either of these next phases:

- candidate-facing exercise use
- step-by-step implementation of the reference solution
