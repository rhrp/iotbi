# G-Eval Evaluation Report

This README presents a **G-Eval-based evaluation** using three input files:

- **poi_150_db.json**: Questions databases
- **poi_150_db_results.json**: System's results
- **poi_150_db_eval.json**: Evaluation results

The purpose of this document is to summarize the evaluation setup, describe the inputs, and present the outcomes in a clear and reproducible format.

## Overview

**G-Eval** is an evaluation approach that uses an LLM as a judge to assess generated answers against defined criteria. In this evaluation, the questions from poi_150_db.json are matched with the generated results from poi_150_db_results.json, and the final assessment is recorded in poi_150_db_eval.json.

## Files

| File | Description |
|---|---|
| **poi_150_db.json** | Contains the evaluation questions, ground_truth, and prompts |
| **poi_150_db_results.json** | Contains the generated answers, consumed tokens and times |
| **poi_150_db_eval.json** | Contains the G-Eval assessment outputs and prompts|

## Evaluation Flow

The evaluation follows this process:

1. **Load questions** from poi_150_db.json.
2. **Load generated results** from poi_150_db_results.json.
3. **Run G-Eval assessment** by comparing the generated outputs against the intended evaluation criteria.
4. **Store evaluation results** in poi_150_db_eval.json.
5. **Summarize findings** for analysis and reporting in poi_150_db_eval.json.

