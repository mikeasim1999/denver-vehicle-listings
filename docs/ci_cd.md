# Git Integration & CI/CD

How the Denver Vehicle Listings pipeline connects to version control and automated checks.

## Git Integration — Databricks Repos

Databricks Repos syncs notebooks directly with GitHub so changes flow both directions — edit in Databricks and commit, or push from GitHub and pull into the workspace.

### Setup

Two repos are cloned as Git folders in Databricks, each into a `dev/` and `main/` folder under the user's Home directory:

```
/Workspace/Users/<your-email>/
├── dev/
│   ├── denver-vehicle-listings/        (dev branch — active development)
│   └── databricks-shared-utilities/    (dev branch)
└── main/
    ├── denver-vehicle-listings/        (main branch — production)
    └── databricks-shared-utilities/    (main branch — %run target)
```

1. **`denver-vehicle-listings`** — The main project (ETL notebooks, SQL queries, reports)
2. **`databricks-shared-utilities`** — Reusable utility notebooks shared across projects

In Databricks: **Workspace → Home → Create → Git folder** → paste the repo URL → select the branch → **Create**

### Branch Workflow

| Branch | Purpose |
|--------|---------|
| `dev` | Active development — edit notebooks here, test changes |
| `main` | Production — merged via pull request, Jobs pull from this branch |

1. Work in the `dev` branch in Databricks Repos
2. Commit and push from the Git dialog in the notebook editor
3. Open a pull request on GitHub (`dev` → `main`)
4. CI checks run automatically (see below)
5. Merge the PR — Databricks Jobs pick up the change on next run

### Cross-Repo Utilities

ETL notebooks in `denver-vehicle-listings` call shared utility functions via `%run` pointing to the `main/` copy of the shared utilities repo:

```python
%run /Workspace/Users/<your-email>/main/databricks-shared-utilities/functions/data_quality_checks
```

The `%run` paths target the `main/` workspace folder so Jobs always reference production-ready code. This keeps utility code in a single repo instead of duplicating it across projects. See [design_decisions.md](design_decisions.md) for the reasoning behind this pattern.

### Jobs — Git Provider as Source

Databricks Jobs are configured to use the **Git provider** as the notebook source, pointed at the `main` branch. The task path is relative to the repo root:

```
notebooks/etl/dvl_bronze_load
```

This ensures Jobs always run the latest merged code from `main`, not a workspace copy.

---

## CI/CD — GitHub Actions

A GitHub Actions workflow runs automatically on every push and pull request to `main`, validating code quality before it reaches Databricks.

### What It Checks

| Check                  | Tool   | Purpose                                                  |
|------------------------|--------|----------------------------------------------------------|
| **Python lint**        | Ruff   | Catches syntax errors, unused imports, formatting issues |
| **Notebook structure** | Custom | Verifies Databricks notebook headers are intact          |

### Workflow File

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install ruff

      - name: Lint Python notebooks
        run: ruff check notebooks/

      - name: Verify notebook structure
        run: |
          echo "Checking Databricks notebook headers..."
          for f in $(find notebooks/ -name "*.py"); do
            head -1 "$f" | grep -q "# Databricks notebook source" || {
              echo "FAIL: $f missing Databricks notebook header"
              exit 1
            }
          done
          echo "All notebooks have valid headers."
```

### Ruff Configuration

`ruff.toml` in the repo root:

```toml
line-length = 120

[lint]
select = ["E", "F", "W"]
ignore = ["E501", "F821", "E402", "F841", "F401", "F541", "W291", "W292", "W293"]

[lint.per-file-ignores]
"notebooks/utilities/*.py" = ["F811"]
```

**Why these rules are ignored:**

| Rule | Reason |
|------|--------|
| E501 | Line too long — Databricks notebooks have wide cells |
| F821 | Undefined name — `spark`, `dbutils`, `display` are Databricks globals |
| E402 | Import not at top — notebooks use `%run` and cell separators |
| F841 | Unused variable — common in notebook exploratory cells |
| F401 | Unused import — notebooks import for use in later cells or `%run` scope |
| F541 | f-string without placeholders — common in log messages |
| W291 | Trailing whitespace — Databricks editor artifacts |
| W292 | No newline at end of file — Databricks editor artifacts |
| W293 | Whitespace on blank line — Databricks editor artifacts |

---

## Deployment Notes

The pipeline deploys through the dev/main branch workflow described above. Notebooks are edited in Databricks Repos on the `dev` branch, pushed to GitHub, and merged into `main` via pull request. Databricks Jobs pull from `main` automatically.

For a production environment, the next step would be **Databricks Asset Bundles (DABs)**, which define Jobs, clusters, and notebook paths as code in a `databricks.yml` file. This allows `databricks bundle deploy` to push the full pipeline configuration from a CI/CD workflow.

That is outside the scope of this project since it runs on a single workspace with no dev/staging/prod separation, but the Git integration and CI checks above establish the foundation for it.
