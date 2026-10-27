# Git Integration & CI/CD

How the Denver Vehicle Listings pipeline connects to version control and automated checks.

## Git Integration — Databricks Repos

Databricks Repos syncs notebooks directly with GitHub so changes flow both directions — edit in Databricks and commit, or push from GitHub and pull into the workspace.

### Setup

1. In Databricks, go to **Workspace → Repos → Add Repo**
2. Paste the repo URL: `https://github.com/mikeasim1999/denver-vehicle-listings`
3. Select the `main` branch and click **Create Repo**
4. Databricks clones the repo into `/Repos/<your-email>/denver-vehicle-listings/`

### Workflow

- **Edit in Databricks** — Make changes to notebooks in the Repos folder, then use the Git dialog (top-right of the notebook) to commit and push
- **Edit in GitHub/local** — Push changes to `main`, then pull from the Databricks Repos UI to sync

### Folder Structure

The Repos folder mirrors the GitHub repo. Notebooks in `notebooks/` are runnable directly. SQL files in `queries/` are read at runtime via `os.getcwd()` or a widget parameter pointing to the Repos path.

> **Note:** Utility notebooks called via `%run` need their path updated to the Repos location:
> ```python
> %run /Repos/<your-email>/denver-vehicle-listings/notebooks/utilities/data_quality_checks
> ```

---

## CI/CD — GitHub Actions

A GitHub Actions workflow runs automatically on every push and pull request to validate code quality before it reaches Databricks.

### What It Checks

| Check                  | Tool   | Purpose                                                  |
|------------------------|--------|----------------------------------------------------------|
| **Python lint**        | Ruff   | Catches syntax errors, unused imports, formatting issues |
| **Notebook structure** | Custom | Verifies Databricks notebook cell separators are intact  |

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
ignore = ["E501", "F821", "E402", "F841", "F401", "F541"]

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

---

## Deployment Notes

The pipeline currently deploys manually — notebooks are edited in Databricks and committed via Repos. For a production environment, the next step would be **Databricks Asset Bundles (DABs)**, which define Jobs, clusters, and notebook paths as code in a `databricks.yml` file. This allows `databricks bundle deploy` to push the full pipeline configuration from a CI/CD workflow.

That is outside the scope of this project since it runs on a single workspace with no dev/staging/prod separation, but the Git integration and CI checks above establish the foundation for it.
