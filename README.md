# dbt-looker-sync 🚀

A powerful CLI tool to synchronize your **dbt** model documentation and columns directly into your **LookML** views.

This tool bridges the gap between your data transformations in dbt and your reporting layer in Looker, ensuring that your descriptions and dimensions are always up to date without manual copy-pasting.

## ✨ Key Features

- **Smart Matching:** Automatically links dbt models to LookML views by inspecting `sql_table_name`, `derived_table` SQL (including `ref` tags), or filenames.
- **Strictly Additive:** Never deletes anything from your LookML files. It only adds missing descriptions and new dimensions.
- **Intelligent Documentation Sync:** Only updates `description` if the field in LookML is currently empty. It will **never** overwrite your manual work in Looker.
- **Modern Field Support:** Scans `dimensions`, `measures`, `dimension_groups`, `filters`, and `parameters` to identify existing columns.
- **Git Integration:** 
  - Automatically checks if you are on `main` or `master` and runs `git pull`.
  - Warns about uncommitted changes before starting.
  - Creates a new Git branch for every sync operation to keep your work safe.
- **Detailed Preview:** Shows a color-coded summary of exactly how many new fields and missing descriptions will be added before you confirm.
- **Mart-First Logic:** Specifically filtered to only sync `mrt_` models to keep your Looker project clean.

## 🛠 Installation

To get started, clone this repository and install it in editable mode:

```bash
git clone <your-repo-url>
cd dbt-looker-sync
pip install -e .
```

This will register the `dbt-looker` command globally on your system.

## 🚀 Usage

Before running the sync, ensure you have generated a fresh `manifest.json` in your dbt project:

```bash
cd /path/to/dbt-project
dbt compile  # or dbt docs generate
```

Then, run the sync tool from anywhere:

```bash
dbt-looker -d /path/to/dbt-project -l /path/to/looker-repo
```

### Options:
- `-d, --dbt-project-dir`: Path to the dbt project directory (containing `dbt_project.yml`).
- `-l, --lookml-project-dir`: Path to the LookML project directory.
- `-m, --model`: (Optional) Sync a single dbt model directly, skipping the interactive menu.

## 🛡 Safety & Logic

### How it matches models
The tool is designed for BigQuery and uses the following hierarchy to find a match:
1. **Source Matching:** Compares dbt's `database.schema.alias` with the `sql_table_name` in LookML.
2. **Derived Table Matching:** Scans `derived_table` SQL for references to the dbt model name or `ref('model_name')`.
3. **Filename Matching:** Falls back to matching the model name with the `.view.lkml` filename.

### How it updates fields
1. **Existing fields:** If a column from dbt is found in LookML (via `${TABLE}.column_name`), it checks if the `description` is missing. If it is, the dbt description is added.
2. **New fields:** If a column in dbt is not found anywhere in the LookML view, a new `dimension` is created at the bottom of the file using the `${TABLE}.column_name` syntax.

## 🤝 Contributing
Feel free to open issues or submit pull requests if you have suggestions for new features or improvements!
