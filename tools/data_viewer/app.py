"""
Local teaching viewer: dbt result CSV exports and read-only browse of source systems.
Run from repo root: streamlit run tools/data_viewer/app.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# tools/data_viewer/app.py -> repo root is three levels up
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SEEDS_DIR = REPO_ROOT / "seeds"
RESULTS_DIR = REPO_ROOT / "results"
DATA_DIR = REPO_ROOT / "data"
ACT_DB = DATA_DIR / "act_system.duckdb"
VRM_DB = DATA_DIR / "vrm_system.duckdb"

RESULTS_MAX_ROWS = 500
SOURCE_PREVIEW_ROWS = 100


def list_csv_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.glob("*.csv") if p.is_file())


def duckdb_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.sql("SHOW TABLES").fetchall()
    return [r[0] for r in rows]


def render_dbt_results_tab() -> None:
    st.subheader("dbt results")
    st.caption(
        "These CSVs are written by the **post-hook** in `dbt_project.yml` after models build: "
        "one file per model under `results/`. Run **`dbt run`** (or `dbt build`) to refresh them. "
        f"Preview shows at most **{RESULTS_MAX_ROWS}** rows per file."
    )
    result_files = list_csv_files(RESULTS_DIR)
    if not result_files:
        st.info(
            "No CSVs in `results/` yet. From the repo root, run **`dbt run`** "
            "(after setup and `dbt seed` if you use seeds)."
        )
        return
    path = st.selectbox(
        "Exported model",
        result_files,
        format_func=lambda p: p.name,
        key="results_csv",
    )
    try:
        df = pd.read_csv(path, nrows=RESULTS_MAX_ROWS)
    except Exception as e:
        st.error(f"Could not read `{path}`: {e}")
        return
    st.write(f"`{path.relative_to(REPO_ROOT)}`")
    st.dataframe(df, width="stretch")


def render_source_systems_tab() -> None:
    st.subheader("Source systems")
    st.markdown(
        "In this exercise, **three upstream systems** feed dbt (see `models/zoo/sources.yml` and seeds):\n\n"
        "- **ACT** and **VRM** — separate **DuckDB files** under `data/`, attached in `profiles.yml`, "
        "standing in for real operational databases.\n"
        "- **Weather** — a **CSV file** under `seeds/`, standing in for a file drop or vendor export; "
        "dbt loads it with **`dbt seed`**.\n\n"
        "Browsing here is **read-only**."
    )

    choice = st.radio(
        "Source",
        ("ACT — Animal Care Tool (DuckDB)", "VRM — visitor system (DuckDB)", "Weather — CSV seed"),
        key="source_pick",
    )

    if choice.startswith("Weather"):
        seed_files = list_csv_files(SEEDS_DIR)
        if not seed_files:
            st.info("No CSV seeds in `seeds/`. Add files and run `dbt seed` from the repo root.")
            return
        st.caption("Weather in this repo is `seeds/weather_data.csv`; other CSV seeds appear here too.")
        csv_path = (
            st.selectbox("CSV file", seed_files, format_func=lambda p: p.name, key="seed_pick")
            if len(seed_files) > 1
            else seed_files[0]
        )
        try:
            df = pd.read_csv(csv_path, nrows=SOURCE_PREVIEW_ROWS)
        except Exception as e:
            st.error(f"Could not read `{csv_path}`: {e}")
            return
        st.write(f"`{csv_path.relative_to(REPO_ROOT)}` — first **{SOURCE_PREVIEW_ROWS}** rows.")
        st.dataframe(df, width="stretch")
        return

    db_path = ACT_DB if choice.startswith("ACT") else VRM_DB
    label = "act_system" if choice.startswith("ACT") else "vrm_system"

    if not db_path.is_file():
        st.warning(
            f"Missing `{db_path.relative_to(REPO_ROOT)}`. Run **`bash duckdb-setup/setup.sh`** from the repo root."
        )
        return

    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception as e:
        st.error(f"Could not open `{db_path}`: {e}")
        return

    try:
        tables = duckdb_tables(con)
    finally:
        con.close()

    if not tables:
        st.info("No tables found.")
        return

    table = st.selectbox(f"Table in {label}", tables, key=f"tbl_{label}")
    try:
        con = duckdb.connect(str(db_path), read_only=True)
        ident = table.replace('"', '""')
        df = con.sql(f'SELECT * FROM "{ident}" LIMIT {SOURCE_PREVIEW_ROWS}').df()
    except Exception as e:
        st.error(f"Preview failed: {e}")
        return
    finally:
        con.close()

    st.write(
        f"**{table}** — first **{SOURCE_PREVIEW_ROWS}** rows from `{db_path.relative_to(REPO_ROOT)}`."
    )
    st.dataframe(df, width="stretch")


def main() -> None:
    st.set_page_config(page_title="Zoo Analytics data viewer", layout="wide")
    st.title("Zoo Analytics — local data viewer")
    st.markdown(
        f"Repository root: `{REPO_ROOT}`  \n"
        "Run from the repo root: `streamlit run tools/data_viewer/app.py`."
    )

    tab_results, tab_sources = st.tabs(("dbt results", "Source systems"))
    with tab_results:
        render_dbt_results_tab()
    with tab_sources:
        render_source_systems_tab()


if __name__ == "__main__":
    main()
