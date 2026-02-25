import streamlit as st
import pandas as pd
import csv
import json
import re
from datetime import date, datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.config import ORION_COLUMNS, OUTPUT_ROOT
from src.export import append_to_master

MASTER_FILE = OUTPUT_ROOT / "orion_master.csv"
FORCES_MASTER_FILE = OUTPUT_ROOT / "orion_forces_master.csv"

FORCE_TYPE_INFO = {
    "MT": {"name": "Megatrends", "color": "#3B82F6", "icon": "🌊"},
    "T": {"name": "Trends", "color": "#10B981", "icon": "📈"},
    "WS": {"name": "Weak Signals", "color": "#F59E0B", "icon": "📡"},
    "WC": {"name": "Wildcards", "color": "#EF4444", "icon": "🃏"},
}

st.set_page_config(
    page_title="ORION Signal Review",
    page_icon="🔭",
    layout="wide"
)

def get_available_dates():
    dates = []
    if OUTPUT_ROOT.exists():
        for folder in OUTPUT_ROOT.iterdir():
            if folder.is_dir() and folder.name != "__pycache__":
                try:
                    datetime.strptime(folder.name, "%Y-%m-%d")
                    dates.append(folder.name)
                except ValueError:
                    pass
    return sorted(dates, reverse=True)


def get_pending_file(date_str: str) -> Path:
    new_path = OUTPUT_ROOT / date_str / "orion_daily_pending_review.csv"
    if new_path.exists():
        return new_path
    old_path = OUTPUT_ROOT / date_str / "orion_daily_review.csv"
    if old_path.exists():
        return old_path
    return new_path


def load_pending_signals(date_str: str) -> pd.DataFrame:
    pending_file = get_pending_file(date_str)
    if not pending_file.exists():
        return pd.DataFrame()
    return pd.read_csv(pending_file)


def load_master_ids() -> set:
    if not MASTER_FILE.exists():
        return set()
    df = pd.read_csv(MASTER_FILE)
    return set(df["id"].astype(str).tolist())


def load_forces() -> pd.DataFrame:
    if not FORCES_MASTER_FILE.exists():
        return pd.DataFrame()
    return pd.read_csv(FORCES_MASTER_FILE)


def load_signals_master() -> pd.DataFrame:
    if not MASTER_FILE.exists():
        return pd.DataFrame()
    return pd.read_csv(MASTER_FILE)


def extract_source_signal_ids(tags_value) -> list:
    if pd.isna(tags_value) or not tags_value:
        return []
    try:
        if isinstance(tags_value, str):
            tags_list = json.loads(tags_value)
        else:
            tags_list = tags_value
        for tag in tags_list:
            if isinstance(tag, str) and tag.startswith("synthesized_from:"):
                ids_str = tag.replace("synthesized_from:", "")
                return [id.strip() for id in ids_str.split(",") if id.strip()]
    except (json.JSONDecodeError, TypeError):
        match = re.search(r"synthesized_from:([^\"'\]]+)", str(tags_value))
        if match:
            return [id.strip() for id in match.group(1).split(",") if id.strip()]
    return []


def promote_signals(df: pd.DataFrame, indices: list) -> int:
    if not indices:
        return 0
    
    rows_to_promote = df.iloc[indices].to_dict("records")
    orion_rows = [{k: row.get(k, "") for k in ORION_COLUMNS} for row in rows_to_promote]
    
    added = append_to_master(orion_rows, MASTER_FILE)
    return added


st.title("🔭 ORION Signal Review Dashboard")

available_dates = get_available_dates()

if not available_dates:
    st.warning("No output dates found. Run the pipeline first to generate signals.")
    st.stop()

col1, col2 = st.columns([2, 4])

with col1:
    selected_date = st.selectbox("Select Date", available_dates, index=0)

df = load_pending_signals(selected_date)

if df.empty:
    st.info(f"No pending signals for {selected_date}")
    st.stop()

master_ids = load_master_ids()
df["already_promoted"] = df["id"].astype(str).isin(master_ids)

with col2:
    st.metric("Pending Signals", len(df))

st.markdown("---")

display_cols = ["title", "steep", "dimension", "priority_index", "credibility_score", "source"]
available_cols = [c for c in display_cols if c in df.columns]

if "selected_indices" not in st.session_state:
    st.session_state.selected_indices = set()

col_config = {
    "title": st.column_config.TextColumn("Title", width="large"),
    "steep": st.column_config.TextColumn("STEEP", width="small"),
    "dimension": st.column_config.TextColumn("Dimension", width="medium"),
    "priority_index": st.column_config.NumberColumn("Priority", format="%.1f"),
    "credibility_score": st.column_config.NumberColumn("Credibility", format="%.0f"),
    "source": st.column_config.TextColumn("Source", width="medium"),
}

st.subheader("Pending Signals")

not_promoted = df[~df["already_promoted"]].copy()
already_promoted = df[df["already_promoted"]].copy()

if not not_promoted.empty:
    not_promoted = not_promoted.reset_index(drop=True)
    
    select_all = st.checkbox("Select All", key="select_all")
    
    not_promoted.insert(0, "Select", select_all)
    
    edited_df = st.data_editor(
        not_promoted[["Select"] + available_cols],
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=select_all),
            **col_config
        },
        hide_index=False,
        use_container_width=True,
        key="signal_editor"
    )
    
    selected_mask = edited_df["Select"] == True
    selected_count = selected_mask.sum()
    
    col_btn1, col_btn2, col_btn3 = st.columns([2, 2, 4])
    
    with col_btn1:
        if st.button(f"✅ Promote Selected ({selected_count})", disabled=selected_count == 0, type="primary"):
            selected_indices = edited_df[selected_mask].index.tolist()
            original_indices = not_promoted.iloc[selected_indices].index.tolist()
            
            rows_to_add = not_promoted.iloc[selected_indices].to_dict("records")
            orion_rows = [{k: row.get(k, "") for k in ORION_COLUMNS} for row in rows_to_add]
            added = append_to_master(orion_rows, MASTER_FILE)
            
            if added > 0:
                st.success(f"Promoted {added} signal(s) to master file!")
                st.rerun()
            else:
                st.warning("No new signals added (may already exist in master)")
    
    with col_btn2:
        if st.button("🚀 Promote All", type="secondary"):
            rows_to_add = not_promoted.to_dict("records")
            orion_rows = [{k: row.get(k, "") for k in ORION_COLUMNS} for row in rows_to_add]
            added = append_to_master(orion_rows, MASTER_FILE)
            
            if added > 0:
                st.success(f"Promoted {added} signal(s) to master file!")
                st.rerun()
            else:
                st.warning("No new signals added")

else:
    st.success("All signals from this date have been promoted!")

if not already_promoted.empty:
    with st.expander(f"Already Promoted ({len(already_promoted)} signals)"):
        st.dataframe(
            already_promoted[available_cols],
            column_config=col_config,
            use_container_width=True
        )

st.markdown("---")

with st.expander("📊 Master File Stats"):
    if MASTER_FILE.exists():
        master_df = pd.read_csv(MASTER_FILE)
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            st.metric("Total Signals in Master", len(master_df))
        with col_s2:
            if "steep" in master_df.columns:
                st.write("**By STEEP:**")
                st.write(master_df["steep"].value_counts())
        with col_s3:
            if "dimension" in master_df.columns:
                st.write("**By Dimension:**")
                st.write(master_df["dimension"].value_counts().head(10))
    else:
        st.info("No master file yet. Promote some signals to create it.")

st.markdown("---")
st.header("🔮 Curated Forces")

forces_df = load_forces()
signals_df = load_signals_master()

if forces_df.empty:
    st.info("No curated forces yet. Run the pipeline with --synthesize to generate forces from accepted signals.")
else:
    signals_lookup = {}
    if not signals_df.empty:
        for _, row in signals_df.iterrows():
            signals_lookup[str(row.get("id", ""))] = row
    
    force_type_counts = forces_df["type"].value_counts().to_dict() if "type" in forces_df.columns else {}
    
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    for i, (ftype, info) in enumerate(FORCE_TYPE_INFO.items()):
        count = force_type_counts.get(ftype, 0)
        with [col_f1, col_f2, col_f3, col_f4][i]:
            st.metric(f"{info['icon']} {info['name']}", count)
    
    st.markdown("---")
    
    for ftype, info in FORCE_TYPE_INFO.items():
        type_forces = forces_df[forces_df["type"] == ftype] if "type" in forces_df.columns else pd.DataFrame()
        
        if type_forces.empty:
            continue
        
        st.subheader(f"{info['icon']} {info['name']} ({len(type_forces)})")
        
        for idx, force in type_forces.iterrows():
            force_title = force.get("title", "Untitled Force")
            force_text = force.get("text", "")
            force_dimension = force.get("dimension", "")
            force_steep = force.get("steep", "")
            
            source_ids = extract_source_signal_ids(force.get("tags", ""))
            
            with st.expander(f"**{force_title}**"):
                st.markdown(f"*{force_text}*")
                
                if force_dimension or force_steep:
                    st.caption(f"Dimension: {force_dimension} | STEEP: {force_steep}")
                
                if source_ids:
                    st.markdown("**Source Signals:**")
                    for sig_id in source_ids:
                        if sig_id in signals_lookup:
                            sig = signals_lookup[sig_id]
                            sig_title = sig.get("title", sig_id)
                            sig_source = sig.get("source", "")
                            st.markdown(f"- {sig_title}")
                            if sig_source:
                                st.caption(f"  Source: {sig_source}")
                        else:
                            st.markdown(f"- Signal ID: {sig_id}")
                else:
                    st.caption("No source signal traceability available")
