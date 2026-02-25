from typing import List, Dict, Any
import pandas as pd

from .config import SOURCES_PATH, SOURCES_SHEET


def load_sources(path=SOURCES_PATH, sheet=SOURCES_SHEET) -> List[Dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet)
    cols = {c.lower(): c for c in df.columns}

    def col(name: str) -> str:
        return cols.get(name, name)

    sources = []
    for _, row in df.iterrows():
        name = str(row.get(col("source_name"), "")).strip()
        link = str(row.get(col("source_link"), "")).strip()
        tier = str(row.get(col("tier"), "")).strip().upper() or "C"
        sources.append(
            {
                "source_name": name,
                "source_link": link,
                "tier": tier,
                "crawl_method": row.get(col("crawl_method")) if col("crawl_method") in df.columns else None,
                "frequency": row.get(col("frequency")) if col("frequency") in df.columns else None,
                "priority": row.get(col("priority")) if col("priority") in df.columns else None,
                "notes": row.get(col("notes")) if col("notes") in df.columns else None,
            }
        )
    return sources
