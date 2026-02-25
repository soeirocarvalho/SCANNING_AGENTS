import json
from collections import Counter
from typing import List, Dict, Any, Tuple

import pandas as pd

from .config import DRIVING_FORCES_PATH


def _parse_tags(value: Any) -> List[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return [str(v).strip() for v in data if str(v).strip()]
        except json.JSONDecodeError:
            return [t.strip() for t in text.split(',') if t.strip()]
    return []


def load_orion_corpus(path=DRIVING_FORCES_PATH) -> Tuple[List[Dict[str, Any]], str, List[str], List[str]]:
    df = pd.read_excel(path)
    project_id = df["project_id"].mode().iloc[0]
    dimensions = sorted({v for v in df["dimension"].dropna().unique().tolist()})

    tags_vocab: Counter = Counter()
    records = []
    for _, row in df.iterrows():
        tags = _parse_tags(row.get("tags"))
        tags_vocab.update(tags)
        records.append(
            {
                "id": str(row.get("id")) if pd.notna(row.get("id")) else "",
                "title": str(row.get("title")) if pd.notna(row.get("title")) else "",
                "text": str(row.get("text")) if pd.notna(row.get("text")) else "",
                "type": str(row.get("type")) if pd.notna(row.get("type")) else "",
                "scope": str(row.get("scope")) if pd.notna(row.get("scope")) else "",
            }
        )

    tag_list = sorted(tags_vocab.keys())
    return records, project_id, dimensions, tag_list
