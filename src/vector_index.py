import math
import re
from typing import List, Dict, Any


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-zA-Z0-9]{3,}", text.lower())


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _cosine(counter_a: Dict[str, int], counter_b: Dict[str, int]) -> float:
    if not counter_a or not counter_b:
        return 0.0
    inter = set(counter_a) & set(counter_b)
    dot = sum(counter_a[t] * counter_b[t] for t in inter)
    norm_a = math.sqrt(sum(v * v for v in counter_a.values()))
    norm_b = math.sqrt(sum(v * v for v in counter_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class VectorIndex:
    def __init__(self):
        self.records: List[Dict[str, Any]] = []
        self._token_sets: List[set] = []
        self._token_counts: List[Dict[str, int]] = []

    def build(self, records: List[Dict[str, Any]]):
        self.records = records
        self._token_sets = []
        self._token_counts = []
        for rec in records:
            text = f"{rec.get('title','')} {rec.get('text','')}"
            tokens = _tokenize(text)
            self._token_sets.append(set(tokens))
            counts: Dict[str, int] = {}
            for t in tokens:
                counts[t] = counts.get(t, 0) + 1
            self._token_counts.append(counts)

    def query(self, text: str, top_k: int = 5) -> List[Dict[str, Any]]:
        q_tokens = _tokenize(text)
        q_set = set(q_tokens)
        q_counts: Dict[str, int] = {}
        for t in q_tokens:
            q_counts[t] = q_counts.get(t, 0) + 1

        scored = []
        for rec, r_set, r_counts in zip(self.records, self._token_sets, self._token_counts):
            j = _jaccard(q_set, r_set)
            c = _cosine(q_counts, r_counts)
            similarity = (j + c) / 2.0
            scored.append((similarity, rec))

        scored.sort(key=lambda x: x[0], reverse=True)
        neighbors = []
        for sim, rec in scored[:top_k]:
            neighbors.append(
                {
                    "id": rec.get("id"),
                    "title": rec.get("title"),
                    "type": rec.get("type"),
                    "scope": rec.get("scope"),
                    "similarity": float(round(sim, 4)),
                }
            )
        return neighbors
