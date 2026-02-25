import hashlib
from typing import List


def _hash_to_vector(text: str, dim: int = 64) -> List[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    vals = [b / 255.0 for b in digest]
    out: List[float] = []
    while len(out) < dim:
        out.extend(vals)
    return out[:dim]


def embed_texts(texts: List[str]) -> List[List[float]]:
    return [_hash_to_vector(t) for t in texts]
