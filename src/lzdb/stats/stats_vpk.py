import math
from collections import Counter, defaultdict

# ------------------------------------------------------------
# Identity Metrics for LZDB
# ------------------------------------------------------------

class IdentityMetrics:
    """
    Compute identity-related statistical metrics for a single field
    across a list of LZDBItem objects.
    """

    def __init__(self, items, field_name, time_field=None, missing_values=(None, "")):
        self.items = list(items)
        self.field_name = field_name
        self.time_field = time_field
        self.missing_values = set(missing_values)

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------

    def _values(self):
        return [getattr(it, self.field_name, None) for it in self.items]

    # ------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------

    def uniqueness(self):
        """
        Birthday-paradox style uniqueness score.
        """
        vals = self._values()
        non_missing = [v for v in vals if v not in self.missing_values]

        n = len(non_missing)
        distinct = len(set(non_missing))

        if distinct == 0 or n == 0:
            return 0.0

        # Approximate collision probability
        p_collision = 1.0
        for i in range(1, n):
            p_collision *= (1.0 - i / distinct)

        p_collision = 1.0 - p_collision
        return max(0.0, min(1.0, 1.0 - p_collision))

    def entropy(self):
        """
        Normalized Shannon entropy.
        """
        vals = self._values()
        non_missing = [v for v in vals if v not in self.missing_values]

        n = len(non_missing)
        if n == 0:
            return 0.0

        counts = Counter(non_missing)
        probs = [c / n for c in counts.values()]

        H = -sum(p * math.log(p) for p in probs)
        max_H = math.log(len(counts)) if counts else 1.0

        return H / max_H if max_H > 0 else 0.0

    def completeness(self):
        """
        Fraction of non-missing values.
        """
        vals = self._values()
        n = len(vals)
        if n == 0:
            return 0.0

        missing = sum(1 for v in vals if v in self.missing_values)
        return 1.0 - missing / n

    def volatility(self):
        """
        Fraction of value changes over time for each entity.
        Requires:
            - item.id
            - item.<time_field>
        """
        if self.time_field is None:
            return 0.0

        by_entity = defaultdict(list)

        for it in self.items:
            eid = getattr(it, "id", None)
            t = getattr(it, self.time_field, None)
            v = getattr(it, self.field_name, None)

            if eid is not None and t is not None:
                by_entity[eid].append((t, v))

        changes = 0
        total_steps = 0

        for eid, seq in by_entity.items():
            seq = sorted(seq, key=lambda x: x[0])
            for i in range(len(seq) - 1):
                total_steps += 1
                if seq[i][1] != seq[i + 1][1]:
                    changes += 1

        if total_steps == 0:
            return 0.0

        return changes / total_steps

    def stability(self):
        return 1.0 - self.volatility()

    # ------------------------------------------------------------
    # Composite score
    # ------------------------------------------------------------

    def score(self, w_U=0.4, w_H=0.2, w_S=0.2, w_C=0.2):
        U = self.uniqueness()
        H = self.entropy()
        S = self.stability()
        C = self.completeness()

        return w_U * U + w_H * H + w_S * S + w_C * C


# ------------------------------------------------------------
# Mutual Information for composite vPKs
# ------------------------------------------------------------

def mutual_information(items, field1, field2, missing_values=(None, "")):
    vals1 = [getattr(it, field1, None) for it in items]
    vals2 = [getattr(it, field2, None) for it in items]

    pairs = [
        (v1, v2)
        for v1, v2 in zip(vals1, vals2)
        if v1 not in missing_values and v2 not in missing_values
    ]

    if not pairs:
        return 0.0

    n = len(pairs)
    c1 = Counter(v1 for v1, _ in pairs)
    c2 = Counter(v2 for _, v2 in pairs)
    c12 = Counter(pairs)

    mi = 0.0
    for (v1, v2), c in c12.items():
        p12 = c / n
        p1 = c1[v1] / n
        p2 = c2[v2] / n
        mi += p12 * math.log(p12 / (p1 * p2))

    return mi


# ------------------------------------------------------------
# vPK Reassessment
# ------------------------------------------------------------

def reassess_vpk(items, candidate_fields, time_field=None,
                 score_threshold=0.6, improvement_margin=0.1):
    """
    Compute identity scores for candidate fields and suggest a new vPK
    if one significantly outperforms the current vPK.

    items: list of LZDBItem
    candidate_fields: list of field names
    time_field: field used for volatility (e.g. "timestamp")
    """

    # Compute scores
    scores = {}
    for field in candidate_fields:
        metrics = IdentityMetrics(items, field, time_field=time_field)
        scores[field] = metrics.score()

    # Current vPK is stored on the collection object
    collection = items[0].collection if items else None
    current_vpk = getattr(collection, "vpk", None)
    current_score = scores.get(current_vpk, 0.0) if current_vpk else 0.0

    # Best candidate
    best_field = max(scores, key=scores.get)
    best_score = scores[best_field]

    # Decide whether to suggest reassessment
    if best_score >= score_threshold and best_score >= current_score + improvement_margin:
        return {
            "current_vpk": current_vpk,
            "current_score": current_score,
            "suggested_vpk": best_field,
            "suggested_score": best_score,
            "scores": scores,
        }

    return {
        "current_vpk": current_vpk,
        "current_score": current_score,
        "suggested_vpk": None,
        "suggested_score": None,
        "scores": scores,
    }
