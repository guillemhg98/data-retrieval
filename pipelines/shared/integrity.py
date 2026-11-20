"""Bounded source reconciliation using small local daily aggregates.

Matching counts are evidence of consistency, never proof of source completeness.
No raw history is downloaded unless a checked day differs from the local total.
"""
from __future__ import annotations

import json
import hashlib
import logging

import pandas as pd

from .imputation import IMPUTED_COL, is_imputed_series

logger = logging.getLogger(__name__)


def suspect_days(df, total_col):
    """Return date -> reasons using observed weekly and prior-year baselines."""
    if df.empty or total_col not in df:
        return {}
    data = df.copy()
    data["timestamp"] = pd.to_datetime(data.timestamp).dt.normalize()
    data = data.sort_values("timestamp").set_index("timestamp")
    if data.index.has_duplicates:
        raise ValueError("Duplicate daily timestamps; reconcile the final file first")
    values = pd.to_numeric(data[total_col], errors="coerce")
    imputed = is_imputed_series(data[IMPUTED_COL]) if IMPUTED_COL in data else pd.Series(False, index=data.index)
    observed = values.mask(imputed)
    calendar = pd.date_range(data.index.min(), data.index.max())
    daily = observed.reindex(calendar)
    weekly = pd.DataFrame({i: daily.shift(7 * i) for i in range(1, 9)})
    weekly_baseline = weekly.median(axis=1).where(weekly.count(axis=1) >= 4)
    prior_center = calendar - pd.DateOffset(years=1)
    prior = {}
    for offset in range(-28, 29):
        dates = prior_center + pd.Timedelta(days=offset)
        prior[offset] = pd.Series(
            observed.reindex(dates).to_numpy(), index=calendar,
        ).where(dates.dayofweek == calendar.dayofweek)
    prior = pd.DataFrame(prior)
    prior_baseline = prior.median(axis=1).where(prior.count(axis=1) >= 4)
    baseline = pd.concat([weekly_baseline, prior_baseline], axis=1).max(axis=1)
    low = daily.notna() & (baseline >= 100) & (daily < 0.1 * baseline)
    reasons = {day: ["extreme_drop"] for day in calendar[low]}
    for day in calendar.difference(data.index):
        reasons[day] = ["missing_day"]
    for day in data.index[values.isna() | (values < 0)]:
        reasons[day] = ["invalid_total"]
    for day in data.index[imputed]:
        reasons[day] = ["imputed"]
    # Detect legacy, unflagged averaging which flattens the weekly cycle.
    for month, group in observed.dropna().groupby(observed.dropna().index.to_period("M")):
        if len(group) < 14:
            continue
        previous = observed[observed.index.to_period("M") == month - 12].dropna()
        if len(previous) < 14:
            continue
        def ratio(series):
            work = series[series.index.dayofweek < 5].median()
            weekend = series[series.index.dayofweek >= 5].median()
            return weekend / work if work > 0 else float("nan")
        if ratio(previous) < 0.35 and ratio(group) > 0.65:
            for day in group.index:
                reasons.setdefault(day, []).append("flattened_weekly_cycle")
    return reasons


def date_ranges(days):
    """Coalesce adjacent days into inclusive ranges."""
    ranges = []
    for day in sorted(set(pd.to_datetime(list(days)).normalize())):
        if ranges and day == ranges[-1][1] + pd.Timedelta(days=1):
            ranges[-1] = (ranges[-1][0], day)
        else:
            ranges.append((day, day))
    return ranges


class IntegrityReview:
    def __init__(self, final_store, total_col, *, recent_days=7, max_days=120, cooldown_days=7, input_signature=None):
        if min(recent_days, max_days, cooldown_days) < 1:
            raise ValueError("Integrity review limits must be positive")
        self.store = final_store
        self.total_col = total_col
        self.recent_days = recent_days
        self.max_days = max_days
        self.cooldown_days = cooldown_days
        self.path = final_store.output_file.with_name(final_store.output_file.stem + "_integrity.json")
        self.state = json.loads(self.path.read_text(encoding="utf-8")) if self.path.exists() else {"checks": {}}
        self.df = final_store.load_final()
        self.reasons = suspect_days(self.df, total_col)
        if input_signature is not None:
            previous = self.state.get("input_signature")
            if previous is not None and previous != input_signature:
                self.state["checks"] = {}
                self.state["pending_rebuild"] = [str(pd.Timestamp(d).date()) for d in self.df.timestamp]
            self.state["input_signature"] = input_signature
        self.pending_rebuild = set(pd.to_datetime(self.state.get("pending_rebuild", [])))
        self.changed_dates = []
        self.expected_counts = {}
        self.checked_days = []

    def plan(self, conn, schema, table, date_column, up_column, min_day, max_day,
             *, new_start=None, review_ranges=None, diagnosis_column=None, now=None, observed_source_end=None):
        """One grouped SQL query for a bounded set of existing calendar days."""
        now = pd.Timestamp.now() if now is None else pd.Timestamp(now)
        min_day, max_day = pd.Timestamp(min_day).normalize(), pd.Timestamp(max_day).normalize()
        forced = set()
        for start, end in review_ranges or []:
            forced.update(pd.date_range(start, end))
        source_end = max_day if observed_source_end is None else pd.Timestamp(observed_source_end).normalize()
        recent_end = min(max_day, source_end)
        recent = set(pd.date_range(max(min_day, recent_end - pd.Timedelta(days=self.recent_days - 1)), recent_end))
        candidates = forced | recent | set(self.reasons) | self.pending_rebuild
        checks = self.state.setdefault("checks", {})
        local = self.df.set_index("timestamp") if not self.df.empty else pd.DataFrame()
        if not self.df.empty:
            # A source MAX that moved backwards must not hide deleted cached days.
            observed = ~is_imputed_series(local[IMPUTED_COL]) if IMPUTED_COL in local else pd.Series(True, index=local.index)
            candidates.update(local.index[(local.index > source_end) & observed])
        pending = []
        for day in sorted(candidates):
            if day < min_day or day > max_day or (new_start is not None and day >= new_start):
                continue
            if day > source_end and (day not in local.index or (IMPUTED_COL in local and is_imputed_series(local.loc[[day], IMPUTED_COL]).iloc[0])):
                continue
            previous = checks.get(day.date().isoformat(), {})
            checked = pd.to_datetime(previous.get("checked_at"))
            ttl = pd.Timedelta(days=1 if day in recent else self.cooldown_days)
            value = local.loc[day, self.total_col] if day in local.index else None
            # Do not trust a cached check after the local aggregate was edited.
            unchanged = pd.notna(value) and previous.get("local_total") == float(value)
            if day not in forced and day not in self.pending_rebuild and unchanged and pd.notna(checked) and now - checked < ttl:
                continue
            pending.append(day)
        pending.sort(key=lambda d: (
            0 if d in forced else 1 if d in recent else 2,
            checks.get(str(d.date()), {}).get("checked_at", "") if d in forced else "",
            d,
        ))
        selected = pending[:self.max_days]
        self.state["deferred_days"] = len(pending) - len(selected)
        if not selected:
            return []
        # Parameterized, sargable date predicates. Group only inside SQL.
        ranges = date_ranges(selected)
        predicates, params = [], []
        for start, end in ranges:
            predicates.append(f"([{date_column}] >= ? AND [{date_column}] < ?)")
            params.extend([start, end + pd.Timedelta(days=1)])
        code_filter = ""
        if diagnosis_column:
            code_filter = f"AND [{diagnosis_column}] IS NOT NULL AND LTRIM(RTRIM(CAST([{diagnosis_column}] AS VARCHAR(50)))) <> ''"
        query = f"""SELECT CAST([{date_column}] AS date) AS [timestamp],
            COUNT_BIG(*) AS source_count,
            COUNT(DISTINCT [{up_column}]) AS source_up_count
            FROM [{schema}].[{table}]
            WHERE ({' OR '.join(predicates)}) {code_filter}
            GROUP BY CAST([{date_column}] AS date)"""
        summary = pd.read_sql_query(query, conn, params=params)
        required = {"timestamp", "source_count", "source_up_count"}
        if not required.issubset(summary):
            raise ValueError("Incomplete source integrity summary")
        if summary["timestamp"].duplicated().any():
            raise ValueError("Duplicate dates in source integrity summary")
        summary["timestamp"] = pd.to_datetime(summary["timestamp"]).dt.normalize()
        source = summary.set_index("timestamp")
        changed = []
        for day in selected:
            key = day.date().isoformat()
            count = int(source.loc[day, "source_count"]) if day in source.index else 0
            ups = int(source.loc[day, "source_up_count"]) if day in source.index else 0
            old_check = checks.get(key, {})
            exists = day in local.index
            estimated = exists and IMPUTED_COL in local and bool(is_imputed_series(local.loc[[day], IMPUTED_COL]).iloc[0])
            value = local.loc[day, self.total_col] if exists else None
            mismatch = not exists or estimated or pd.isna(value) or float(value) != count
            coverage_changed = "source_up_count" in old_check and old_check["source_up_count"] != ups
            if (count > 0 and (mismatch or coverage_changed or day in self.pending_rebuild)) or (count == 0 and exists and not estimated):
                changed.append(day)
            checks[key] = {
                "checked_at": now.isoformat(), "source_count": count, "source_up_count": ups,
                "status": "suspect" if day in self.reasons else "source_count_checked",
                "reasons": self.reasons.get(day, []),
            }
            self.expected_counts[day] = count
        self.state["reviewed_days"] = len(selected)
        self.state["changed_days"] = len(changed)
        logger.info("Integrity review: %d grouped days checked, %d changed, %d deferred",
                    len(selected), len(changed), self.state["deferred_days"])
        self.changed_dates = changed
        self.checked_days = selected
        return [(start, end + pd.Timedelta(days=1), end) for start, end in date_ranges(changed)]

    def validate_chunks(self, manager):
        """Do not commit a review if extraction differs from the SQL summary."""
        if not self.changed_dates:
            return
        frames = [pd.read_parquet(p) for p in manager.iter_incremental_files()]
        actual = combine_daily_snapshots(frames).set_index("timestamp") if frames else pd.DataFrame()
        for day in self.changed_dates:
            expected = self.expected_counts[day]
            if expected == 0:
                continue
            if day not in actual.index or self.total_col not in actual or actual.loc[day, self.total_col] != expected:
                raise ValueError(f"Source changed or extraction incomplete during review: {day.date()}")

    def commit(self):
        """Persist review checkpoints only after successful final publication."""
        current = self.store.load_final()
        self.state["suspect_days"] = {str(k.date()): v for k, v in suspect_days(current, self.total_col).items()}
        local = current.set_index("timestamp") if not current.empty else pd.DataFrame()
        for day in self.checked_days:
            value = local.loc[day, self.total_col] if day in local.index else None
            check = self.state["checks"][str(day.date())]
            check["local_total"] = float(value) if pd.notna(value) else None
            check["reasons"] = self.state["suspect_days"].get(str(day.date()), [])
            check["status"] = "suspect" if check["reasons"] else "source_count_checked"
        self.state["pending_rebuild"] = [str(d.date()) for d in sorted(self.pending_rebuild - set(self.checked_days))]
        temp = self.path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(self.state, indent=2), encoding="utf-8")
        temp.replace(self.path)


def input_signature(source, mapping, selections):
    """Fingerprint inputs to avoid reusing aggregates after their meaning changes."""
    columns = [c for c in ("Codi UP", "RS") if c in mapping]
    normalized = mapping[columns].astype("string").fillna("")
    if columns:
        normalized = normalized.sort_values(columns)
    lookup = normalized.to_csv(index=False)
    payload = json.dumps({"aggregation_version": 2, "source": source, "mapping": lookup,
                          "selections": selections}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def combine_daily_snapshots(frames, timestamp_col="timestamp"):
    """Latest snapshot wins per day/column; missing columns are never zeros."""
    combined = pd.DataFrame()
    for frame in frames:
        if frame.empty:
            continue
        indexed = frame.set_index(timestamp_col)
        if indexed.index.has_duplicates:
            raise ValueError("Snapshot contains duplicate dates")
        if any(c in indexed for c in ("DEMAND__TOTAL", "DIAGNOSIS__TOTAL")):
            # These are complete daily snapshots, not additive fragments.
            combined = combined.loc[~combined.index.isin(indexed.index)]
            combined = pd.concat([combined, indexed], axis=0, sort=False)
        else:
            combined = indexed.combine_first(combined)
    combined.index.name = timestamp_col
    return combined.sort_index().reset_index()
