"""
MappingEngine — loads IID-SID-ITEM.csv once, deduplicates, and provides
fast SID→IID→DDL and IID→DDL lookups.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


class MappingEngine:
    """
    Immutable, in-memory lookup built from IID-SID-ITEM.csv.

    The CSV has hundreds of duplicate SID entries; we keep the first
    occurrence per SID (deterministic across runs given a sorted file).

    DDL column derivation:
        IID  "E2_I_042"  →  "co" + "E2_I_042".replace("_", "")  →  "coE2I042"
    """

    def __init__(self, csv_path: str | Path) -> None:
        df = pd.read_csv(
            str(csv_path),
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
        )
        df.columns = [c.strip() for c in df.columns]
        df = df[["ItmSID", "ItmIID"]].dropna()
        df["ItmSID"] = df["ItmSID"].str.strip()
        df["ItmIID"] = df["ItmIID"].str.strip()
        df = df[df["ItmSID"] != ""].drop_duplicates(subset=["ItmSID"], keep="first")

        self._sid_to_iid: dict[str, str] = dict(zip(df["ItmSID"], df["ItmIID"]))
        self._iid_set: set[str] = set(df["ItmIID"])
        self._iid_to_ddl: dict[str, str] = {
            iid: "co" + iid.replace("_", "") for iid in self._iid_set
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, col: str) -> str | None:
        """
        Map a column name to its DDL column name.

        Accepts:
          - IID directly  (e.g. "E2_I_042"  → "coE2I042")
          - SID           (e.g. "08_02"     → "coE2I042")

        Returns None if the column is not in the mapping.
        """
        if col in self._iid_to_ddl:
            return self._iid_to_ddl[col]
        iid = self._sid_to_iid.get(col)
        if iid:
            return self._iid_to_ddl.get(iid)
        return None

    def is_sid(self, col: str) -> bool:
        return col in self._sid_to_iid

    def is_iid(self, col: str) -> bool:
        return col in self._iid_set

    def sid_to_iid(self, sid: str) -> str | None:
        return self._sid_to_iid.get(sid)

    def iid_to_ddl(self, iid: str) -> str | None:
        return self._iid_to_ddl.get(iid)

    def get_sample_sids(self, n: int = 20) -> list[str]:
        return list(self._sid_to_iid.keys())[:n]

    def get_sample_iids(self, n: int = 20) -> list[str]:
        return list(self._iid_set)[:n]
