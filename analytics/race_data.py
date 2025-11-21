"""High-level race data container built on analytics utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass
class RaceData:
    """
    Container for all derived data for a single race.

    Parameters
    ----------
    race_id : int or str
        Four-digit race identifier embedded in the clean data filenames.

    base_dir : str or pathlib.Path, optional
        Project root directory. If not provided, defaults to the parent
        of the ``analytics`` package (i.e. ``..`` relative to this file).

    Attributes
    ----------
    race_id : str
        Normalised race identifier used for file matching.

    section : pandas.DataFrame
        Clean section results dataframe loaded from
        ``cleandata/section results``.

    results : pandas.DataFrame
        Clean race results dataframe loaded from ``cleandata/results``.

    timing : pandas.DataFrame
        Per-lap timing table.

    pit_stops : pandas.DataFrame
        Pit stop metadata per lap.

    flags : pandas.DataFrame
        Per-car/per-lap flag classifications.

    overtakes : pandas.DataFrame
        Overtake feature table.
    """

    race_id: str
    base_dir: Path
    section: pd.DataFrame
    results: pd.DataFrame
    timing: pd.DataFrame
    pit_stops: pd.DataFrame
    flags: pd.DataFrame

    def __init__(
        self,
        race_id: int | str,
        base_dir: Optional[str | Path] = None,
    ) -> None:
        race_id_str = str(race_id)

        if base_dir is None:
            # analytics/ -> project root
            base_dir = Path(__file__).resolve().parents[1]
        else:
            base_dir = Path(base_dir).resolve()

        self.base_dir = base_dir
        self.race_id = race_id_str

        section_path = self._find_parquet(
            base_dir / "cleandata" / "section results",
            prefix="sectionresults_",
            race_id=race_id_str,
        )
        results_path = self._find_parquet(
            base_dir / "cleandata" / "results",
            prefix="results_",
            race_id=race_id_str,
        )

        self.section = pd.read_parquet(section_path)
        self.results = pd.read_parquet(results_path)

        # Tag raw data with race id
        self.section["RaceID"] = race_id_str
        self.results["RaceID"] = race_id_str

        # Build derived tables using existing analytics utilities
        self.timing = self._build_lap_timing()
        self.pit_stops = self._build_pit_stops()
        self.flags = self._build_flags_data()

        # Tag derived tables with race id
        self.timing["RaceID"] = race_id_str
        self.pit_stops["RaceID"] = race_id_str
        self.flags["RaceID"] = race_id_str
 
    def _parse_elapsed_time(self, value):
        parts = str(value).split(":")
        hours, minutes, seconds = parts

        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)

    def _build_lap_timing(self) -> pd.DataFrame:
        section = self.section.copy()
        results = self.results.copy()

        section["Car"] = section["Car"].astype(str)
        results["Car"] = results["Car"].astype(str)

        merged = section.merge(results[["Car", "Laps"]], on="Car", how="inner")
        merged = merged.loc[merged["Lap"] <= merged["Laps"]].copy()

        lap_sections = merged.loc[
            merged["Section"] == "Lap",
            ["Car", "Driver", "Lap", "Flag", "Time"],
        ]

        lap_sections = lap_sections.rename(columns={"Time": "LapTime"})

        totals = (
            lap_sections
            .groupby(["Car", "Driver"], as_index=False)["LapTime"]
            .agg(sum="sum", count="count")
        )

        results["ElapsedSeconds"] = results["Elapsed Time"].apply(
            self._parse_elapsed_time
        )

        leader_gaps = results.merge(totals, on="Car", how="inner")

        leader_gaps["Gap"] = leader_gaps["ElapsedSeconds"] - leader_gaps["sum"]

        lap_zero = leader_gaps[["Car", "Driver", "Gap"]].rename(
            columns={"Gap": "LapTime"}
        )
        lap_zero["Lap"] = 0
        lap_zero["Flag"] = "Green"

        times = (
            pd.concat([lap_zero, lap_sections], ignore_index=True, sort=False)
            .sort_values(["Lap", "Car"])
            .reset_index(drop=True)
        )

        times["RaceTime"] = times.groupby("Car")["LapTime"].cumsum()
        times = times.sort_values("RaceTime").reset_index(drop=True)
        times["Gap"] = times["RaceTime"].diff()

        times.rename(columns={"Lap": "LapCompleted"}, inplace=True)
        times["LapStarted"] = times["LapCompleted"] + 1

        ordered_columns = [
            "Car",
            "Driver",
            "LapStarted",
            "LapCompleted",
            "LapTime",
            "RaceTime",
            "Gap",
            "Flag",
        ]

        return times[ordered_columns]

    def _build_pit_stops(self) -> pd.DataFrame:
        section = self.section.copy()
        times = self.timing.copy()

        section["Car"] = section["Car"].astype(str)
        times["Car"] = times["Car"].astype(str)

        inlaps = (
            section.loc[section["Section"] == "SF to PI", ["Car", "Lap"]]
            .rename(columns={"Lap": "LapStarted"})
            .copy()
        )
        inlaps["InLap"] = 1

        outlaps = (
            section.loc[
                (section["Lap"] > 1) & (section["Section"] == "PO to SF"),
                ["Car", "Lap"],
            ]
            .rename(columns={"Lap": "LapStarted"})
            .copy()
        )
        outlaps["OutLap"] = 1

        ps = (
            times
            .merge(inlaps, on=["Car", "LapStarted"], how="left")
            .merge(outlaps, on=["Car", "LapStarted"], how="left")
        )

        ps["LastPitLap"] = ps.loc[ps["OutLap"] == 1, "LapStarted"]
        ps.loc[ps["LapStarted"] == 1, "LastPitLap"] = 1
        ps["LastPitLap"] = ps.groupby("Car")["LastPitLap"].ffill()

        ps["LapsSincePit"] = (ps["LapStarted"] - ps["LastPitLap"]).astype(int)

        ps["InLap"] = ps["InLap"].fillna(0).astype(int)
        ps["OutLap"] = ps["OutLap"].fillna(0).astype(int)

        return ps

    def _build_flags_data(self) -> pd.DataFrame:
        df = self.section.copy()
        df["Car"] = df["Car"].astype(str)

        flags = (
            df
            .groupby(["Car", "Lap"])["Flag"]
            .apply(list)
            .rename("SectionFlags")
            .reset_index()
        )

        def _aggregate_flags(values):
            has_green = "Green" in values
            has_yellow = "Yellow" in values
            has_red = "Red" in values

            if has_yellow and has_green:
                return "Yellow Thrown"
            if has_red:
                return "Red"
            if has_yellow:
                return "Yellow"
            return "Green"

        flags["Flag"] = flags["SectionFlags"].apply(_aggregate_flags)

        return flags

    def _build_overtake_features(self) -> pd.DataFrame:
        # Placeholder until overtake feature logic is implemented
        return pd.DataFrame()

    @staticmethod
    def _find_parquet(directory: Path, prefix: str, race_id: str) -> Path:
        """
        Locate a parquet file matching the given race_id.

        The expected naming pattern is:

            {prefix}YYYY-MM-DD_{race_id}_<description>.pq

        e.g.
            sectionresults_2017-04-09_3678_Toyota Grand Prix....pq
            results_2017-04-09_3678_Toyota Grand Prix....pq
        """
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        token = f"_{race_id}_"
        candidates = [
            f
            for f in os.listdir(directory)
            if f.startswith(prefix) and token in f and f.endswith(".pq")
        ]

        if not candidates:
            raise FileNotFoundError(
                f"No parquet file found in {directory} for race_id={race_id} "
                f"and prefix={prefix!r}"
            )
        if len(candidates) > 1:
            raise RuntimeError(
                f"Multiple parquet files found in {directory} for "
                f"race_id={race_id}: {candidates}"
            )

        return directory / candidates[0]