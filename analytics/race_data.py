"""High-level race data container built on analytics utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import re

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

    base_dir: Path
    race_options: pd.DataFrame
    races: pd.DataFrame
    results_df: Optional[pd.DataFrame]
    section_results_df: Optional[pd.DataFrame]
    timing_df: Optional[pd.DataFrame]
    pit_stops: Optional[pd.DataFrame]
    flags: Optional[pd.DataFrame]

    def __init__(
        self,
        base_dir: Optional[str | Path] = None,
        
    ) -> None:
        
        # determine project root directory
        if base_dir is None:
            # analytics/ -> project root
            base_dir = Path(__file__).resolve().parents[1]
        else:
            base_dir = Path(base_dir).resolve()

        self.base_dir = base_dir
        
        res_dfs = self._get_race_table_from_dir(
            directory=self.base_dir / "cleandata" / "results",
            prefix="results_"
        )
        sr_dfs = self._get_race_table_from_dir(
            directory=self.base_dir / "cleandata" / "section results",
            prefix="sectionresults_"            
        )
        
        self.race_options = res_dfs.merge(sr_dfs,
                                on=['Date','RaceID','Name'],
                                how='outer'
                           )
        self.races = pd.DataFrame({},
                                  columns = self.race_options.columns
                                  )
        
    def add_races_by_id(self,race_ids,section_results=True):

        # determine list of ids to use base on input
        if type(race_ids) == list:
            race_id_lst = list(map(str,race_ids))
        else:
            race_id_lst = [str(race_ids)]
            
        new_races = self.race_options.copy()
        new_races = new_races.loc[new_races.RaceID.isin(race_id_lst) &
                                  ~new_races.RaceID.isin(self.races.RaceID)
                                 ]

        self.races = pd.concat([self.races,new_races],ignore_index=True)

        if section_results:
            new_sr_dfs = []
            for f in new_races.sectionresults_File.dropna():
                df = pd.read_parquet(self.base_dir / "cleandata" / "section results" / f)
                new_sr_dfs.append(df)

            if not hasattr(self, 'section_results_df'):
                self.section_results_df = pd.concat(new_sr_dfs,ignore_index=True)
            else:
                self.section_results_df = pd.concat([self.section_results_df]+new_sr_dfs,
                                                    ignore_index=True)

        new_res_dfs = []
        for f in new_races.results_File.dropna():
            df = pd.read_parquet(self.base_dir / "cleandata" / "results" / f)
            new_res_dfs.append(df)

        if not hasattr(self, 'results_df'):
            self.results_df = pd.concat(new_res_dfs,ignore_index=True)
        else:
            self.results_df = pd.concat([self.results_df]+new_res_dfs,
                                        ignore_index=True) 

    def add_races_by_date(self,start_date,end_date,section_results=True):

        startdt = pd.to_datetime(start_date)
        enddt = pd.to_datetime(end_date)
            
        new_races = self.race_options.copy()
        new_races['RaceDate'] = pd.to_datetime(new_races.Date)
        new_races = new_races.loc[(new_races.RaceDate >= startdt) &
                                  (new_races.RaceDate <= enddt) & 
                                  ~new_races.RaceID.isin(self.races.RaceID)
                                 ]
        
        new_races.drop(columns=['RaceDate'],inplace=True)
        self.races = pd.concat([self.races,new_races],ignore_index=True)

        if section_results:
            new_sr_dfs = []
            for i in new_races.loc[~new_races.sectionresults_File.isna()].index:
                f = new_races.loc[i,'sectionresults_File']
                df = pd.read_parquet(self.base_dir / "cleandata" / "section results" / f)
                df['RaceID'] = new_races.loc[i,'RaceID']
                new_sr_dfs.append(df)

            if not hasattr(self, 'section_results_df'):
                self.section_results_df = pd.concat(new_sr_dfs,ignore_index=True)
            else:
                self.section_results_df = pd.concat([self.section_results_df]+new_sr_dfs,
                                                    ignore_index=True)

        new_res_dfs = []
        for i in new_races.loc[~new_races.results_File.isna()].index:
            f = new_races.loc[i,'results_File']            
            df = pd.read_parquet(self.base_dir / "cleandata" / "results" / f)
            df['RaceID'] = new_races.loc[i,'RaceID']
            new_res_dfs.append(df)

        if not hasattr(self, 'results_df'):
            self.results_df = pd.concat(new_res_dfs,ignore_index=True)
        else:
            self.results_df = pd.concat([self.results_df]+new_res_dfs,
                                        ignore_index=True)     


    def _parse_elapsed_time(self, value):
        parts = str(value).split(":")
        hours, minutes, seconds = parts

        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)

    def _build_lap_timing_df(self):
        section = self.section_results_df.copy()
        results = self.results_df.copy()

        section["Car"] = section["Car"].astype(str)
        results["Car"] = results["Car"].astype(str)
        results["Laps"] = results["Lap"].astype(int)

        merged = section.merge(results[["Car", "Laps", "RaceID"]], on=["Car","RaceID"], how="inner")
        merged = merged.loc[merged["Lap"] <= merged["Laps"]].copy()

        lap_sections = merged.loc[
            merged["Section"] == "Lap",
            ["RaceID", "Car", "Driver", "Lap", "Flag", "Time"],
        ]

        lap_sections = lap_sections.rename(columns={"Time": "LapTime"})

        totals = (
            lap_sections
            .groupby(["Car", "Driver", "RaceID"], as_index=False)
            ["LapTime"].agg(sum="sum", count="count")
        )

        results["ElapsedSeconds"] = results["Elapsed Time"].apply(
            self._parse_elapsed_time
        )

        leader_gaps = (
            results[["RaceID", 'Car','ElapsedSeconds','Driver']]
            .merge(totals[['Car','sum', "RaceID"]], on=["Car","RaceID"], how="inner")
        )

        leader_gaps["Gap"] = leader_gaps["ElapsedSeconds"] - leader_gaps["sum"]

        lap_zero = leader_gaps[["RaceID", "Car", "Driver", "Gap"]].rename(
            columns={"Gap": "LapTime"}
        )
        lap_zero["Lap"] = 0
        lap_zero["Flag"] = "Green"

        times = (
            pd.concat([lap_zero, lap_sections], ignore_index=True, sort=False)
            .sort_values(["Lap", "Car"])
            .reset_index(drop=True)
        )

        times["RaceTime"] = times.groupby(["Car","RaceID"])["LapTime"].cumsum()
        times = times.sort_values(["RaceID","RaceTime"]).reset_index(drop=True)
        times["Gap"] = times.groupby("RaceID")["RaceTime"].diff()

        times.rename(columns={"Lap": "LapCompleted"}, inplace=True)
        times["LapStarted"] = times["LapCompleted"] + 1

        ordered_columns = [
            "RaceID",
            "Car",
            "Driver",
            "LapStarted",
            "LapCompleted",
            "LapTime",
            "RaceTime",
            "Gap",
            "Flag",
        ]

        self.timing_df = times[ordered_columns]

    def _build_pit_stops(self) -> pd.DataFrame:
        section = self.section.copy()
        times = self.timing.copy()

        section["Car"] = section["Car"].astype(str)
        times["Car"] = times["Car"].astype(str)

        inlaps = (
            section.loc[
                section["Section"] == "SF to PI",
                ["RaceID", "Car", "Lap"],
            ]
            .rename(columns={"Lap": "LapStarted"})
            .copy()
        )
        inlaps["InLap"] = 1

        outlaps = (
            section.loc[
                (section["Lap"] > 1) & (section["Section"] == "PO to SF"),
                ["RaceID", "Car", "Lap"],
            ]
            .rename(columns={"Lap": "LapStarted"})
            .copy()
        )
        outlaps["OutLap"] = 1

        ps = (
            times[['RaceID','Car','LapStarted']]
            .merge(inlaps, on=["RaceID", "Car", "LapStarted"], how="left")
            .merge(outlaps, on=["RaceID", "Car", "LapStarted"], how="left")
        )

        ps["LastPitLap"] = ps.loc[ps["OutLap"] == 1, "LapStarted"]
        ps.loc[ps["LapStarted"] == 1, "LastPitLap"] = 1
        ps["LastPitLap"] = ps.groupby(["RaceID", "Car"])["LastPitLap"].ffill()

        ps["LapsSincePit"] = (ps["LapStarted"] - ps["LastPitLap"]).astype(int)

        ps["InLap"] = ps["InLap"].fillna(0).astype(int)
        ps["OutLap"] = ps["OutLap"].fillna(0).astype(int)

        return ps

    def _build_flags_data(self) -> pd.DataFrame:
        df = self.section.copy()
        df["Car"] = df["Car"].astype(str)

        flags = (
            df
            .groupby(["RaceID", "Car", "Lap"])["Flag"]
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
    
    @staticmethod
    def _find_race_ids_from_year(directory: Path, prefix: str, year: str) -> list:
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")

        token = f"{year}-"
        return [
            re.findall(r'.+_([0-9]{4})_.+',f)[0]
            for f in os.listdir(directory)
            if f.startswith(prefix) and token in f and f.endswith(".pq")
        ]
    
    @staticmethod
    def _get_race_table_from_dir(directory: Path, prefix: str) -> pd.DataFrame:
        re_str = prefix+r'(\d{4}-\d{2}-\d{2})_(\d{4})_(.+)\.pq'
        
        candidates = [
            (re.findall(re_str,f)[0],f)
            for f in os.listdir(directory)
            if f.startswith(prefix) and f.endswith(".pq")
        ]
        
        return pd.DataFrame([
            {'Date':m[0], 'RaceID':m[1], 'Name': m[2], f'{prefix}File':f}
            for m,f in candidates
            if len(m) == 3
        ])
        
        
        
        