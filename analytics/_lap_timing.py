"""Lap timing analytics utilities."""

import pandas as pd


def _parse_elapsed_time(value):
    """Convert HH:MM:SS.sss strings to seconds (best effort)."""

    parts = str(value).split(":")
    hours, minutes, seconds = parts

    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)

def build_lap_timing(section_df, results_df):

    section = section_df.copy()
    results = results_df.copy()

    section["Car"] = section["Car"].astype(str)
    results["Car"] = results["Car"].astype(str)

    merged = section.merge(results[["Car", "Laps"]], on="Car", how="inner")
    merged = merged.loc[merged["Lap"] <= merged["Laps"]].copy()

    lap_sections = merged.loc[merged["Section"] == "Lap", 
                             ["Car", "Driver", "Lap", "Flag", "Time"]
                             ]

    lap_sections = lap_sections.rename(columns={"Time": "LapTime"})

    totals = (
        lap_sections
        .groupby(["Car", "Driver"], as_index=False)
        ["LapTime"]
        .agg(sum="sum", count="count")
    )

    results["ElapsedSeconds"] = results["Elapsed Time"].apply(_parse_elapsed_time)

    leader_gaps = results.merge(totals, on="Car", how="inner")

    leader_gaps["Gap"] = leader_gaps["ElapsedSeconds"] - leader_gaps["sum"]

    lap_zero = leader_gaps[["Car", "Driver", "Gap"]].rename(columns={"Gap": "LapTime"})
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
