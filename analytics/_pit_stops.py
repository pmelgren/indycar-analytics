"""Pit stop analytics utilities."""

import pandas as pd


def build_pit_stops(section_df: pd.DataFrame, lap_timing_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-lap pit stop metadata.

    Parameters
    ----------
    section_df : pandas.DataFrame
        Section-level timing data. Must include columns:
        ``Car``, ``Lap``, and ``Section``. This should be the same
        structure used as input to ``build_lap_timing``.

    lap_timing_df : pandas.DataFrame
        Output of ``build_lap_timing``. Must include columns:
        ``Car`` and ``LapStarted`` (at minimum), plus any other
        timing metadata that should be carried through.

    Returns
    -------
    pandas.DataFrame
        A dataframe based on ``lap_timing_df`` with the following
        additional columns:

        - ``InLap``: 1 if the lap begins with a pit entry (``SF to PI``), else 0.
        - ``OutLap``: 1 if the lap begins with a pit exit (``PO to SF``), else 0.
        - ``LastPitLap``: the most recent lap on which the car exited the pits.
        - ``LapsSincePit``: integer laps completed since ``LastPitLap``.
    """

    # Ensure we do not mutate caller dataframes
    section = section_df.copy()

    # Normalise car identifiers to strings for consistent joins
    section["Car"] = section["Car"].astype(str)
    times["Car"] = times["Car"].astype(str)

    # Identify pit-in and pit-out laps from section data
    inlaps = (
        section.loc[section["Section"] == "SF to PI", ["Car", "Lap"]]
        .rename(columns={"Lap": "LapStarted"})
        .copy()
    )
    inlaps["InLap"] = 1

    outlaps = (
        section.loc[
            (section["Lap"] > 1) & (section["Section"] == "PO to SF"), ["Car", "Lap"]
        ]
        .rename(columns={"Lap": "LapStarted"})
        .copy()
    )
    outlaps["OutLap"] = 1

    # Attach pit-in / pit-out flags to lap timing
    ps = (
        times
        .merge(inlaps, on=["Car", "LapStarted"], how="left")
        .merge(outlaps, on=["Car", "LapStarted"], how="left")
    )

    # Define last pit lap and laps since pit, following notebook logic
    ps["LastPitLap"] = ps.loc[ps["OutLap"] == 1, "LapStarted"]
    ps.loc[ps["LapStarted"] == 1, "LastPitLap"] = 1
    ps["LastPitLap"] = ps.groupby("Car")["LastPitLap"].ffill()

    # Integer laps since the last pit stop
    ps["LapsSincePit"] = (ps["LapStarted"] - ps["LastPitLap"]).astype(int)

    # Fill missing flags with 0/NaN as appropriate
    ps["InLap"] = ps["InLap"].fillna(0).astype(int)
    ps["OutLap"] = ps["OutLap"].fillna(0).astype(int)

    return ps
