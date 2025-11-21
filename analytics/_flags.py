"""Flag analytics utilities."""

import pandas as pd


def build_flags_data(section_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-lap flag classifications from section-level data.

    Parameters
    ----------
    section_df : pandas.DataFrame
        Section-level timing data (the same structure used in the
        Build Overtake Data notebook). Must include columns:
        ``Car``, ``Lap``, and ``Flag``.

    Returns
    -------
    pandas.DataFrame
        A dataframe with one row per (Car, Lap) and columns:

        - ``Car``
        - ``Lap``
        - ``SectionFlags``: list of section-level flags for that lap.
        - ``Flag``: aggregated lap flag with the following precedence:

          * ``"Yellow Thrown"`` if both "Yellow" and "Green" occur in the lap.
          * ``"Red"`` if any "Red" occurs.
          * ``"Yellow"`` if any "Yellow" occurs.
          * ``"Green"`` otherwise.
    """

    df = section_df.copy()
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
