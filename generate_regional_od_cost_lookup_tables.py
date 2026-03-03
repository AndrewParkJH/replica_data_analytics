import pandas as pd
from metadata.uam_schema import UAMSchema
import json
from pathlib import Path

# Column mapping
SCHEMA = UAMSchema()

# this file creates a lookup table for the speed

INPUT_CSV = "Chicago_Chicago_Thur.csv"

TIME_COL = "trip_start_time"
ORIGIN_COL = "origin_trct_2020"
DEST_COL = "destination_trct_2020"

DURATION_COL = "trip_duration_minutes"
DISTANCE_COL = "trip_distance_miles"

TT_OUTPUT_FOLDER = "Chicago_hourly_od_lookup_tables"
DIST_OUTPUT_FOLDER = "OD_distance_matrix"
FILE_NAME = "Chicago_TT_Matrix"

def save_trip_time_lookup_table(lookup_table, output_folder=TT_OUTPUT_FOLDER):

    for hour in lookup_table.keys():
        od_matrix = lookup_table[hour]

        # Build filename with zero-padded hour
        filename = Path(output_folder) / f"{FILE_NAME}_{hour:02d}.csv"

        # Save to CSV; NaNs will appear as empty cells (can be read as N/A)
        od_matrix.to_csv(filename, index=True)

        print(f"Saved: {filename}")

    print('\t all files saved')


def create_hourly_od_lookup_tables(
    input_csv_path: str = INPUT_CSV,
    output_dir: str = TT_OUTPUT_FOLDER,
    time_col: str = TIME_COL,
    origin_col: str = ORIGIN_COL,
    dest_col: str = DEST_COL,
    duration_col: str = DURATION_COL,
):
    """
    Create 24 OD lookup tables (one per hour of day) with average travel time.

    Each CSV will be an origin x destination matrix where each cell is the
    average trip duration (in minutes) for trips starting in that hour of day.
    Missing OD entries are left as NaN (N/A).
    """

    lookup_table = dict()

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    # df = pd.read_csv(input_csv_path)
    df = pd.read_csv(input_csv_path, low_memory=False)

    # Parse trip_start_time to datetime and extract hour-of-day
    df[time_col] = pd.to_datetime(df[time_col], format="%H:%M:%S", errors="coerce")
    df["hour_of_day"] = df[time_col].dt.hour

    # Filter data so only driving remains
    df = df[df['primary_mode'].isin(['private_auto', 'auto_passenger', 'on_demand_auto'])]

    # Identify all unique origins and destinations for full OD grid
    all_origins = sorted(df[origin_col].dropna().unique())
    all_dests = sorted(df[dest_col].dropna().unique())

    # Precompute the full OD MultiIndex for reindexing
    full_od_index = pd.MultiIndex.from_product(
        [all_origins, all_dests],
        names=[origin_col, dest_col],
    )

    # Group by origin, destination, and hour, compute mean duration
    grouped = (
        df.groupby([origin_col, dest_col, "hour_of_day"], dropna=True)[duration_col]
          .mean()
          .reset_index()
    )

    # Loop over each hour 0–23 and create a CSV
    for hour in range(24):
        # Filter for this hour
        hour_df = grouped[grouped["hour_of_day"] == hour]

        # Set OD as index and reindex to full OD grid to insert NaNs for missing combos
        hour_od = (
            hour_df.set_index([origin_col, dest_col])[duration_col]
                   .reindex(full_od_index)
        )

        # Convert to OD matrix (origins as rows, destinations as columns)
        od_matrix = hour_od.unstack(level=1)

        # Optional: sort rows/cols (already sorted by construction, but just in case)
        od_matrix = od_matrix.sort_index(axis=0).sort_index(axis=1)

        # # Build filename with zero-padded hour
        # filename = output_dir / f"{FILE_NAME}_{hour:02d}.csv"
        #
        # # Save to CSV; NaNs will appear as empty cells (can be read as N/A)
        # od_matrix.to_csv(filename, index=True)
        #
        # print(f"Saved: {filename}")

        lookup_table[hour] = od_matrix

    return lookup_table

def correct_hourly_od_matrices(
        lookup_table
):
    """
    Load hourly OD matrices, fill missing entries over time, and save corrected versions.

    - Builds a dict: {hour: DataFrame} where `hour` is 0–23.
    - For each OD cell (i, j), if the value at some hour is NaN:
        * Look backward in time for the nearest non-NaN value.
        * Look forward in time for the nearest non-NaN value.
        * If both exist: fill with their average.
        * If only one exists (first/last hours etc.): fill with that value.
        * If none exist across all hours: leave as NaN.
    - Saves corrected matrices as CSVs with filenames:
        `<input_dir>/<corrected_prefix>_<file_name_prefix>_<hour:02d>.csv`
    """
    corrected_prefix = "corrected"

    hours_list = list(range(24))

    # Assume all matrices share the same index/columns
    sample_df = lookup_table[hours_list[0]]
    rows = sample_df.index
    cols = sample_df.columns

    # Deep copies for corrected matrices
    corrected_mats = {h: lookup_table[h].copy() for h in hours_list}

    # 2. For each OD cell, interpolate missing values over time
    for r in rows:
        for c in cols:
            # Time series of this OD pair across hours
            vals = pd.Series(
                [lookup_table[h].at[r, c] for h in hours_list],
                index=hours_list,
                dtype="float64",
            )

            # If the entire series is NaN, nothing to do
            if vals.isna().all():
                continue

            # Fill missing hours using nearest prev/next values
            for pos, h in enumerate(hours_list):
                if pd.isna(vals.iloc[pos]):
                    prev_val = None
                    next_val = None

                    # Search backward in time
                    for p in range(pos - 1, -1, -1):
                        v = vals.iloc[p]
                        if pd.notna(v):
                            prev_val = v
                            break

                    # Search forward in time
                    for p in range(pos + 1, len(hours_list)):
                        v = vals.iloc[p]
                        if pd.notna(v):
                            next_val = v
                            break

                    # Decide fill value
                    if (prev_val is not None) and (next_val is not None):
                        fill_val = 0.5 * (prev_val + next_val)
                    elif prev_val is not None:
                        fill_val = prev_val
                    elif next_val is not None:
                        fill_val = next_val
                    else:
                        # No info at any hour; leave NaN
                        continue

                    vals.iloc[pos] = fill_val

            # Write back corrected values into each hour's matrix
            for pos, h in enumerate(hours_list):
                corrected_mats[h].at[r, c] = vals.iloc[pos]

    return corrected_mats

def create_average_distance_od_matrix(
    input_csv_path: str = INPUT_CSV,
    output_dir: str = DIST_OUTPUT_FOLDER,
    origin_col: str = ORIGIN_COL,
    dest_col: str = DEST_COL,
    distance_col: str = DISTANCE_COL,
    output_file_name: str = FILE_NAME,
):
    """
    Create a single OD lookup table (one matrix) with average travel distance.

    The output CSV will be an origin x destination matrix where each cell is the
    average trip distance (in miles) across all trips between that OD pair.
    Missing OD entries are left as NaN (N/A).
    """

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    df = pd.read_csv(input_csv_path, low_memory=False)

    # Filter data so only driving remains
    df = df[df["primary_mode"].isin(["private_auto", "auto_passenger", "on_demand_auto"])]

    # Identify all unique origins and destinations for full OD grid
    all_origins = sorted(df[origin_col].dropna().unique())
    all_dests = sorted(df[dest_col].dropna().unique())

    # Precompute the full OD MultiIndex for reindexing
    full_od_index = pd.MultiIndex.from_product(
        [all_origins, all_dests],
        names=[origin_col, dest_col],
    )

    # Group by origin, destination, compute mean distance
    grouped = (
        df.groupby([origin_col, dest_col], dropna=True)[distance_col]
          .mean()
          .reset_index()
    )

    # Set OD as index and reindex to full OD grid to insert NaNs for missing combos
    od_series = (
        grouped.set_index([origin_col, dest_col])[distance_col]
               .reindex(full_od_index)
    )

    # Convert to OD matrix (origins as rows, destinations as columns)
    od_matrix = od_series.unstack(level=1)

    # Sort rows/cols just to be safe
    od_matrix = od_matrix.sort_index(axis=0).sort_index(axis=1)

    # --------- Correction step: fill missing values ---------
    global_mean = od_matrix.stack().mean()     # stack() drops NaNs
    od_matrix = od_matrix.fillna(global_mean)
    # ---------------------------------------------------------

    # Save single matrix
    out_path = Path(output_dir) / f"{output_file_name}.csv"
    od_matrix.to_csv(out_path, index=True)

    print(f"Saved average distance OD matrix to: {out_path}")

    return od_matrix


if __name__ == "__main__":

    # trip time
    lookup_table = create_hourly_od_lookup_tables()
    lookup_table = correct_hourly_od_matrices(lookup_table)
    save_trip_time_lookup_table(lookup_table)

    # trip distance
    create_average_distance_od_matrix()

    pass
