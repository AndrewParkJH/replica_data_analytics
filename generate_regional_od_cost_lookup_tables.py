import argparse
import pandas as pd
from metadata.uam_schema import UAMSchema
from pathlib import Path

SCHEMA = UAMSchema()


def save_trip_time_lookup_table(lookup_table, output_folder, file_name):
    output_folder = Path(output_folder)
    for hour in lookup_table.keys():
        od_matrix = lookup_table[hour]
        filename = output_folder / f"{file_name}_{hour:02d}.csv"
        od_matrix.to_csv(filename, index=True)
        print(f"Saved: {filename}")
    print('\t all files saved')


def create_hourly_od_lookup_tables(
    input_csv_path: str,
    output_dir: str,
    time_col: str = SCHEMA.START_TIME_O,
    origin_col: str = SCHEMA.ORIGIN_TRACT_O,
    dest_col: str = SCHEMA.DESTINATION_TRACT_O,
    duration_col: str = SCHEMA.DURATION_MIN_O,
):
    """
    Create 24 OD lookup tables (one per hour of day) with average travel time.

    Each CSV will be an origin x destination matrix where each cell is the
    average trip duration (in minutes) for trips starting in that hour of day.
    Missing OD entries are left as NaN (N/A).
    """
    lookup_table = dict()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_csv_path, low_memory=False)

    df[time_col] = pd.to_datetime(df[time_col], format="%H:%M:%S", errors="coerce")
    df["hour_of_day"] = df[time_col].dt.hour

    df = df[df['primary_mode'].isin(['private_auto', 'auto_passenger', 'on_demand_auto'])]

    all_origins = sorted(df[origin_col].dropna().unique())
    all_dests = sorted(df[dest_col].dropna().unique())

    full_od_index = pd.MultiIndex.from_product(
        [all_origins, all_dests],
        names=[origin_col, dest_col],
    )

    grouped = (
        df.groupby([origin_col, dest_col, "hour_of_day"], dropna=True)[duration_col]
          .mean()
          .reset_index()
    )

    for hour in range(24):
        hour_df = grouped[grouped["hour_of_day"] == hour]

        hour_od = (
            hour_df.set_index([origin_col, dest_col])[duration_col]
                   .reindex(full_od_index)
        )

        od_matrix = hour_od.unstack(level=1)
        od_matrix = od_matrix.sort_index(axis=0).sort_index(axis=1)

        lookup_table[hour] = od_matrix

    return lookup_table


def correct_hourly_od_matrices(lookup_table):
    """
    Load hourly OD matrices, fill missing entries over time, and save corrected versions.

    - Builds a dict: {hour: DataFrame} where `hour` is 0–23.
    - For each OD cell (i, j), if the value at some hour is NaN:
        * Look backward in time for the nearest non-NaN value.
        * Look forward in time for the nearest non-NaN value.
        * If both exist: fill with their average.
        * If only one exists (first/last hours etc.): fill with that value.
        * If none exist across all hours: leave as NaN.
    """
    hours_list = list(range(24))

    sample_df = lookup_table[hours_list[0]]
    rows = sample_df.index
    cols = sample_df.columns

    corrected_mats = {h: lookup_table[h].copy() for h in hours_list}

    for r in rows:
        for c in cols:
            vals = pd.Series(
                [lookup_table[h].at[r, c] for h in hours_list],
                index=hours_list,
                dtype="float64",
            )

            if vals.isna().all():
                continue

            for pos, h in enumerate(hours_list):
                if pd.isna(vals.iloc[pos]):
                    prev_val = None
                    next_val = None

                    for p in range(pos - 1, -1, -1):
                        v = vals.iloc[p]
                        if pd.notna(v):
                            prev_val = v
                            break

                    for p in range(pos + 1, len(hours_list)):
                        v = vals.iloc[p]
                        if pd.notna(v):
                            next_val = v
                            break

                    if (prev_val is not None) and (next_val is not None):
                        fill_val = 0.5 * (prev_val + next_val)
                    elif prev_val is not None:
                        fill_val = prev_val
                    elif next_val is not None:
                        fill_val = next_val
                    else:
                        continue

                    vals.iloc[pos] = fill_val

            for pos, h in enumerate(hours_list):
                corrected_mats[h].at[r, c] = vals.iloc[pos]

    return corrected_mats


def create_average_distance_od_matrix(
    input_csv_path: str,
    output_dir: str,
    output_file_name: str,
    origin_col: str = SCHEMA.ORIGIN_TRACT_O,
    dest_col: str = SCHEMA.DESTINATION_TRACT_O,
    distance_col: str = SCHEMA.DISTANCE_MI_O,
):
    """
    Create a single OD lookup table (one matrix) with average travel distance.

    The output CSV will be an origin x destination matrix where each cell is the
    average trip distance (in miles) across all trips between that OD pair.
    Missing OD entries are left as NaN (N/A).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_csv_path, low_memory=False)

    df = df[df["primary_mode"].isin(["private_auto", "auto_passenger", "on_demand_auto"])]

    all_origins = sorted(df[origin_col].dropna().unique())
    all_dests = sorted(df[dest_col].dropna().unique())

    full_od_index = pd.MultiIndex.from_product(
        [all_origins, all_dests],
        names=[origin_col, dest_col],
    )

    grouped = (
        df.groupby([origin_col, dest_col], dropna=True)[distance_col]
          .mean()
          .reset_index()
    )

    od_series = (
        grouped.set_index([origin_col, dest_col])[distance_col]
               .reindex(full_od_index)
    )

    od_matrix = od_series.unstack(level=1)
    od_matrix = od_matrix.sort_index(axis=0).sort_index(axis=1)

    global_mean = od_matrix.stack().mean()
    od_matrix = od_matrix.fillna(global_mean)

    out_path = output_dir / f"{output_file_name}.csv"
    od_matrix.to_csv(out_path, index=True)
    print(f"Saved average distance OD matrix to: {out_path}")

    return od_matrix


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate OD cost lookup tables from raw Replica data")
    parser.add_argument("--state", required=True, help="State name (e.g. Illinois)")
    parser.add_argument("--city", required=True, help="City name (e.g. Chicago)")
    parser.add_argument("--raw_data_dir", default=None, help="Root raw data dir (default: <cwd>/data/replica_data)")
    parser.add_argument("--processed_data_dir", default=None, help="Root processed data dir (default: <cwd>/data/processed_data)")
    args = parser.parse_args()

    cwd = Path.cwd()
    raw_data_dir = Path(args.raw_data_dir) if args.raw_data_dir else cwd / "data" / "replica_data"
    processed_data_dir = Path(args.processed_data_dir) if args.processed_data_dir else cwd / "data" / "processed_data"

    input_csv = raw_data_dir / args.state / f"{args.city}_{args.city}_Thur.csv"
    tt_output = processed_data_dir / "od_cost_matrix" / args.state / f"{args.city}_hourly_od_lookup_tables"
    dist_output = processed_data_dir / "od_cost_matrix" / args.state / "od_distance_matrix"

    lookup = create_hourly_od_lookup_tables(input_csv_path=str(input_csv), output_dir=str(tt_output))
    lookup = correct_hourly_od_matrices(lookup)
    save_trip_time_lookup_table(lookup, output_folder=str(tt_output), file_name=f"{args.city}_TT_Matrix")
    create_average_distance_od_matrix(
        input_csv_path=str(input_csv),
        output_dir=str(dist_output),
        output_file_name=f"{args.city}_DIST_Matrix",
    )
