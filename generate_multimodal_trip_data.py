import argparse
import pandas as pd
from pathlib import Path
import json
from metadata.uam_schema import UAMSchema

SCHEMA = UAMSchema()


# ========= Helpers =========

def load_vertiport_specification(veriport_spec_file):
    with open(veriport_spec_file, 'r') as f:
        vertiport_spec = json.load(f)

    trip_assumptions = vertiport_spec['assumptions']
    vertiports = vertiport_spec["vertiports"]

    return vertiports, trip_assumptions


def load_trip_data(state, o_city, d_city, raw_data_dir):
    trips_csv = f"{o_city}_{d_city}_Thur.csv"

    df = pd.read_csv(Path(raw_data_dir) / state / trips_csv, low_memory=False)
    df.rename(columns=SCHEMA.mapping, inplace=True)

    df = df[df[SCHEMA.MODE].isin(["private_auto", "auto_passenger", "on_demand_auto"])].copy()

    df[SCHEMA.START_TIME] = pd.to_datetime(df[SCHEMA.START_TIME], format="%H:%M:%S", errors="coerce")
    df[SCHEMA.END_TIME] = pd.to_datetime(df[SCHEMA.END_TIME], format="%H:%M:%S", errors="coerce")

    df[SCHEMA.ORIGIN_TRACT] = df[SCHEMA.ORIGIN_TRACT].astype(str)
    df[SCHEMA.DESTINATION_TRACT] = df[SCHEMA.DESTINATION_TRACT].astype(str)

    return df


def load_cost_matrix(state, o_city, d_city, processed_data_dir):
    """
    Load hourly OD matrices into a dict {hour: DataFrame}.
    Assumes filenames like: <od_prefix>_<HH>.csv
    """
    od_cost_dir = Path(processed_data_dir) / "od_cost_matrix" / state

    od_folder = {
        "FM": od_cost_dir / f"{o_city}_hourly_od_lookup_tables",
        "LM": od_cost_dir / f"{d_city}_hourly_od_lookup_tables",
    }

    od_prefix = {
        "FM": f"{o_city}_TT_Matrix",
        "LM": f"{d_city}_TT_Matrix",
    }

    od_dist_files = {
        "FM": od_cost_dir / "od_distance_matrix" / f"{o_city}_DIST_Matrix.csv",
        "LM": od_cost_dir / "od_distance_matrix" / f"{d_city}_DIST_Matrix.csv",
    }

    fm_dist_mat = pd.read_csv(od_dist_files["FM"], index_col=0)
    lm_dist_mat = pd.read_csv(od_dist_files["LM"], index_col=0)

    fm_dist_mat.index = fm_dist_mat.index.astype(str)
    fm_dist_mat.columns = fm_dist_mat.columns.astype(str)

    lm_dist_mat.index = lm_dist_mat.index.astype(str)
    lm_dist_mat.columns = lm_dist_mat.columns.astype(str)

    distance_cost_matrix = {
        "FM": fm_dist_mat,
        "LM": lm_dist_mat,
    }

    time_cost_matrix = {}

    for key in ['FM', 'LM']:
        matrices = {}
        for h in range(24):
            path = od_folder[key] / f"{od_prefix[key]}_{h:02d}.csv"
            df = pd.read_csv(path, index_col=0)
            df.index = df.index.astype(str)
            df.columns = df.columns.astype(str)
            matrices[h] = df
        time_cost_matrix[key] = matrices

    return distance_cost_matrix, time_cost_matrix


def compute_taxi_fare(duration, mile):
    fare = 1 + duration * 0.3 + mile * 1.3
    return fare


def compute_ram_trip_statistics(
    veriport_spec_file: str,
    o_city: str,
    d_city: str,
    state: str,
    raw_data_dir: str,
    processed_data_dir: str,
    output_dir: str,
):
    segment_options = ["option1_ram", "option2_car"]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f"{o_city}_{d_city}_RAM_trip_stats.csv"

    vertiports, trip_assumptions = load_vertiport_specification(veriport_spec_file)

    if trip_assumptions['assignment_strategy']['type'] != "nearest":
        raise Exception("current set up only allows \"nearest\" assignment strategy!")

    trip_segments_choice1 = trip_assumptions["multimodal_segments"][segment_options[0]]
    trip_segments_choice2 = trip_assumptions["multimodal_segments"][segment_options[1]]

    FM_OVTT = trip_segments_choice1[0]['ovtt_min']
    MM_OVTT = trip_segments_choice1[1]['ovtt_min']
    LM_OVTT = trip_segments_choice1[2]['ovtt_min']
    MIDDLE_MILE_FLIGHT_MIN = trip_segments_choice1[1]['ivtt_min']
    RAM_fare = 100
    DRIVING_OVTT_CONST = 3.0
    cost_driving_per_mile = 0.6

    fixed_driving_cost = 0
    if d_city == "Chicago" or "Houston" or "Orlando":
        fixed_driving_cost = 20

    df = load_trip_data(state, o_city, d_city, raw_data_dir)
    distance_cost_matrix, time_cost_matrix = load_cost_matrix(state, o_city, d_city, processed_data_dir)

    df["FM_duration_min"] = pd.NA
    df["FM_fare_USD"] = pd.NA
    df["depart_hour"] = df[SCHEMA.START_TIME].dt.hour

    for h in range(24):
        mask_first = df["depart_hour"] == h
        time_cost_matrix_h = time_cost_matrix["FM"][h]

        if mask_first.any():
            origin_tracts = df.loc[mask_first, SCHEMA.ORIGIN_TRACT]

            candidate_tracts = [
                vp['census_tract_id'] for vp in vertiports
                if vp['city'] == o_city and vp['census_tract_id'] != '-1'
            ]

            tract_to_vp_id = {
                vp['census_tract_id']: vp['vertiport_id']
                for vp in vertiports
                if vp['census_tract_id'] != '-1'
            }

            if not candidate_tracts:
                raise Exception("not vertiport available for some trip takers")

            routing_options = time_cost_matrix_h.loc[origin_tracts, candidate_tracts]
            routing_options.index = origin_tracts.index

            origin_vertiport_trct = routing_options.idxmin(axis=1)

            df.loc[mask_first, 'origin_vertiport_id'] = origin_vertiport_trct.map(tract_to_vp_id)
            df.loc[mask_first, 'origin_vertiport_tract'] = origin_vertiport_trct

            duration_vals = routing_options.min(axis=1)
            dist_vals = pd.Series(
                [distance_cost_matrix["FM"].at[o, d] for o, d in zip(origin_tracts, origin_vertiport_trct)],
                index=origin_tracts.index
            )

            df.loc[mask_first, "FM_duration_min"] = duration_vals.values
            df.loc[mask_first, "FM_fare_USD"] = compute_taxi_fare(duration=duration_vals.values, mile=dist_vals.values)

    df["arrival_time_at_dest_vertiport"] = (df[SCHEMA.START_TIME]
                                            + pd.to_timedelta(df["FM_duration_min"], unit="m")
                                            + pd.to_timedelta(FM_OVTT, unit="m")
                                            + pd.to_timedelta(MIDDLE_MILE_FLIGHT_MIN, unit="m"))

    df["last_hour"] = df["arrival_time_at_dest_vertiport"].dt.hour

    df["LM_duration_min"] = pd.NA
    df["dest_vertiport_id"] = pd.NA
    df["dest_vertiport_tract"] = pd.NA

    candidate_tracts_d = [
        vp['census_tract_id'] for vp in vertiports
        if vp['city'] == d_city and vp['census_tract_id'] != '-1'
    ]

    if not candidate_tracts_d:
        raise Exception(f"No vertiports available in destination city: {d_city}")

    tract_to_vp_id_d = {
        vp['census_tract_id']: vp['vertiport_id']
        for vp in vertiports
        if vp['city'] == d_city and vp['census_tract_id'] != '-1'
    }

    for h in range(24):
        time_cost_matrix_h = time_cost_matrix["LM"][h]
        mask_last = df["last_hour"] == h
        if mask_last.any():
            dest_tracts = df.loc[mask_last, SCHEMA.DESTINATION_TRACT]

            routing_options = time_cost_matrix_h.loc[candidate_tracts_d, dest_tracts]
            routing_options.columns = dest_tracts.index

            chosen_dest_vps_trct = routing_options.idxmin(axis=0)

            df.loc[mask_last, "dest_vertiport_tract"] = chosen_dest_vps_trct.values
            df.loc[mask_last, "dest_vertiport_id"] = chosen_dest_vps_trct.map(tract_to_vp_id_d).values

            duration_vals = routing_options.min(axis=0)

            dist_vals = pd.Series(
                [distance_cost_matrix["LM"].at[v, d] for v, d in zip(chosen_dest_vps_trct, dest_tracts)],
                index=dest_tracts.index
            )

            df.loc[mask_last, "LM_duration_min"] = duration_vals.values
            df.loc[mask_last, "LM_fare_USD"] = compute_taxi_fare(
                duration=duration_vals.values,
                mile=dist_vals.values
            )

    df["MM_duration_min"] = MIDDLE_MILE_FLIGHT_MIN

    df["RAM_IVTT_min"] = (
        df["FM_duration_min"].astype("float64")
        + df["MM_duration_min"].astype("float64")
        + df["LM_duration_min"].astype("float64")
    )

    df["RAM_OVTT_min"] = FM_OVTT + LM_OVTT

    df["RAM_Fare_USD"] = (df["FM_fare_USD"].astype("float64") +
                          RAM_fare +
                          df["LM_fare_USD"].astype("float64"))

    df["Driving_IVTT_min"] = pd.to_numeric(df[SCHEMA.DURATION_MIN], errors="coerce")
    df["Driving_OVTT_min"] = DRIVING_OVTT_CONST

    df["Driving_Fare_USD"] = (
        pd.to_numeric(df[SCHEMA.DISTANCE_MI], errors="coerce") * cost_driving_per_mile + fixed_driving_cost
    )

    income_col = SCHEMA.USER_INCOME

    if income_col in df.columns:
        income = pd.to_numeric(df[income_col], errors="coerce")
        df["Income_low"] = (income < 45000).astype(int)
        df["Income_mid"] = ((income >= 45000) & (income <= 152000)).astype(int)
        df["Income_high"] = (income > 152000).astype(int)
        missing_mask = income.isna()
        df.loc[missing_mask, ["Income_low", "Income_mid", "Income_high"]] = [0, 1, 0]
    else:
        df["Income_mid"] = 1

    df.to_csv(output_csv, index=False)
    print(f"Saved RAM trip stats to: {output_csv}")

    grouped = df.groupby(['origin_vertiport_id', 'dest_vertiport_id'])
    for (o_vp, d_vp), group_df in grouped:
        partition_path = output_dir / f"{o_vp}_{d_vp}_trips.csv"
        group_df.to_csv(partition_path, index=False)
        print(f"Saved {len(group_df)} trips to: {partition_path}")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate multimodal RAM trip statistics")
    parser.add_argument("--state", required=True, help="State name (e.g. Illinois)")
    parser.add_argument("--o_city", required=True, help="Origin city (e.g. Chicago)")
    parser.add_argument("--d_city", required=True, help="Destination city (e.g. UIUC)")
    parser.add_argument("--vertiport_config", required=True, help="Path to vertiport JSON config")
    parser.add_argument("--raw_data_dir", default=None, help="Root raw data dir (default: <cwd>/data/replica_data)")
    parser.add_argument("--processed_data_dir", default=None, help="Root processed data dir (default: <cwd>/data/processed_data)")
    parser.add_argument("--output_dir", default=None, help="Output dir (default: <cwd>/data/processed_data/demand)")
    args = parser.parse_args()

    cwd = Path.cwd()
    raw_data_dir = Path(args.raw_data_dir) if args.raw_data_dir else cwd / "data" / "replica_data"
    processed_data_dir = Path(args.processed_data_dir) if args.processed_data_dir else cwd / "data" / "processed_data"
    output_dir = Path(args.output_dir) if args.output_dir else processed_data_dir / "demand"

    compute_ram_trip_statistics(
        veriport_spec_file=args.vertiport_config,
        o_city=args.o_city,
        d_city=args.d_city,
        state=args.state,
        raw_data_dir=str(raw_data_dir),
        processed_data_dir=str(processed_data_dir),
        output_dir=str(output_dir),
    )
