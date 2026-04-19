import argparse
import numpy as np
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
    links = vertiport_spec.get("links", {})

    return vertiports, trip_assumptions, links


def load_trip_data(state, o_city, d_city, raw_data_dir, day="Thu"):
    trips_csv = f"{o_city}_{d_city}_{day}.csv"

    df = pd.read_csv(Path(raw_data_dir) / state / trips_csv, low_memory=False)
    df.rename(columns=SCHEMA.mapping, inplace=True)

    df = df[df[SCHEMA.MODE].isin(["private_auto", "auto_passenger", "on_demand_auto"])].copy()

    df[SCHEMA.START_TIME] = pd.to_datetime(df[SCHEMA.START_TIME], format="%H:%M:%S", errors="coerce")
    df[SCHEMA.END_TIME] = pd.to_datetime(df[SCHEMA.END_TIME], format="%H:%M:%S", errors="coerce")

    df[SCHEMA.ORIGIN_TRACT] = df[SCHEMA.ORIGIN_TRACT].astype(str)
    df[SCHEMA.DESTINATION_TRACT] = df[SCHEMA.DESTINATION_TRACT].astype(str)

    return df


def load_cost_matrix(state, o_city, d_city, processed_data_dir, day="Thu"):
    """
    Load hourly OD matrices into a dict {hour: DataFrame}.
    Assumes filenames like: <od_prefix>_<HH>.csv
    """
    od_cost_dir = Path(processed_data_dir) / "od_cost_matrix" / state

    od_folder = {
        "FM": od_cost_dir / f"{o_city}_{day}_hourly_od_lookup_tables",
        "LM": od_cost_dir / f"{d_city}_{day}_hourly_od_lookup_tables",
    }

    od_prefix = {
        "FM": f"{o_city}_{day}_TT_Matrix",
        "LM": f"{d_city}_{day}_TT_Matrix",
    }

    od_dist_files = {
        "FM": od_cost_dir / "od_distance_matrix" / f"{o_city}_{day}_DIST_Matrix.csv",
        "LM": od_cost_dir / "od_distance_matrix" / f"{d_city}_{day}_DIST_Matrix.csv",
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
    day: str = "Thu",
):
    segment_options = ["option1_ram", "option2_car"]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f"{o_city}_{d_city}_{day}_RAM_trip_stats.csv"

    vertiports, trip_assumptions, links = load_vertiport_specification(veriport_spec_file)

    if trip_assumptions['assignment_strategy']['type'] != "nearest":
        raise Exception("current set up only allows \"nearest\" assignment strategy!")

    trip_segments_choice1 = trip_assumptions["multimodal_segments"][segment_options[0]]

    FM_OVTT = trip_segments_choice1[0]['ovtt_min']
    MM_OVTT = trip_segments_choice1[1]['ovtt_min']
    LM_OVTT = trip_segments_choice1[2]['ovtt_min']
    RAM_fare = 100
    DRIVING_OVTT_CONST = 3.0
    cost_driving_per_mile = 0.6

    # Flight matrices from config links (actual per-OD flight times)
    nodes     = links["nodes"]
    node_idx  = {name: i for i, name in enumerate(nodes)}
    ft_matrix = np.array(links["flight_time_matrix"],     dtype=float)  # minutes
    fd_matrix = np.array(links["flight_distance_matrix"], dtype=float)  # miles

    # Per-vertiport parking cost added to driving fare when a trip ends near that vertiport
    vp_parking = {vp['vertiport_id']: vp.get('fixed_driving_cost_USD', 0) for vp in vertiports}

    df = load_trip_data(state, o_city, d_city, raw_data_dir, day=day)
    distance_cost_matrix, time_cost_matrix = load_cost_matrix(state, o_city, d_city, processed_data_dir, day=day)

    # --- Filter passengers whose tracts are outside the OD matrices ---
    # The FM matrix covers o_city internal trips; the LM matrix covers d_city internal
    # trips.  Cross-region passengers whose origin/destination tracts don't appear in
    # the respective matrix cannot be assigned a vertiport and are dropped here.
    n_before = len(df)
    valid_o_tracts = set(distance_cost_matrix["FM"].index)
    valid_d_tracts = set(distance_cost_matrix["LM"].columns)   # LM: cols = destination tracts
    df = df[
        df[SCHEMA.ORIGIN_TRACT].isin(valid_o_tracts) &
        df[SCHEMA.DESTINATION_TRACT].isin(valid_d_tracts)
    ].copy()
    n_after = len(df)
    if n_before != n_after:
        print(f"[generate_trips] Dropped {n_before - n_after} of {n_before} passengers "
              f"with tracts outside the OD matrices ({n_after} remaining).")
    # -----------------------------------------------------------------------

    df["FM_duration_min"] = np.nan
    df["FM_fare_USD"]     = np.nan
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

            # Some passengers may originate from tracts outside the FM OD matrix
            # (e.g. suburbs in a different county).  Filter to rows that exist in
            # the matrix so idxmin never sees an all-NaN row; unmatched passengers
            # simply keep NaN FM columns and will be car-only downstream.
            in_matrix = origin_tracts.isin(time_cost_matrix_h.index)
            if not in_matrix.any():
                continue
            origin_tracts_valid = origin_tracts[in_matrix]

            routing_options = time_cost_matrix_h.reindex(
                index=origin_tracts_valid.values, columns=candidate_tracts
            )
            routing_options.index = origin_tracts_valid.index

            # Secondary filter: drop rows where all candidate-tract columns are NaN
            row_has_data = routing_options.notna().any(axis=1)
            routing_options = routing_options[row_has_data]
            if routing_options.empty:
                continue
            origin_tracts_valid = origin_tracts_valid[row_has_data]

            origin_vertiport_trct = routing_options.idxmin(axis=1)

            df.loc[origin_vertiport_trct.index, 'origin_vertiport_id']    = origin_vertiport_trct.map(tract_to_vp_id)
            df.loc[origin_vertiport_trct.index, 'origin_vertiport_tract'] = origin_vertiport_trct

            duration_vals = routing_options.min(axis=1)
            dm_fm = distance_cost_matrix["FM"]
            dist_vals = pd.Series(
                [dm_fm.at[o, d] if (o in dm_fm.index and d in dm_fm.columns) else np.nan
                 for o, d in zip(origin_tracts_valid, origin_vertiport_trct)],
                index=origin_vertiport_trct.index
            )

            df.loc[origin_vertiport_trct.index, "FM_duration_min"] = duration_vals.values
            df.loc[origin_vertiport_trct.index, "FM_fare_USD"] = compute_taxi_fare(
                duration=duration_vals.values, mile=dist_vals.values
            )

    # Arrival at origin vertiport: trip start + FM wait + FM ride
    df["arrival_time_at_origin_vertiport"] = (
        df[SCHEMA.START_TIME]
        + pd.to_timedelta(FM_OVTT, unit="m")
        + pd.to_timedelta(df["FM_duration_min"], unit="m")
    )
    df["origin_vertiport_arrival_hour"] = df["arrival_time_at_origin_vertiport"].dt.hour

    # ── Destination vertiport: pick the one minimising total arrival time ──────
    #
    # For each candidate destination vertiport k evaluate (all passengers at once):
    #   arrival_at_dest_vp_k  = arrival_at_origin_vp + MM_OVTT + flight_time(origin→k)
    #   lm_duration_k         = LM travel time from vertiport-k to each passenger's dest tract
    #   total_arrival_k       = arrival_at_dest_vp_k + LM_OVTT + lm_duration_k
    # Then argmin over k gives the fastest path for each passenger.

    candidate_dest_vps = [
        vp for vp in vertiports
        if vp['city'] == d_city and vp['census_tract_id'] != '-1'
    ]
    if not candidate_dest_vps:
        raise Exception(f"No vertiports available in destination city: {d_city}")

    K = len(candidate_dest_vps)
    N = len(df)
    BASE_TS = pd.Timestamp("1900-01-01")

    # Map each passenger's origin vertiport to its flight-matrix row index
    origin_vp_idx_arr = (
        df["origin_vertiport_id"].map(node_idx).fillna(-1).astype(int).values
    )
    dest_tract_arr = df[SCHEMA.DESTINATION_TRACT].astype(str).values
    dm_lm = distance_cost_matrix["LM"]

    cand_total_sec   = np.full((N, K), np.inf)   # comparable arrival time (seconds)
    cand_lm_duration = np.full((N, K), np.nan)
    cand_lm_fare     = np.full((N, K), np.nan)
    cand_flight_time = np.full((N, K), np.nan)
    cand_arr_dest_vp = []                         # one datetime Series per candidate

    for k, dest_vp in enumerate(candidate_dest_vps):
        dest_tract_k = dest_vp['census_tract_id']
        dest_id_k    = dest_vp['vertiport_id']
        dest_node_k  = node_idx[dest_id_k]

        # Flight time for every passenger (vectorised; NaN where origin vp is unknown)
        valid_origin = origin_vp_idx_arr >= 0
        flight_times_k = np.full(N, np.nan)
        flight_times_k[valid_origin] = ft_matrix[origin_vp_idx_arr[valid_origin], dest_node_k]
        cand_flight_time[:, k] = flight_times_k

        # Arrival at destination vertiport
        arr_dest_vp_k = (
            df["arrival_time_at_origin_vertiport"]
            + pd.to_timedelta(MM_OVTT, unit="m")
            + pd.to_timedelta(pd.Series(flight_times_k, index=df.index), unit="m")
        )
        cand_arr_dest_vp.append(arr_dest_vp_k)
        last_hour_k = arr_dest_vp_k.dt.hour.values  # (N,) int

        # LM lookup: vectorised per hour bucket
        lm_dur_k  = np.full(N, np.nan)
        lm_fare_k = np.full(N, np.nan)

        for h in range(24):
            hmask = last_hour_k == h
            if not hmask.any():
                continue
            tm_h = time_cost_matrix["LM"][h]
            if dest_tract_k not in tm_h.index:
                continue

            lm_row = tm_h.loc[dest_tract_k]          # Series: dest_tract → LM minutes
            dt_h   = pd.Series(dest_tract_arr[hmask], index=np.where(hmask)[0])
            in_lm  = dt_h[dt_h.isin(lm_row.index)]
            lm_dur_k[in_lm.index] = lm_row[in_lm.values].values

            if dest_tract_k in dm_lm.index:
                dist_row = dm_lm.loc[dest_tract_k]
                in_dist  = in_lm[in_lm.isin(dist_row.index)]
                lm_fare_k[in_dist.index] = compute_taxi_fare(
                    duration=lm_dur_k[in_dist.index],
                    mile=dist_row[in_dist.values].values,
                )

        cand_lm_duration[:, k] = lm_dur_k
        cand_lm_fare[:, k]     = lm_fare_k

        # Total arrival at final destination (only where LM path exists)
        valid_lm = ~np.isnan(lm_dur_k)
        if valid_lm.any():
            vi = np.where(valid_lm)[0]
            arr_final = (
                arr_dest_vp_k.iloc[vi]
                + pd.to_timedelta(LM_OVTT + lm_dur_k[valid_lm], unit="m")
            )
            cand_total_sec[valid_lm, k] = (arr_final - BASE_TS).dt.total_seconds().values

    # Choose the candidate with the earliest total arrival for each passenger
    best_k = np.argmin(cand_total_sec, axis=1)  # (N,)

    # Build (N, K) matrix of dest-vertiport arrival times in int64 nanoseconds
    arr_dest_vp_ns = np.column_stack(
        [s.values.astype("int64") for s in cand_arr_dest_vp]
    )

    # Assign chosen results (same column names as before)
    df["dest_vertiport_id"]    = [candidate_dest_vps[k]['vertiport_id']    for k in best_k]
    df["dest_vertiport_tract"] = [candidate_dest_vps[k]['census_tract_id'] for k in best_k]
    df["MM_duration_min"]      = cand_flight_time[np.arange(N), best_k]
    df["LM_duration_min"]      = cand_lm_duration[np.arange(N), best_k]
    df["LM_fare_USD"]          = cand_lm_fare[np.arange(N), best_k]

    best_arr_ns = arr_dest_vp_ns[np.arange(N), best_k]
    df["arrival_time_at_dest_vertiport"] = pd.to_datetime(best_arr_ns)
    df["last_hour"] = df["arrival_time_at_dest_vertiport"].dt.hour

    df["RAM_IVTT_min"] = (
        pd.to_numeric(df["FM_duration_min"], errors="coerce")
        + pd.to_numeric(df["MM_duration_min"], errors="coerce")
        + pd.to_numeric(df["LM_duration_min"], errors="coerce")
    )

    df["RAM_OVTT_min"] = FM_OVTT + MM_OVTT + LM_OVTT

    df["RAM_Fare_USD"] = (
        pd.to_numeric(df["FM_fare_USD"],  errors="coerce")
        + RAM_fare
        + pd.to_numeric(df["LM_fare_USD"], errors="coerce")
    )

    df["Driving_IVTT_min"] = pd.to_numeric(df[SCHEMA.DURATION_MIN], errors="coerce")
    df["Driving_OVTT_min"] = DRIVING_OVTT_CONST

    # Driving fare: distance cost + parking cost at the passenger's destination vertiport
    df["Driving_Fare_USD"] = (
        pd.to_numeric(df[SCHEMA.DISTANCE_MI], errors="coerce") * cost_driving_per_mile
        + df["dest_vertiport_id"].map(vp_parking).fillna(0)
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
    parser.add_argument("--day", default="Thu", choices=["Thu", "Sat"], help="Day of week (default: Thu)")
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
        day=args.day,
        raw_data_dir=str(raw_data_dir),
        processed_data_dir=str(processed_data_dir),
        output_dir=str(output_dir),
    )
