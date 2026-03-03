from urllib.parse import uses_relative

import pandas as pd
from pathlib import Path
import json
import os
from metadata.uam_schema import UAMSchema

# Replica data column mapping and column standard
SCHEMA = UAMSchema()

# ========= Helpers =========

def load_vertiport_specification(veriport_spec_file):
    # load veriport specification
    with open(veriport_spec_file, 'r') as f:
        vertiport_spec = json.load(f)

    trip_assumptions = vertiport_spec['assumptions']
    # trip_segments = vertiport_spec["assumptions"]["multimodal_segments"]["option1_ram"]
    vertiports = vertiport_spec["vertiports"]

    return vertiports, trip_assumptions

def load_trip_data (o_city, d_city):
    """
    Hard-coded file names

    """
    trips_csv = f"{o_city}_{d_city}_Thur.csv"

    # ---- Load trips and preprocessing----
    df = pd.read_csv(os.path.join("data","raw_replica",trips_csv), low_memory=False)
    df.rename(columns=SCHEMA.mapping, inplace=True)  # replcae column names

    # Filter to driving modes
    df = df[df[SCHEMA.MODE].isin(["private_auto", "auto_passenger", "on_demand_auto"])].copy()

    # Parse time as HH:MM:SS
    df[SCHEMA.START_TIME] = pd.to_datetime(df[SCHEMA.START_TIME], format="%H:%M:%S", errors="coerce")
    df[SCHEMA.END_TIME] = pd.to_datetime(df[SCHEMA.END_TIME], format="%H:%M:%S", errors="coerce")

    # Convert tract IDs to string for consistent lookup
    df[SCHEMA.ORIGIN_TRACT] = df[SCHEMA.ORIGIN_TRACT].astype(str)
    df[SCHEMA.DESTINATION_TRACT] = df[SCHEMA.DESTINATION_TRACT].astype(str)


    return df


def load_cost_matrix(o_city, d_city):
    """
    Hard Coded Function!
    Load hourly OD matrices into a dict {hour: DataFrame}.
    Assumes filenames like: <corrected_prefix>_<od_prefix>_<HH>.csv
    """
    # OD cost matrix files (we assume corrected matrices)
    od_folder = {
        "FM": f"{o_city}_hourly_od_lookup_tables",
        "LM": f"{d_city}_hourly_od_lookup_tables"
    }

    od_prefix = {
        "FM": f"{o_city}_TT_Matrix",
        "LM": f"{d_city}_TT_Matrix"
    }

    od_dist_files = {
        "FM": f"od_distance_matrix/{o_city}_DIST_Matrix.csv",
        "LM": f"od_distance_matrix/{d_city}_DIST_Matrix.csv"
    }

    fm_dist_mat = pd.read_csv(
        os.path.join("data","od_cost_matrix",od_dist_files["FM"]),
        index_col=0)
    lm_dist_mat = pd.read_csv(
        os.path.join("data","od_cost_matrix",od_dist_files["LM"]),
        index_col=0)

    fm_dist_mat.index = fm_dist_mat.index.astype(str)
    fm_dist_mat.columns = fm_dist_mat.columns.astype(str)

    lm_dist_mat.index = lm_dist_mat.index.astype(str)
    lm_dist_mat.columns = lm_dist_mat.columns.astype(str)

    # distance cost
    distance_cost_matrix = {
        "FM": fm_dist_mat,
        "LM": lm_dist_mat
    }

    # time_cost
    time_cost_matrix = {}
    hours = range(24)

    for key in ['FM', 'LM']:
        matrices = {}
        for h in hours:
            path = os.path.join("data","od_cost_matrix", od_folder[key], f"{od_prefix[key]}_{h:02d}.csv")
            # Path(od_folder[key]) / f"{od_prefix[key]}_{h:02d}.csv"

            df = pd.read_csv(path, index_col=0)

            # Make sure index/columns are strings for consistent lookup
            df.index = df.index.astype(str)
            df.columns = df.columns.astype(str)

            matrices[h] = df

        time_cost_matrix[key] = matrices

    return distance_cost_matrix, time_cost_matrix

def compute_taxi_fare(duration, mile):
    fare = 2.2 + duration*0.42 + mile*1.6 + 1.7

    return fare

def compute_middle_mile_trips (replica_data_row):
    """
    :param replica_data_row:
        single row data from Replica as a raw input
    :return:
    """
    pass

def compute_ram_trip_statistics(
    veriport_spec_file = "metadata/vertiport_specification.json",
    o_city : str = "Chicago",
    d_city : str = "UIUC"
):
    """
    -o [segment_option] : "option1_ram"
    """
    segment_options = ["option1_ram", "option2_car"]

    output_csv = f"output/{o_city}_{d_city}_RAM_trip_stats.csv"

    # ========= Global config =========
    vertiports, trip_assumptions = load_vertiport_specification(veriport_spec_file)

    # check specification validity
    if trip_assumptions['assignment_strategy']['type'] != "nearest":
        raise Exception("current set up only allows \"nearest\" assignment strategy!")

    trip_segments_choice1 = trip_assumptions["multimodal_segments"][segment_options[0]]
    trip_segments_choice2 = trip_assumptions["multimodal_segments"][segment_options[1]]

    # parameters
    FM_OVTT = trip_segments_choice1[0]['ovtt_min']
    LM_OVTT = trip_segments_choice1[1]['ovtt_min']
    MIDDLE_MILE_FLIGHT_MIN = 70
    RAM_fare = 100
    DRIVING_OVTT_CONST = 3.0  # minutes, fixed value
    cost_driving_per_mile = 0.6

    # load data
    df = load_trip_data(o_city, d_city)

    # load cost matrix
    distance_cost_matrix, time_cost_matrix = load_cost_matrix(o_city, d_city)

    # First mile
    # ---- Compute first-mile times hour by hour ----
    df["FM_duration_min"] = pd.NA
    df["FM_fare_USD"] = pd.NA
    df["depart_hour"] = df[SCHEMA.START_TIME].dt.hour

    for h in range(24):
        # First mile: origin tract -> origin vertiport tract
        mask_first = df["depart_hour"] == h
        time_cost_matrix_h = time_cost_matrix["FM"][h]

        if mask_first.any():
            origin_tracts = df.loc[mask_first, SCHEMA.ORIGIN_TRACT]

            # find candidate tracts with vertiport locations
            candidate_tracts = [
                vp['census_tract_id'] for vp in vertiports
                if vp['city'] == o_city and vp['census_tract_id'] != '-1'
            ]

            # convert from census tract id to vertiport id
            tract_to_vp_id = {
                vp['census_tract_id']: vp['vertiport_id']
                for vp in vertiports
                if vp['census_tract_id'] != '-1'
            }

            if not candidate_tracts:
                raise Exception("not vertiport available for some trip takers")

            # --- THE CORE COMPUTATION ---
            ### Assumption: nearest vertiport
            routing_options = time_cost_matrix_h.loc[origin_tracts, candidate_tracts]
            routing_options.index = origin_tracts.index

            # Find the ID of the nearest vertiport (idxmin) and the travel time (min)
            origin_vertiport_trct = routing_options.idxmin(axis=1)

            df.loc[mask_first, 'origin_vertiport_id'] = origin_vertiport_trct.map(tract_to_vp_id)
            df.loc[mask_first, 'origin_vertiport_tract'] = origin_vertiport_trct


            # vectorized lookup for specific pairs
            duration_vals = routing_options.min(axis=1)
            dist_vals = pd.Series(
                [distance_cost_matrix["FM"].at[o, d] for o, d in zip(origin_tracts, origin_vertiport_trct)],
                index=origin_tracts.index
            )
            # dist_vals = distance_cost_matrix["FM"].loc[origin_tracts, origin_vertiport_trct]
            #
            # # -------- ERROR CHECKING FOR MISSING VALUES --------
            # bad_mask = duration_vals.isna() | dist_vals.isna()
            #
            # if bad_mask.any():
            #     raise Exception("DEBUG: no duration values")
            #     sub_idx = df.index[mask_first]  # row indices of subset
            #
            #     # ---------- GLOBAL AVERAGE FALLBACK ----------
            #     # Compute global averages from your FM tables
            #     global_dur_mean = time_cost_matrix_h.loc[:, origin_vertiport_trct].mean()
            #     global_dist_mean = distance_cost_matrix["FM"].loc[:, origin_vertiport_trct].mean()
            #
            #     # Apply fallback: replace missing duration + distance with global means
            #     duration_vals.loc[bad_mask] = global_dur_mean
            #     dist_vals.loc[bad_mask] = global_dist_mean
            #
            #     # dist_vals = pd.Series(
            #     #     [distance_cost_matrix["FM"].at[o, d] for o, d in zip(origin_tracts, origin_vertiport_trct)],
            #     #     index=origin_tracts.index)
            #     # ------------------------------------------------------

            df.loc[mask_first, "FM_duration_min"] = duration_vals.values
            df.loc[mask_first, "FM_fare_USD"] = compute_taxi_fare(duration=duration_vals.values, mile= dist_vals.values)

    # Hour for last mile (approx: add middle mile time, then take hour)
    df["arrival_time_at_dest_vertiport"] = (df[SCHEMA.START_TIME]
                                            + pd.to_timedelta(df["FM_duration_min"], unit="m")
                                            + pd.to_timedelta(FM_OVTT, unit="m")
                                            + pd.to_timedelta(MIDDLE_MILE_FLIGHT_MIN, unit="m") )

    df["last_hour"] = df["arrival_time_at_dest_vertiport"].dt.hour

    # ---- Compute last-mile times hour by hour ----
    df["LM_duration_min"] = pd.NA
    df["dest_vertiport_id"] = pd.NA
    df["dest_vertiport_tract"] = pd.NA

    # dest_vertiport_trct = [
    #             vp['census_tract_id'] for vp in vertiports
    #             if vp['city'] == d_city
    #         ]

    candidate_tracts_d = [
        vp['census_tract_id'] for vp in vertiports
        if vp['city'] == d_city and vp['census_tract_id'] != '-1'
    ]

    # dest_vertiport_trct = dest_vertiport_trct[0]

    if not candidate_tracts_d:
        raise Exception(f"No vertiports available in destination city: {d_city}")

    # 2. Map for converting tract ID back to Vertiport ID
    tract_to_vp_id_d = {
        vp['census_tract_id']: vp['vertiport_id']
        for vp in vertiports
        if vp['city'] == d_city and vp['census_tract_id'] != '-1'
    }

    for h in range(24):
        time_cost_matrix_h = time_cost_matrix["LM"][h]
        # Last mile: dest vertiport tract -> destination tract
        mask_last = df["last_hour"] == h
        if mask_last.any():
            dest_tracts = df.loc[mask_last, SCHEMA.DESTINATION_TRACT]

            # --- THE CORE NEAREST COMPUTATION ---
            # Matrix lookup: rows are candidate vertiports, columns are trip destinations
            # We want to find the best vertiport (row) for each destination tract (column)
            routing_options = time_cost_matrix_h.loc[candidate_tracts_d, dest_tracts]

            # Align indices to match the original dataframe trip indices
            routing_options.columns = dest_tracts.index

            # Find the nearest vertiport tract for each trip (idxmin across rows)
            chosen_dest_vps_trct = routing_options.idxmin(axis=0)

            # Store Vertiport IDs and Tracts
            df.loc[mask_last, "dest_vertiport_tract"] = chosen_dest_vps_trct.values
            df.loc[mask_last, "dest_vertiport_id"] = chosen_dest_vps_trct.map(tract_to_vp_id_d).values

            # Get the minimum duration and corresponding distance
            duration_vals = routing_options.min(axis=0)

            # Distance lookup for specific (Vertiport -> Final Destination) pairs
            dist_vals = pd.Series(
                [distance_cost_matrix["LM"].at[v, d] for v, d in zip(chosen_dest_vps_trct, dest_tracts)],
                index=dest_tracts.index
            )

            # Apply values back to the main dataframe
            df.loc[mask_last, "LM_duration_min"] = duration_vals.values
            df.loc[mask_last, "LM_fare_USD"] = compute_taxi_fare(
                duration=duration_vals.values,
                mile=dist_vals.values
            )

            # duration_vals = time_cost_matrix_h.loc[dest_vertiport_trct, dests]
            # dist_vals = distance_cost_matrix["LM"].loc[dest_vertiport_trct, dests]
            #
            # # This comes back as a Series with dests as index,
            # # so just align by order using .values
            # df.loc[mask_last, "LM_duration_min"] = duration_vals.values
            # df.loc[mask_last, "LM_fare_USD"] = compute_taxi_fare(duration=duration_vals.values, mile=dist_vals.values)

            # # -------- ERROR CHECKING FOR MISSING VALUES --------
            # bad_mask = duration_vals.isna() | dist_vals.isna()
            #
            # if bad_mask.any():
            #     sub_idx = df.index[mask_last]  # row indices of subset
            #
            #     # ---------- GLOBAL AVERAGE FALLBACK ----------
            #     # Compute global averages from your FM tables
            #     global_dur_mean = time_cost_matrix_h.loc[dest_vertiport_trct, :].mean()
            #     global_dist_mean = distance_cost_matrix["LM"].loc[dest_vertiport_trct, :].mean()
            #
            #     # Apply fallback: replace missing duration + distance with global means
            #     duration_vals.loc[bad_mask] = global_dur_mean
            #     dist_vals.loc[bad_mask] = global_dist_mean
            #     # ------------------------------------------------------

    # Middle mile: constant for all trips
    df["MM_duration_min"] = MIDDLE_MILE_FLIGHT_MIN

    # RAM_IVTT = first + middle + last (in minutes)
    df["RAM_IVTT_min"] = (
        df["FM_duration_min"].astype("float64")
        + df["MM_duration_min"].astype("float64")
        + df["LM_duration_min"].astype("float64")
    )

    df["RAM_OVTT_min"] = FM_OVTT + LM_OVTT

    df["RAM_Fare_USD"] = (df["FM_fare_USD"].astype("float64") +
                          RAM_fare +
                          df["LM_fare_USD"].astype("float64"))


    # ------------------------------------------------------------------
    # 2) Driving_OVTT and Driving_IVTT
    # ------------------------------------------------------------------

    df["Driving_IVTT_min"] = pd.to_numeric(df[SCHEMA.DURATION_MIN], errors="coerce")
    df["Driving_OVTT_min"] = DRIVING_OVTT_CONST

    # ------------------------------------------------------------------
    # 3) Driving_Fare
    # ------------------------------------------------------------------
    df["Driving_Fare_USD"] = (
            pd.to_numeric(df[SCHEMA.DISTANCE_MI], errors="coerce") * cost_driving_per_mile
    )

    # ------------------------------------------------------------------
    # 1) Income flag columns
    # ------------------------------------------------------------------
    income_col = SCHEMA.USER_INCOME

    if income_col in df.columns:

        income = pd.to_numeric(df[income_col], errors="coerce")

        # Base flags
        df["Income_low"] = (income < 45000).astype(int)
        df["Income_mid"] = ((income >= 45000) & (income <= 152000)).astype(int)
        df["Income_high"] = (income > 152000).astype(int)

        # If income is missing or NaN → assign to mid
        missing_mask = income.isna()
        df.loc[missing_mask, ["Income_low", "Income_mid", "Income_high"]] = [0, 1, 0]

    else:
        # Column missing → only create mid flag as 1
        df["Income_mid"] = 1


    # ---- Save ----
    df.to_csv(output_csv, index=False)
    print(f"Saved RAM trip stats to: {output_csv}")

    # separated files

    # Group by the pair of vertiport IDs
    grouped = df.groupby(['origin_vertiport_id', 'dest_vertiport_id'])

    for (o_vp, d_vp), group_df in grouped:
        # Create a clean filename (e.g., Chicago1_UIUC_Hub_trips.csv)
        partition_filename = f"{o_vp}_{d_vp}_trips.csv"
        partition_path = f"output/{partition_filename}"

        group_df.to_csv(partition_path, index=False)
        print(f"Saved {len(group_df)} trips to: {partition_path}")

    return df


if __name__ == "__main__":
    df = compute_ram_trip_statistics(o_city="UIUC", d_city="Chicago")

