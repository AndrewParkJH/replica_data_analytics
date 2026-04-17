# replica_data_analytics

Scripts for generating OD cost matrices and multimodal RAM trip demand data from Replica trip data.

## Scripts

| Script | Purpose |
|--------|---------|
| `generate_regional_od_cost_lookup_tables.py` | Builds hourly travel-time OD matrices and a distance OD matrix from raw Replica CSV data |
| `generate_multimodal_trip_data.py` | Assigns vertiports and computes per-trip RAM vs. driving statistics |

These scripts are designed to be run from the **project root** (one level above this directory), so that the default data paths resolve correctly.

---

## Standalone usage

### Step 1 — Generate OD cost matrices

Run once per city. Results are cached; re-running will overwrite existing files.

```bash
# From the project root
python external/replica_data_analytics/generate_regional_od_cost_lookup_tables.py \
    --state <state> \
    --city <city>
```

**Required args:**

| Arg | Description |
|-----|-------------|
| `--state` | State name matching the raw data folder (e.g. `Illinois`) |
| `--city` | City name (e.g. `Chicago`) |

**Optional path overrides** (defaults shown):

| Arg | Default |
|-----|---------|
| `--raw_data_dir` | `<cwd>/data/replica_data` |
| `--processed_data_dir` | `<cwd>/data/processed_data` |

**Example:**
```bash
python external/replica_data_analytics/generate_regional_od_cost_lookup_tables.py \
    --state Illinois --city Chicago

python external/replica_data_analytics/generate_regional_od_cost_lookup_tables.py \
    --state Illinois --city UIUC
```

**Output** written to `data/processed_data/od_cost_matrix/<state>/`:
```
od_cost_matrix/
└── Illinois/
    ├── Chicago_hourly_od_lookup_tables/
    │   ├── Chicago_TT_Matrix_00.csv
    │   ├── Chicago_TT_Matrix_01.csv
    │   └── ... (24 files)
    └── od_distance_matrix/
        └── Chicago_DIST_Matrix.csv
```

---

### Step 2 — Generate trip demand data

Requires OD cost matrices for both origin and destination cities (Step 1).

```bash
# From the project root
python external/replica_data_analytics/generate_multimodal_trip_data.py \
    --state <state> \
    --o_city <origin_city> \
    --d_city <destination_city> \
    --vertiport_config <path_to_config.json>
```

**Required args:**

| Arg | Description |
|-----|-------------|
| `--state` | State name (e.g. `Illinois`) |
| `--o_city` | Origin city (e.g. `Chicago`) |
| `--d_city` | Destination city (e.g. `UIUC`) |
| `--vertiport_config` | Path to vertiport JSON config (e.g. `config/vertiport_configuration/UIUC.json`) |

**Optional path overrides** (defaults shown):

| Arg | Default |
|-----|---------|
| `--raw_data_dir` | `<cwd>/data/replica_data` |
| `--processed_data_dir` | `<cwd>/data/processed_data` |
| `--output_dir` | `<cwd>/data/processed_data/demand` |

**Example:**
```bash
python external/replica_data_analytics/generate_multimodal_trip_data.py \
    --state Illinois --o_city Chicago --d_city UIUC \
    --vertiport_config config/vertiport_configuration/UIUC.json
```

**Output** written to `data/processed_data/demand/`:
```
demand/
├── Chicago_UIUC_RAM_trip_stats.csv
├── Chicago1_UIUC_Hub_trips.csv
└── ...
```

---

## Expected input data layout

```
data/
└── replica_data/
    └── <state>/
        └── <o_city>_<d_city>_Thur.csv    ← raw Replica trip export
```

---

## Running via the project pipeline

For the full automated workflow (OD matrix generation + demand in one command, with caching), use the root-level pipeline instead:

```bash
python run_pipeline.py \
    --state Illinois \
    --o_city Chicago \
    --d_city UIUC \
    --vertiport_config config/vertiport_configuration/UIUC.json
```
