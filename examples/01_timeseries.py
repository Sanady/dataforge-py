"""Time-Series Generation — IoT Sensor Monitoring Dashboard.

Real-world scenario: Generate synthetic sensor data for an IoT monitoring
platform. Simulates temperature and humidity sensors with realistic daily
cycles, gradual drift, occasional anomalies, and missing readings.

This example demonstrates:
- Configuring trend, seasonality, noise, and anomalies
- Clamping values to physical bounds
- Handling missing data points
- Regime changes (e.g., HVAC failure)
- Exporting to CSV and JSON
"""

from dataforge import DataForge
from dataforge.timeseries import TimeSeriesSchema

forge = DataForge(seed=42)

# --- Example 1: Basic temperature sensor ---------------------------------

print("=== Basic Temperature Sensor (1 week, hourly) ===\n")

ts = TimeSeriesSchema(
    forge,
    start="2024-06-01",
    end="2024-06-07",
    interval="1h",
    fields={
        "temperature_c": {
            "base": 22.0,  # baseline 22 degrees C
            "trend": 0.005,  # slight warming trend per step
            "seasonality": {
                "period": 24,  # 24-hour daily cycle
                "amplitude": 5.0,  # +/- 5 degrees swing
            },
            "noise": 0.3,  # small random fluctuations
            "min_val": 10.0,  # physical minimum
            "max_val": 45.0,  # physical maximum
        },
    },
)

rows = ts.generate()
print(f"Generated {len(rows)} data points")
print("First 5 rows:")
for row in rows[:5]:
    print(f"  {row['timestamp']}  temp={row['temperature_c']}")
print()

# --- Example 2: Multi-sensor with anomalies and missing data -------------

print("=== Multi-Sensor with Anomalies and Missing Data ===\n")

multi_ts = TimeSeriesSchema(
    forge,
    start="2024-01-01",
    end="2024-01-31",
    interval="30m",
    fields={
        "temperature_c": {
            "base": 21.0,
            "trend": 0.001,
            "seasonality": {"period": 48, "amplitude": 4.0},  # 48 half-hours = 24h
            "noise": 0.5,
            "anomaly_rate": 0.005,  # 0.5% chance of anomaly per reading
            "anomaly_scale": 4.0,  # anomalies are 4x the noise
            "missing_rate": 0.02,  # 2% of readings are missing
            "min_val": -10.0,
            "max_val": 50.0,
        },
        "humidity_pct": {
            "base": 55.0,
            "trend": -0.002,
            "seasonality": {"period": 48, "amplitude": 15.0, "phase": 12},
            "noise": 2.0,
            "missing_rate": 0.01,
            "min_val": 0.0,
            "max_val": 100.0,
        },
        "pressure_hpa": {
            "base": 1013.25,
            "trend": 0.0,
            "noise": 1.5,
            "spike_rate": 0.001,  # rare pressure spikes
            "spike_scale": 3.0,
            "min_val": 950.0,
            "max_val": 1060.0,
        },
    },
)

rows = multi_ts.generate()
print(f"Generated {len(rows)} multi-sensor readings")

# Count missing values
missing_temp = sum(1 for r in rows if r["temperature_c"] is None)
missing_hum = sum(1 for r in rows if r["humidity_pct"] is None)
print(f"Missing temperature readings: {missing_temp}")
print(f"Missing humidity readings: {missing_hum}")
print()

# Show a sample
print("Sample readings:")
for row in rows[100:105]:
    t = row["temperature_c"]
    h = row["humidity_pct"]
    p = row["pressure_hpa"]
    print(
        f"  {row['timestamp']}  "
        f"temp={'N/A' if t is None else f'{t:.1f}C'}  "
        f"hum={'N/A' if h is None else f'{h:.0f}%'}  "
        f"press={p:.1f}hPa"
    )
print()

# --- Example 3: Regime change (HVAC failure simulation) -------------------

print("=== Regime Change — HVAC Failure ===\n")

hvac_ts = TimeSeriesSchema(
    forge,
    start="2024-03-01",
    end="2024-03-03",
    interval="15m",
    fields={
        "room_temp_c": {
            "base": 22.0,
            "trend": 0.0,
            "seasonality": {"period": 96, "amplitude": 1.0},  # subtle daily cycle
            "noise": 0.2,
            "regime_changes": [
                # HVAC fails at step 48 (12 hours in): temperature starts rising
                {"at_step": 48, "base": 22.0, "trend": 0.15},
                # HVAC fixed at step 96 (24 hours in): returns to normal
                {"at_step": 96, "base": 22.0, "trend": 0.0},
            ],
            "min_val": 15.0,
            "max_val": 40.0,
        },
    },
)

rows = hvac_ts.generate()
print(f"Generated {len(rows)} readings (15-min intervals over 2 days)")
print(f"Before failure (step 0):   temp={rows[0]['room_temp_c']}")
print(f"During failure (step 72):  temp={rows[72]['room_temp_c']}")
print(f"After repair (step 120):   temp={rows[120]['room_temp_c']}")
print()

# --- Example 4: Export to CSV and JSON ------------------------------------

print("=== Export to CSV ===\n")

export_ts = TimeSeriesSchema(
    forge,
    start="2024-07-01",
    end="2024-07-02",
    interval="1h",
    fields={
        "temperature": {
            "base": 25.0,
            "noise": 1.0,
            "seasonality": {"period": 24, "amplitude": 3.0},
        },
        "wind_speed": {"base": 10.0, "noise": 3.0, "min_val": 0.0},
    },
)

csv_output = export_ts.to_csv()
lines = csv_output.strip().split("\n")
print(f"CSV output: {len(lines)} lines (including header)")
print(f"Header: {lines[0]}")
print(f"First row: {lines[1]}")
print()

# Export to file (uncomment to save):
# export_ts.to_csv(path="weather_data.csv")
# export_ts.to_json(path="weather_data.json")

print("=== Stream rows lazily ===\n")
count = 0
for row in export_ts.stream():
    count += 1
print(f"Streamed {count} rows")
