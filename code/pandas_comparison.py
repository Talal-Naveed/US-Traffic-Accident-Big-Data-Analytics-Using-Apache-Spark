import os
import time
import pandas as pd
import matplotlib.pyplot as plt

# ============================================================
# 1. FILE PATH
# ============================================================

file_path = r"..\data\US_Accidents_March23.csv"

output_path = r"..\outputs\comparison"
os.makedirs(output_path, exist_ok=True)


# ============================================================
# 2. LOAD DATASET USING PANDAS
# ============================================================

print("\n==================================================")
print("PANDAS COMPARISON")
print("==================================================")

start_time = time.perf_counter()

df = pd.read_csv(
    file_path,
    usecols=[
        "ID",
        "Severity",
        "Start_Time",
        "State"
    ]
)

load_time = time.perf_counter() - start_time

print(f"\nDataset loading time: {load_time:.4f} seconds")
print("Original rows:", len(df))


# ============================================================
# 3. PREPROCESSING
# ============================================================

start_time = time.perf_counter()

df["Start_Time"] = pd.to_datetime(
    df["Start_Time"],
    format="mixed",
    errors="coerce"
)

print(
    "Unparsed Start_Time values:",
    df["Start_Time"].isna().sum()
)

df = df[
    (df["Start_Time"] >= "2019-01-01") &
    (df["Start_Time"] < "2023-04-01")
].copy()

df = df.dropna(
    subset=[
        "ID",
        "Severity",
        "Start_Time",
        "State"
    ]
)

df["Year"] = df["Start_Time"].dt.year

preprocessing_time = time.perf_counter() - start_time

print(f"Preprocessing time: {preprocessing_time:.4f} seconds")
print("Filtered rows:", len(df))


# ============================================================
# 4. ACCIDENTS BY YEAR
# ============================================================

start_time = time.perf_counter()

yearly_accidents = (
    df.groupby("Year")
    .size()
    .reset_index(name="count")
    .sort_values("Year")
)

year_time = time.perf_counter() - start_time

print("\nAccidents by Year:")
print(yearly_accidents)

print(
    f"Year analysis execution time: "
    f"{year_time:.6f} seconds"
)


# ============================================================
# 5. ACCIDENTS BY STATE
# ============================================================

start_time = time.perf_counter()

state_accidents = (
    df.groupby("State")
    .size()
    .reset_index(name="count")
    .sort_values(
        "count",
        ascending=False
    )
)

state_time = time.perf_counter() - start_time

print("\nTop 10 States:")
print(state_accidents.head(10))

print(
    f"State analysis execution time: "
    f"{state_time:.6f} seconds"
)


# ============================================================
# 6. SEVERITY DISTRIBUTION
# ============================================================

start_time = time.perf_counter()

severity_accidents = (
    df.groupby("Severity")
    .size()
    .reset_index(name="count")
    .sort_values("Severity")
)

severity_time = time.perf_counter() - start_time

print("\nSeverity Distribution:")
print(severity_accidents)

print(
    f"Severity analysis execution time: "
    f"{severity_time:.6f} seconds"
)


# ============================================================
# 7. SAVE PANDAS RESULTS
# ============================================================

yearly_accidents.to_csv(
    os.path.join(
        output_path,
        "pandas_accidents_by_year.csv"
    ),
    index=False
)

state_accidents.head(10).to_csv(
    os.path.join(
        output_path,
        "pandas_top_10_states.csv"
    ),
    index=False
)

severity_accidents.to_csv(
    os.path.join(
        output_path,
        "pandas_severity_distribution.csv"
    ),
    index=False
)


# ============================================================
# 8. SAVE EXECUTION TIMES
# ============================================================

timing_results = pd.DataFrame({
    "Operation": [
        "Dataset Loading",
        "Preprocessing",
        "Accidents by Year",
        "Accidents by State",
        "Severity Distribution"
    ],
    "Pandas_Time_Seconds": [
        load_time,
        preprocessing_time,
        year_time,
        state_time,
        severity_time
    ]
})

timing_results.to_csv(
    os.path.join(
        output_path,
        "pandas_execution_times.csv"
    ),
    index=False
)


print("\nPandas comparison completed successfully.")

# ============================================================
# 9. SPARK VS PANDAS PERFORMANCE COMPARISON
# ============================================================

# Spark execution times obtained from main_analysis.py
spark_year_time = 11.776039
spark_state_time = 13.742439
spark_severity_time = 10.952268


comparison_results = pd.DataFrame({
    "Operation": [
        "Accidents by Year",
        "Accidents by State",
        "Severity Distribution"
    ],
    "Spark_Time_Seconds": [
        spark_year_time,
        spark_state_time,
        spark_severity_time
    ],
    "Pandas_Time_Seconds": [
        year_time,
        state_time,
        severity_time
    ]
})


# ============================================================
# 10. DISPLAY COMPARISON
# ============================================================

print("\n==================================================")
print("SPARK VS PANDAS PERFORMANCE COMPARISON")
print("==================================================")

print(comparison_results.to_string(index=False))


# ============================================================
# 11. SAVE COMPARISON TABLE
# ============================================================

comparison_results.to_csv(
    os.path.join(
        output_path,
        "spark_vs_pandas_comparison.csv"
    ),
    index=False
)


# ============================================================
# 12. CREATE COMPARISON GRAPH
# ============================================================

comparison_plot = comparison_results.set_index("Operation")

ax = comparison_plot[
    ["Spark_Time_Seconds", "Pandas_Time_Seconds"]
].plot(
    kind="bar",
    figsize=(10, 6)
)

plt.xlabel("Analysis Operation")
plt.ylabel("Execution Time (Seconds)")
plt.title("Spark vs Pandas Execution Time")

plt.xticks(rotation=20, ha="right")

plt.legend([
    "Spark",
    "Pandas"
])

plt.tight_layout()

plt.savefig(
    os.path.join(
        output_path,
        "spark_vs_pandas_comparison.png"
    ),
    dpi=300
)

plt.close()


print(
    "\nSpark vs Pandas comparison files "
    "saved successfully."
)