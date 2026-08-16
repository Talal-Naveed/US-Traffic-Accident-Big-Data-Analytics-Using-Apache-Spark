import os
import pandas as pd
import matplotlib.pyplot as plt
import time

# ============================================================
# 1. SPARK SETUP
# ============================================================

# Tell PySpark which Python installation to use
python_path = r"C:\Users\tnave\.venv\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    hour,
    date_format,
    count,
    when
)

# Create local Spark session
spark = (
    SparkSession.builder
    .appName("US Accidents Big Data Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 2. LOAD DATASET
# ============================================================

file_path = r"..\data\US_Accidents_March23.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)

print("\n==================================================")
print("ORIGINAL DATASET")
print("==================================================")

print("Original rows:", df.count())
print("Original columns:", len(df.columns))


# ============================================================
# 3. FILTER STUDY PERIOD
# January 2019 to March 2023
# ============================================================

df_filtered = df.filter(
    (col("Start_Time") >= "2019-01-01 00:00:00") &
    (col("Start_Time") < "2023-04-01 00:00:00")
)

print("\n==================================================")
print("FILTERED DATASET: JAN 2019 - MAR 2023")
print("==================================================")

print("Filtered rows:", df_filtered.count())


# ============================================================
# 4. CHECK DUPLICATE ACCIDENT IDs
# ============================================================

duplicate_ids = (
    df_filtered
    .groupBy("ID")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_ids.count()

print("\nDuplicate IDs:", duplicate_count)


# ============================================================
# 5. CHECK MISSING VALUES
# ============================================================

analysis_columns = [
    "ID",
    "Severity",
    "Start_Time",
    "City",
    "State",
    "Temperature(F)",
    "Humidity(%)",
    "Visibility(mi)",
    "Wind_Speed(mph)",
    "Precipitation(in)",
    "Weather_Condition",
    "Sunrise_Sunset"
]

print("\n==================================================")
print("MISSING VALUES")
print("==================================================")

missing_values = df_filtered.select(
    [
        count(when(col(c).isNull(), c)).alias(c)
        for c in analysis_columns
    ]
)

missing_values.show(truncate=False)


# ============================================================
# 6. REMOVE RECORDS MISSING ESSENTIAL INFORMATION
# ============================================================

df_clean = df_filtered.dropna(
    subset=[
        "ID",
        "Start_Time",
        "Severity",
        "State"
    ]
)

print("\nRows after removing missing essential values:")
print(df_clean.count())


# ============================================================
# 7. CREATE TEMPORAL VARIABLES
# ============================================================

df_clean = (
    df_clean
    .withColumn("Year", year(col("Start_Time")))
    .withColumn("Month", month(col("Start_Time")))
    .withColumn("DayOfWeek", date_format(col("Start_Time"), "EEEE"))
    .withColumn("Hour", hour(col("Start_Time")))
)

print("\n==================================================")
print("PREPROCESSED DATA SAMPLE")
print("==================================================")

df_clean.select(
    "ID",
    "Start_Time",
    "Year",
    "Month",
    "DayOfWeek",
    "Hour",
    "State",
    "City",
    "Severity"
).show(10, truncate=False)

# ============================================================
# 8. TEMPORAL ANALYSIS
# ============================================================

print("\n==================================================")
print("TEMPORAL ANALYSIS")
print("==================================================")


# ------------------------------------------------------------
# 8.1 ACCIDENTS BY YEAR
# ------------------------------------------------------------

start_time = time.perf_counter()

yearly_accidents = (
    df_clean
    .groupBy("Year")
    .count()
    .orderBy("Year")
)

yearly_accidents.collect()

spark_year_time = time.perf_counter() - start_time

print("\nAccidents by Year:")
yearly_accidents.show()

print(
    f"Spark year analysis execution time: "
    f"{spark_year_time:.6f} seconds"
)

# ------------------------------------------------------------
# 8.2 ACCIDENTS BY MONTH
# ------------------------------------------------------------

monthly_accidents = (
    df_clean
    .groupBy("Month")
    .count()
    .orderBy("Month")
)

print("\nAccidents by Month:")
monthly_accidents.show(12)


# ------------------------------------------------------------
# 8.3 ACCIDENTS BY DAY OF WEEK
# ------------------------------------------------------------

daily_accidents = (
    df_clean
    .groupBy("DayOfWeek")
    .count()
    .orderBy(col("count").desc())
)

print("\nAccidents by Day of Week:")
daily_accidents.show(7)


# ------------------------------------------------------------
# 8.4 ACCIDENTS BY HOUR
# ------------------------------------------------------------

hourly_accidents = (
    df_clean
    .groupBy("Hour")
    .count()
    .orderBy("Hour")
)

print("\nAccidents by Hour:")
hourly_accidents.show(24)

# ============================================================
# 9. SAVE TEMPORAL RESULTS AND CREATE GRAPHS
# ============================================================

output_path = r"..\outputs\temporal"

os.makedirs(output_path, exist_ok=True)


# ------------------------------------------------------------
# 9.1 ACCIDENTS BY YEAR
# ------------------------------------------------------------

year_pd = yearly_accidents.toPandas()

year_pd.to_csv(
    os.path.join(output_path, "accidents_by_year.csv"),
    index=False
)

plt.figure(figsize=(8, 5))
plt.bar(year_pd["Year"], year_pd["count"])
plt.xlabel("Year")
plt.ylabel("Number of Accidents")
plt.title("US Traffic Accidents by Year")
plt.tight_layout()

plt.savefig(
    os.path.join(output_path, "accidents_by_year.png"),
    dpi=300
)

plt.close()


# ------------------------------------------------------------
# 9.2 ACCIDENTS BY MONTH
# ------------------------------------------------------------

month_pd = monthly_accidents.toPandas()

month_pd.to_csv(
    os.path.join(output_path, "accidents_by_month.csv"),
    index=False
)

plt.figure(figsize=(9, 5))
plt.bar(month_pd["Month"], month_pd["count"])
plt.xlabel("Month")
plt.ylabel("Number of Accidents")
plt.title("US Traffic Accidents by Month")
plt.xticks(range(1, 13))
plt.tight_layout()

plt.savefig(
    os.path.join(output_path, "accidents_by_month.png"),
    dpi=300
)

plt.close()


# ------------------------------------------------------------
# 9.3 ACCIDENTS BY DAY OF WEEK
# ------------------------------------------------------------

day_pd = daily_accidents.toPandas()

day_order = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday"
]

day_pd["DayOfWeek"] = pd.Categorical(
    day_pd["DayOfWeek"],
    categories=day_order,
    ordered=True
)

day_pd = day_pd.sort_values("DayOfWeek")

day_pd.to_csv(
    os.path.join(output_path, "accidents_by_day.csv"),
    index=False
)

plt.figure(figsize=(9, 5))
plt.bar(day_pd["DayOfWeek"], day_pd["count"])
plt.xlabel("Day of Week")
plt.ylabel("Number of Accidents")
plt.title("US Traffic Accidents by Day of Week")
plt.xticks(rotation=30)
plt.tight_layout()

plt.savefig(
    os.path.join(output_path, "accidents_by_day.png"),
    dpi=300
)

plt.close()


# ------------------------------------------------------------
# 9.4 ACCIDENTS BY HOUR
# ------------------------------------------------------------

hour_pd = hourly_accidents.toPandas()

hour_pd.to_csv(
    os.path.join(output_path, "accidents_by_hour.csv"),
    index=False
)

plt.figure(figsize=(10, 5))
plt.plot(
    hour_pd["Hour"],
    hour_pd["count"],
    marker="o"
)

plt.xlabel("Hour of Day")
plt.ylabel("Number of Accidents")
plt.title("US Traffic Accidents by Hour")
plt.xticks(range(0, 24))
plt.tight_layout()

plt.savefig(
    os.path.join(output_path, "accidents_by_hour.png"),
    dpi=300
)

plt.close()


print("\nTemporal result files and graphs saved successfully.")

# ============================================================
# 10. GEOGRAPHIC ANALYSIS
# ============================================================

print("\n==================================================")
print("GEOGRAPHIC ANALYSIS")
print("==================================================")


# ------------------------------------------------------------
# 10.1 ACCIDENTS BY STATE
# ------------------------------------------------------------

start_time = time.perf_counter()

state_accidents = (
    df_clean
    .groupBy("State")
    .count()
    .orderBy(col("count").desc())
)

state_accidents.collect()

spark_state_time = time.perf_counter() - start_time

print("\nAccidents by State:")
state_accidents.show(50)

print(
    f"Spark state analysis execution time: "
    f"{spark_state_time:.6f} seconds"
)

# Top 10 states
top_10_states = state_accidents.limit(10)

print("\nTop 10 States by Number of Accidents:")
top_10_states.show(10)

# ------------------------------------------------------------
# 10.2 ACCIDENTS BY CITY
# ------------------------------------------------------------

# Exclude records where City is missing
city_data = df_clean.filter(col("City").isNotNull())

city_accidents = (
    city_data
    .groupBy("State", "City")
    .count()
    .orderBy(col("count").desc())
)

print("\nTop 10 Cities by Number of Accidents:")
city_accidents.show(10)

# ============================================================
# 11. SAVE GEOGRAPHIC RESULTS AND CREATE GRAPHS
# ============================================================

geo_output_path = r"..\outputs\geographic"

os.makedirs(geo_output_path, exist_ok=True)


# ------------------------------------------------------------
# 11.1 SAVE STATE RESULTS
# ------------------------------------------------------------

state_pd = state_accidents.toPandas()

state_pd.to_csv(
    os.path.join(geo_output_path, "accidents_by_state.csv"),
    index=False
)

top_states_pd = top_10_states.toPandas()

top_states_pd.to_csv(
    os.path.join(geo_output_path, "top_10_states.csv"),
    index=False
)


# Top 10 states graph
plt.figure(figsize=(10, 6))

plt.bar(
    top_states_pd["State"],
    top_states_pd["count"]
)

plt.xlabel("State")
plt.ylabel("Number of Accidents")
plt.title("Top 10 US States by Number of Traffic Accidents")

plt.tight_layout()

plt.savefig(
    os.path.join(geo_output_path, "top_10_states.png"),
    dpi=300
)

plt.close()


# ------------------------------------------------------------
# 11.2 SAVE CITY RESULTS
# ------------------------------------------------------------

city_pd = city_accidents.toPandas()

top_10_cities = city_accidents.limit(10)
top_cities_pd = top_10_cities.toPandas()

city_pd.to_csv(
    os.path.join(geo_output_path, "accidents_by_city.csv"),
    index=False
)


top_cities_pd.to_csv(
    os.path.join(geo_output_path, "top_10_cities.csv"),
    index=False
)


# Create labels such as Miami, FL
top_cities_pd["Location"] = (
    top_cities_pd["City"] + ", " + top_cities_pd["State"]
)


plt.figure(figsize=(11, 6))

plt.bar(
    top_cities_pd["Location"],
    top_cities_pd["count"]
)

plt.xlabel("City")
plt.ylabel("Number of Accidents")
plt.title("Top 10 US Cities by Number of Traffic Accidents")

plt.xticks(rotation=45, ha="right")

plt.tight_layout()

plt.savefig(
    os.path.join(geo_output_path, "top_10_cities.png"),
    dpi=300
)

plt.close()


print("\nGeographic result files and graphs saved successfully.")

# ============================================================
# 12. SEVERITY ANALYSIS
# ============================================================

print("\n==================================================")
print("SEVERITY ANALYSIS")
print("==================================================")


# ------------------------------------------------------------
# 12.1 OVERALL SEVERITY DISTRIBUTION
# ------------------------------------------------------------

start_time = time.perf_counter()

severity_accidents = (
    df_clean
    .groupBy("Severity")
    .count()
    .orderBy("Severity")
)

# Force Spark to execute the operation
severity_accidents.collect()

spark_severity_time = time.perf_counter() - start_time

print("\nAccidents by Severity Level:")
severity_accidents.show()

print(
    f"Spark severity analysis execution time: "
    f"{spark_severity_time:.6f} seconds"
)


# ------------------------------------------------------------
# 12.2 SEVERITY BY YEAR
# ------------------------------------------------------------

severity_by_year = (
    df_clean
    .groupBy("Year", "Severity")
    .count()
    .orderBy("Year", "Severity")
)

print("\nSeverity Distribution by Year:")
severity_by_year.show(30)

# ============================================================
# 13. SAVE SEVERITY RESULTS AND CREATE GRAPHS
# ============================================================

severity_output_path = r"..\outputs\severity"

os.makedirs(severity_output_path, exist_ok=True)


# Overall severity distribution
severity_pd = severity_accidents.toPandas()

severity_pd.to_csv(
    os.path.join(severity_output_path, "severity_distribution.csv"),
    index=False
)

plt.figure(figsize=(8, 5))

plt.bar(
    severity_pd["Severity"].astype(str),
    severity_pd["count"]
)

plt.xlabel("Severity Level")
plt.ylabel("Number of Accidents")
plt.title("Distribution of Traffic Accident Severity")

plt.tight_layout()

plt.savefig(
    os.path.join(
        severity_output_path,
        "severity_distribution.png"
    ),
    dpi=300
)

plt.close()


# Severity by year
severity_year_pd = severity_by_year.toPandas()

severity_year_pd.to_csv(
    os.path.join(
        severity_output_path,
        "severity_by_year.csv"
    ),
    index=False
)


print("\nSeverity result files and graph saved successfully.")

# ============================================================
# 14. WEATHER AND ENVIRONMENT ANALYSIS
# ============================================================

print("\n==================================================")
print("WEATHER AND ENVIRONMENT ANALYSIS")
print("==================================================")


# ------------------------------------------------------------
# 14.1 TOP WEATHER CONDITIONS
# ------------------------------------------------------------

weather_data = df_clean.filter(
    col("Weather_Condition").isNotNull()
)

weather_conditions = (
    weather_data
    .groupBy("Weather_Condition")
    .count()
    .orderBy(col("count").desc())
)

print("\nTop Weather Conditions:")
weather_conditions.show(15)


# ------------------------------------------------------------
# 14.2 SEVERITY BY WEATHER CONDITION
# ------------------------------------------------------------

severity_weather = (
    weather_data
    .groupBy("Weather_Condition", "Severity")
    .count()
    .orderBy(col("count").desc())
)

print("\nSeverity by Weather Condition:")
severity_weather.show(30)


# ------------------------------------------------------------
# 14.3 DAY VS NIGHT ACCIDENTS
# ------------------------------------------------------------

day_night_data = df_clean.filter(
    col("Sunrise_Sunset").isNotNull()
)

day_night_accidents = (
    day_night_data
    .groupBy("Sunrise_Sunset")
    .count()
    .orderBy(col("count").desc())
)

print("\nAccidents by Day/Night:")
day_night_accidents.show()


# ------------------------------------------------------------
# 14.4 AVERAGE VISIBILITY BY SEVERITY
# ------------------------------------------------------------

visibility_data = df_clean.filter(
    col("Visibility(mi)").isNotNull()
)

visibility_severity = (
    visibility_data
    .groupBy("Severity")
    .agg({"Visibility(mi)": "avg"})
    .orderBy("Severity")
)

print("\nAverage Visibility by Severity:")
visibility_severity.show()

# ============================================================
# 15. SAVE WEATHER RESULTS AND CREATE GRAPHS
# ============================================================

weather_output_path = r"..\outputs\weather"

os.makedirs(weather_output_path, exist_ok=True)


# Top weather conditions
top_weather = weather_conditions.limit(10)
weather_pd = top_weather.toPandas()

weather_pd.to_csv(
    os.path.join(
        weather_output_path,
        "top_weather_conditions.csv"
    ),
    index=False
)

plt.figure(figsize=(11, 6))

plt.bar(
    weather_pd["Weather_Condition"],
    weather_pd["count"]
)

plt.xlabel("Weather Condition")
plt.ylabel("Number of Accidents")
plt.title("Top Weather Conditions During Traffic Accidents")

plt.xticks(rotation=45, ha="right")

plt.tight_layout()

plt.savefig(
    os.path.join(
        weather_output_path,
        "top_weather_conditions.png"
    ),
    dpi=300
)

plt.close()


# Day vs night
day_night_pd = day_night_accidents.toPandas()

day_night_pd.to_csv(
    os.path.join(
        weather_output_path,
        "day_vs_night.csv"
    ),
    index=False
)

plt.figure(figsize=(7, 5))

plt.bar(
    day_night_pd["Sunrise_Sunset"],
    day_night_pd["count"]
)

plt.xlabel("Time of Day")
plt.ylabel("Number of Accidents")
plt.title("Traffic Accidents During Day and Night")

plt.tight_layout()

plt.savefig(
    os.path.join(
        weather_output_path,
        "day_vs_night.png"
    ),
    dpi=300
)

plt.close()


# Severity by weather condition
severity_weather_pd = severity_weather.toPandas()

severity_weather_pd.to_csv(
    os.path.join(
        weather_output_path,
        "severity_by_weather.csv"
    ),
    index=False
)


# Visibility by severity
visibility_pd = visibility_severity.toPandas()

visibility_pd.to_csv(
    os.path.join(
        weather_output_path,
        "visibility_by_severity.csv"
    ),
    index=False
)


print("\nWeather and environmental result files saved successfully.")

# ============================================================
# 16. STOP SPARK
# ============================================================

spark.stop()