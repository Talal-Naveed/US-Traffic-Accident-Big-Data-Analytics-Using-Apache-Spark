# US-Traffic-Accident-Big-Data-Analytics-Using-Apache-Spark

## Project Overview

This project applies **Big Data Analytics** techniques to the US Accidents dataset using **Apache Spark through PySpark.** The original dataset contains **7,728,394 accident records and 46 attributes** covering traffic incidents across the United States from February 2016 to March 2023.

For this analysis, the dataset was filtered to the period from **January 2019 to March 2023**, resulting in **5,706,054 records.** PySpark was used as the primary processing framework to perform large-scale filtering, transformation, aggregation, and analysis. An alternative implementation using **Pandas** was also developed to validate selected results and compare local execution performance.

The analysis covers four main dimensions:

-
  Temporal accident patterns
-
  Geographic accident patterns
-
  Accident severity
-
  Weather and environmental conditions

A Spark versus Pandas performance comparison was also conducted using equivalent aggregation operations.

## Dataset

**Dataset:** US Accidents

**Source:** Kaggle

**Dataset Link:** 
https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

### Original Dataset

-
  Records: **7,728,394**
-
  Attributes: **46**
-
  Coverage: **February 2016 to March 2023**
-
  Geographic coverage: United States

### Dataset Used for Analysis

-
  Selected period: **January 2019 to March 2023**
-
  Filtered records: **5,706,054**
-
  Duplicate IDs identified: **0**

The dataset is not included in the repository due to its large file size. Download US_Accidents_March23.csv from the dataset link above and place it inside the data/ folder before running the analysis.

## Code Files

**load_dataset.py**

Loads the original dataset and verifies its structure, row count, column count, sample records, and schema.

**main_analysis.py**

Contains the primary PySpark implementation, including:

-
  dataset loading
-
  temporal filtering
-
  duplicate checking
-
  missing-value assessment
-
  temporal feature creation
-
  temporal analysis
-
  geographic analysis
-
  severity analysis
-
  weather and environmental analysis
-
  result export
-
  visualisation generation
-
  Spark execution-time measurement

**pandas_comparison.py**

Contains the alternative Pandas implementation used to:

-
  reproduce selected Spark aggregations
-
  validate analytical results
-
  measure Pandas execution times
-
  compare Spark and Pandas performance
-
  generate the performance comparison output

## Technologies Used

-
  Python
-
  Apache Spark
-
  PySpark
-
  Pandas
-
  Matplotlib
-
  PyArrow
-
  Java 17
-
  Visual Studio Code

## Environment Requirements

The project was developed locally using:

-
  **Java:** OpenJDK 17
-
  **PySpark:** 4.2.0
-
  **Pandas:** 2.3.3
-
  **PyArrow:** 25.0.1
-
  **Python:** 3.12

Install the required Python libraries using:

pip install pyspark pandas matplotlib pyarrow

Java must also be installed and configured correctly for PySpark.

## Running the Project

**1. Activate the Virtual Environment**

From PowerShell:

C:\Users\<username>\.venv\Scripts\Activate.ps1

A successfully activated environment will display:
(.venv)

**2. Navigate to the Code Folder**

cd C:\path\to\IST3134_US_Accidents\code

**3. Check the Dataset**

Run:

python load_dataset.py

This verifies that the dataset can be loaded correctly.

**4. Run the PySpark Analysis**

python main_analysis.py

The program filters the dataset to:

2019-01-01 00:00:00 <= Start_Time < 2023-04-01 00:00:00 

Therefore, the analysis covers **1 January 2019 through 31 March 2023.**

**5. Run the Pandas Comparison**

After the Spark analysis has completed, run:

python pandas_comparison.py

This reproduces selected operations using Pandas and generates the Spark versus Pandas performance comparison.

## Data Pre-processing

The original dataset is filtered according to:

df_filtered = df.filter (
    
    (col("Start_Time") >= "2019-01-01 00:00:00") &
    
    (col("Start_Time") < "2023-04-01 00:00:00")

)

This produces **5,706,054 records** for analysis.

Duplicate checking identified **0 duplicate IDs.**

The following temporal features are derived from **Start_Time**:

-
  Year
-
  Month
-
  DayOfWeek
-
  Hour

Missing environmental values are retained in the main dataset and excluded only when the relevant attribute is required for a particular analysis.

## Analysis

### Temporal Analysis

The PySpark implementation analyses accident frequency by:

-
  Year
-
  Month
-
  Day of week
-
  Hour

### Geographic Analysis

Geographic aggregations include:

-
  Accidents by state
-
  Top 10 states
-
  Accidents by city
-
  Top 10 cities

### Severity Analysis

Severity analysis includes:

-
  Overall severity distribution
-
  Severity distribution by year

### Weather and Environmental Analysis

Environmental analysis includes:

-
  Weather-condition distribution
-
  Severity by weather condition
-
  Day and night accident distribution
-
  Average visibility by severity

## Spark vs Pandas Comparison

Three equivalent operations are implemented in both PySpark and Pandas:

**1.** Accidents by year

**2.** Accidents by state

**3.** Severity distribution

Both implementations produced matching analytical results.

| Operation | PySpark | Pandas |
|---|---:|---:|
| Accidents by Year | 11.776 s | 0.127 s |
| Accidents by State | 13.742 s | 0.247 s |
| Severity Distribution | 10.952 s | 0.107 s |


These timings represent execution on the same **local machine.** Spark was executed in local mode rather than on a multi-node cluster.

## Key Outputs

The analysis generates CSV files containing aggregated results and PNG files containing visualisations.

Important outputs include:

**Temporal**

-
  Accidents by year
-
  Accidents by month
-
  Accidents by day of week
-
  Accidents by hour

**Geographic**

-
  Accidents by state
-
  Top 10 states
-
  Accidents by city
-
  Top 10 cities

**Severity**

-
  Severity distribution
-
  Severity by year

**Weather and Environment**

-
  Top weather conditions
-
  Severity by weather condition
-
  Day versus night distribution
-
  Visibility by severity

**Performance**

-
  Spark execution times
-
  Pandas execution times
-
  Spark versus Pandas performance comparison

## Key Results

-
  Filtered dataset: **5,706,054 records**
-
  Highest full-year accident count: **2022 with 1,762,452**
-
  Highest day-of-week count: **Friday with 996,816**
-
  Highest hourly count: **16:00 with 436,931**
-
  Highest state count: **California with 1,291,335**
-
  Highest city count: **Miami, Florida with 163,773**
-
  Dominant severity category: **Severity 2 with 4,849,813 records**
-
  Most frequently recorded weather condition: **Fair**
