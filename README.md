# Steam Games Dataset Analysis - Milestone 2

## Dataset Description

**Dataset Name:** Steam Games Dataset  
**Source Location:** `https://www.kaggle.com/datasets/fronkongames/steam-games-dataset
`  
**Format:** CSV (Comma-Separated Values)  
**Total Records:** 125,855 games  
**Original Columns:** 39 fields  
**Final Schema Columns:** 9 fields (after schema optimization)

This dataset contains comprehensive information about games available on the Steam platform, including game metadata, pricing information, ownership estimates, user engagement metrics, release dates, and descriptive content.

### Dataset Source
The Steam games dataset appears to be a comprehensive collection of Steam platform game metadata, likely aggregated from Steam's public API or web scraping. While the specific source URL is not provided in the notebook, similar datasets are commonly found on:
- Kaggle (Steam Games Dataset)
- Steam API documentation
- Gaming analytics platforms

---

## Project Steps Performed

### Step 1: Initial Data Loading with Infer Schema
**Code:**
```python
steam_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .option("mode", "PERMISSIVE")\
    .load("/Volumes/workspace/default/steam")
```

**Purpose:** Load the CSV file with automatic schema inference to explore the data structure.

**Results:**
- Successfully loaded 125,855 records
- Detected 39 columns automatically
- Used PERMISSIVE mode to handle malformed records gracefully

### Step 2: Schema Exploration
**Code:**
```python
steam_df.columns
steam_df.count()
steam_df.printSchema()
print(len(steam_df.columns))
```

**Purpose:** Understand the dataset structure and dimensions.

**Key Findings:**
- **Total Rows:** 125,855
- **Total Columns:** 39
- **Column Names Include:** AppID, Name, Release date, Estimated owners, Peak CCU, Required age, Price, Website, Support email, Reviews, Metacritic score, User score, Developers, Publishers, Categories, Genres, Tags, and many more

### Step 3: Corrupted Record Detection
**Code:**
```python
spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("mode", "PERMISSIVE")\
    .option("columnNameOfCorruptRecord", "_corrupt_record")\
    .load("/Volumes/workspace/default/steam").display()

if "_corrupt_record" in steam_df.columns:
    steam_df.filter(col("_corrupt_record").isNotNull()).show(truncate=False)
else:
    print("No corrupted records found.")
```

**Purpose:** Identify and examine any malformed or corrupted records in the dataset.

**Result:** No corrupted records were found (`_corrupt_record` column was not present).

### Step 4: Define Custom Schema
**Code:**
```python
schema = StructType([
    StructField("AppID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Release date", StringType(), True),
    StructField("Estimated owners", StringType(), True),
    StructField("Peak CCU", IntegerType(), True),
    StructField("Required age", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("DiscountDLC count", IntegerType(), True),
    StructField("About the game", StringType(), True)
])

steam_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("mode", "PERMISSIVE")\
    .schema(schema)\
    .load("/Volumes/workspace/default/steam")
```

**Purpose:** Explicitly define schema for better control over data types and focus on key columns.

**Justification:**
- **Performance:** Explicit schema is faster than inferSchema for large datasets
- **Data Quality:** Ensures consistent data types across loads
- **Focus:** Narrows down from 39 to 9 essential columns for analysis
- **Type Safety:** Prevents incorrect type inference (e.g., ensuring Price is Double, not String)

**Final Schema:**
```
root
 |-- AppID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Release date: string (nullable = true)
 |-- Estimated owners: string (nullable = true)
 |-- Peak CCU: integer (nullable = true)
 |-- Required age: integer (nullable = true)
 |-- Price: double (nullable = true)
 |-- DiscountDLC count: integer (nullable = true)
 |-- About the game: string (nullable = true)
```

### Step 5: Column Selection and Aliasing
**Code:**
```python
steam_df.select(
    col("Name").alias("Game_Name"),
    col("Price").alias("Game_Price")
).show()
```

**Purpose:** Demonstrate column selection and renaming for focused analysis.

**Transformation:** Selected only game names and prices with more descriptive aliases.

### Step 6: Filtering Data
**Code:**
```python
steam_df.filter(col("Price") > 0)
```

**Purpose:** Filter out free games to analyze only paid games.

**Business Logic:** Separate paid games from free-to-play titles for pricing analysis.

### Step 7: Adding Literal Columns
**Code:**
```python
from pyspark.sql.functions import lit
steam_df.withColumn("Platform", lit("Steam")).display()
```

**Purpose:** Add a constant column to identify the data source platform.

**Use Case:** Useful when combining data from multiple gaming platforms (Steam, Epic, PlayStation, etc.).

### Step 8: Computed Columns
**Code:**
```python
steam_df.withColumn(
    "Discounted_Price",
    col("Price") * 0.9
).display()
```

**Purpose:** Create a new column with calculated values (10% discount simulation).

**Business Application:** Simulate promotional pricing or discount scenarios.

### Step 9: Column Renaming
**Code:**
```python
steam_df.withColumnRenamed("Name", "Game_Name").display()
```

**Purpose:** Rename columns for better readability and consistency.

### Step 10: Null Value Analysis
**Code:**
```python
from pyspark.sql.functions import col, when, count

steam_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in steam_df.columns
]).display()
```

**Purpose:** Count null values in each column to assess data completeness.

**Results:**
- **AppID:** 0 nulls
- **Name:** 1 null
- **Release date:** 0 nulls
- **Estimated owners:** 0 nulls
- **Peak CCU:** 0 nulls
- **Required age:** 0 nulls
- **Price:** 0 nulls
- **DiscountDLC count:** 0 nulls
- **About the game:** 0 nulls

**Key Finding:** The dataset is highly complete with minimal null values (only 1 null in Name column).

### Step 11: Null Value Handling (Attempted)
**Code:**
```python
steam_df = steam_df.fillna({
    "Website": "Not Available",
    "Support email": "Not Available",
    "Notes": "No Notes"
})
```

**Status:** ❌ **FAILED**

**Error:** `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name 'Support email' cannot be resolved.`

**Reason:** The columns "Website", "Support email", and "Notes" do not exist in the custom schema (which only has 9 columns). These columns were part of the original 39-column dataset but were excluded in the custom schema definition.

**Resolution:** This step should either:
1. Be removed (since these columns don't exist in the current schema), OR
2. The schema should be expanded to include these columns if null handling for them is needed

### Step 12: Data Export (Skipped)
**Code:**
```python
steam_df.write.mode("overwrite").parquet("/Volumes/workspace/default/steam/new_steam_data")
```

**Status:** ⏭️ **SKIPPED** (due to error in previous cell)

**Purpose:** Save the transformed dataset in Parquet format for efficient storage and future analysis.

---

## Key Decisions and Justifications

### 1. Read Mode: PERMISSIVE
**Decision:** Used `mode("PERMISSIVE")` for CSV reading.

**Justification:**
- **Robustness:** Handles malformed records gracefully without failing the entire load
- **Data Preservation:** Keeps corrupted records in a special column (`_corrupt_record`) for investigation
- **Production-Ready:** Suitable for real-world data with potential quality issues
- **Alternative Considered:** DROPMALFORMED would silently discard bad records; FAILFAST would abort on first error

### 2. Schema Strategy: Custom Schema vs InferSchema
**Decision:** Moved from `inferSchema` to explicit schema definition.

**Justification:**
- **Performance:** InferSchema requires two passes over data (expensive for large files); explicit schema requires one pass
- **Consistency:** Guarantees the same data types across multiple loads
- **Column Selection:** Reduced from 39 to 9 columns, focusing on essential fields
- **Type Control:** Ensured Price is Double (not String), avoiding conversion issues later
- **Documentation:** Schema serves as clear documentation of expected data structure

### 3. Column Selection: 9 out of 39 Columns
**Decision:** Selected only 9 core columns for analysis.

**Justification:**
- **Focus:** Concentrated on game identification, pricing, and basic metadata
- **Performance:** Smaller schema reduces memory footprint and speeds up operations
- **Simplicity:** Easier to work with fewer columns for initial analysis
- **Extensibility:** Additional columns can be added later if needed

### 4. Null Handling Strategy
**Decision:** Analyzed null counts but deferred filling nulls.

**Justification:**
- **Data Quality Assessment:** First understand the extent of missing data (Result: minimal nulls)
- **Column Mismatch:** The attempted fillna() failed because target columns weren't in the schema
- **Best Practice:** Don't fill nulls blindly; understand their business meaning first
- **Recommendation:** For the single null in "Name" column, options include:
  - Filter out the one record with null Name
  - Fill with "Unknown Game" or "AppID_[value]"
  - Investigate the source record to understand why Name is missing

---

## Challenges Faced and Resolutions

### Challenge 1: Column Count Confusion
**Issue:** Original dataset had 39 columns, but custom schema defined only 9.

**Resolution:**
- Used inferSchema initially to discover all available columns
- Documented all 39 columns for reference
- Consciously selected 9 most relevant columns for focused analysis
- This is intentional data subsetting, not an error

### Challenge 2: Null Handling for Non-Existent Columns
**Issue:** Attempted to fill nulls in columns (Website, Support email, Notes) that don't exist in the custom schema.

**Error:** `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name 'Support email' cannot be resolved.`

**Root Cause:** The custom schema (Step 4) excluded these columns, but the null-filling logic (Step 11) referenced them.

**Resolution:**
1. **Immediate Fix:** Remove or comment out the fillna() statement for non-existent columns
2. **Long-term Fix:** Either:
   - Expand schema to include Website, Support email, and Notes if they're needed
   - Update the fillna() logic to only target columns that exist in the current schema

**Corrected Code:**
```python
# Only fill nulls for columns that exist in our schema
steam_df = steam_df.fillna({
    "Name": "Unknown Game"
})
```

### Challenge 3: Data Type for "About the game"
**Issue:** "About the game" column initially inferred as Integer, but should be String (contains game descriptions).

**Resolution:**
- Cast the column to String: `col("About the game").cast("string")`
- Updated the custom schema to define it as StringType()
- This ensures text descriptions are stored properly

### Challenge 4: Understanding Data Completeness
**Issue:** Need to assess data quality before proceeding with analysis.

**Resolution:**
- Implemented comprehensive null counting across all columns
- Found only 1 null value (in Name column) out of 125,855 records
- This indicates excellent data quality (99.999% completeness)

---

## Screenshots and Key Outputs

### 1. Dataset Overview
- **Total Records:** 125,855 games
- **Total Columns:** 39 (original) → 9 (custom schema)

### 2. Schema Structure (Custom Schema)
```
root
 |-- AppID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Release date: string (nullable = true)
 |-- Estimated owners: string (nullable = true)
 |-- Peak CCU: integer (nullable = true)
 |-- Required age: integer (nullable = true)
 |-- Price: double (nullable = true)
 |-- DiscountDLC count: integer (nullable = true)
 |-- About the game: string (nullable = true)
```

### 3. Null Value Count Results
| Column | Null Count |
|--------|------------|
| AppID | 0 |
| Name | 1 |
| Release date | 0 |
| Estimated owners | 0 |
| Peak CCU | 0 |
| Required age | 0 |
| Price | 0 |
| DiscountDLC count | 0 |
| About the game | 0 |

**Data Quality:** 99.999% complete (only 1 null value across 125,855 records)

### 4. Sample Transformations Demonstrated
-  Column selection with aliases (Game_Name, Game_Price)
-  Filtering (Price > 0 for paid games)
-  Adding literal columns (Platform = "Steam")
-  Computed columns (Discounted_Price = Price * 0.9)
-  Column renaming (Name → Game_Name)

---

## Recommendations for Next Steps

1. **Fix Cell 21:** Remove references to non-existent columns in fillna() or expand the schema
2. **Handle the Single Null:** Address the one null value in the Name column
3. **Complete Data Export:** Once Cell 21 is fixed, execute the Parquet write operation
4. **Add Data Validation:** Implement checks for price ranges, date formats, and AppID uniqueness
5. **Exploratory Analysis:** Perform statistical analysis on Price, Peak CCU, and Estimated owners
6. **Consider Full Schema:** Evaluate if additional columns (Genres, Tags, Reviews) would be valuable for analysis

---

## Technical Environment

- **Platform:** Databricks
- **Compute:** Serverless CPU
- **Language:** Python (PySpark)
- **Data Format:** CSV (source) → Parquet (target)
- **Storage:** Unity Catalog Volumes (`/Volumes/workspace/default/steam`)

---

## Conclusion

This project successfully demonstrates fundamental PySpark data engineering operations on a large-scale Steam games dataset:
-  Efficient data loading with custom schema
-  Schema exploration and optimization
-  Data quality assessment (null analysis)
-  Various transformations (filtering, selection, computation, renaming)
-  Identified and documented an error in null handling logic
-  Ready for advanced analytics once data export is completed

The dataset shows excellent quality with minimal null values, making it suitable for comprehensive gaming industry analysis, pricing strategies, and trend identification.

---

**Author:** Dhanush D  
**USN:** 4SF24CI051
