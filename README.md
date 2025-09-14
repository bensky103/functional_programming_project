# HIT-FP-Spark-Project

An Apache Spark analytics project built with Scala for functional programming coursework.

## Dataset

This project uses the MovieLens 25M dataset. Download it from: https://grouplens.org/datasets/movielens/25m/

Extract the downloaded files to the `data/ml-25m/` directory in the project root.

## Prerequisites

- **JDK 11** or higher
- **Scala 2.12.19**
- **sbt 1.9+**
- **Apache Spark 3.5.1** (for runtime execution)

## Project Structure

```
HIT-FP-Spark-Project/
├── build.sbt                  # Project build configuration
├── project/
│   └── build.properties       # sbt version configuration
├── src/
│   ├── main/
│   │   ├── scala/             # Main Scala source code
│   │   │   └── Main.scala     # Application entry point
│   │   └── resources/         # Application resources
│   └── test/
│       └── scala/             # Test source code
│           └── MainSpec.scala # Unit tests
├── docs/                      # Project documentation
├── data/                      # Data files for analytics
└── README.md                  # This file
```

## Running the Project

### Development Mode (sbt)

**Option 1: Run specific class directly (recommended)**
```bash
sbt "runMain Driver --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv --minRating 4.0 --minCount 1000 --topN 10"
```

**Option 2: Run with increased memory (for large datasets like ml-25m)**
```bash
# Windows PowerShell
$env:SBT_OPTS="-Xmx4g"; sbt "runMain Driver --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv --minRating 4.0 --minCount 1000 --topN 10"

# Windows Command Prompt
set SBT_OPTS=-Xmx4g && sbt "runMain Driver --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv --minRating 4.0 --minCount 1000 --topN 10"

# Linux/Mac
SBT_OPTS="-Xmx4g" sbt "runMain Driver --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv --minRating 4.0 --minCount 1000 --topN 10"
```

**Option 3: Interactive mode**
```bash
# Start sbt shell (with memory if needed)
$env:SBT_OPTS="-Xmx4g"; sbt

# Then run (will prompt for class selection)
run

# Select option 1 for Driver class
```

**Option 4: Run with input redirection**
```bash
echo "1" | sbt run
```

**Memory Requirements:**
- Small datasets (< 1M records): Default memory (1GB) is sufficient
- Medium datasets (1-10M records): Use `-Xmx2g` (2GB heap)
- Large datasets (10M+ records like ml-25m): Use `-Xmx4g` (4GB heap)

**Expected Processing Times:**
- ml-25m dataset with minCount=1000: 2-5 minutes
- ml-25m dataset with minCount=10000: 30-60 seconds
- Smaller sample datasets: 10-30 seconds

### Quick Development Workflow
```bash
# Run all tests to verify everything works
sbt test

# Generate ScalaDoc documentation
sbt doc

# Build fat JAR for deployment
sbt assembly
```

## 🎯 **PROJECT DEFENSE DEMO COMMANDS**

### **Step 1: Project Verification & Setup**
```bash
# Navigate to project directory
cd HIT-FP-Spark-Project

# Verify project structure
ls -la

# Check build configuration
cat build.sbt

# Check sbt version
cat project/build.properties

# Verify dataset availability (25M records)
ls -la data/ml-25m/
wc -l data/ml-25m/*.csv
```

### **Step 2: Run Comprehensive Test Suite**
```bash
# Run all 129 tests to demonstrate correctness
sbt test

# Expected output: "Tests: succeeded 128, failed 0"
# This demonstrates:
# - Functional programming patterns (pattern matching, closures, monads)
# - Spark DataFrames/Datasets integration
# - Error handling with Try/Either
# - Data validation and parsing
```

### **Step 3: Generate Documentation**
```bash
# Generate ScalaDoc API documentation
sbt doc

# View generated docs (opens in target/scala-2.12/api/)
ls -la target/scala-2.12/api/

# This demonstrates professional documentation with:
# - Class/method documentation
# - Functional programming concepts explained
# - Pipeline architecture overview
```

### **Step 4: Build Deployment Package**
```bash
# Create fat JAR with all dependencies
sbt assembly

# Verify JAR was created (~5.6MB)
ls -lh target/scala-2.12/hit-fp-spark-assembly.jar

# This demonstrates production-ready packaging
```

### **Step 5: Demo Runs (Choose Based on Available Time)**

#### **Quick Demo (2-3 minutes) - Small Sample**
```bash
# Use higher minCount to get manageable results from 25M dataset
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     --add-opens=java.base/java.net=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     -Xmx2g \
     -jar target/scala-2.12/hit-fp-spark-assembly.jar \
     --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv \
     --minRating 4.5 --minCount 10000 --topN 5 --output demo-quick/

# Expected: ~1-2 minutes processing, shows top 5 movies with >10k ratings >4.5 stars
```

#### **Medium Demo (5-10 minutes) - Comprehensive Analysis**
```bash
# Full pipeline with detailed filtering
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     --add-opens=java.base/java.net=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     -Xmx2g \
     -jar target/scala-2.12/hit-fp-spark-assembly.jar \
     --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv \
     --minRating 4.0 --minCount 5000 --topN 10 --output demo-medium/

# Expected: ~3-5 minutes processing, shows top 10 high-quality popular movies
```

#### **Full Demo (10+ minutes) - Production Scale**
```bash
# Process entire dataset with lower thresholds
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     --add-opens=java.base/java.net=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     -Xmx2g \
     -jar target/scala-2.12/hit-fp-spark-assembly.jar \
     --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv \
     --minRating 3.5 --minCount 1000 --topN 20 --output demo-full/

# Expected: ~5-10 minutes processing, comprehensive movie recommendations
```

### **Step 6: Demonstrate Results Analysis**
```bash
# Show generated outputs
ls -la demo-quick/  # (or demo-medium/, demo-full/)

# Display CSV results (formatted table)
head -20 demo-quick/top_movies.csv/part-00000*.csv

# Display JSON results (programmatic format)
head -10 demo-quick/top_movies.json/part-00000*.json

# Show file sizes to demonstrate data processing scale
wc -l demo-quick/top_movies.csv/part-00000*.csv
```

### **Step 7: Code Walkthrough (If Time Permits)**
```bash
# Show main functional programming components
cat src/main/scala/Driver.scala | head -50        # Main driver
cat src/main/scala/RowParsers.scala | head -30    # Pattern matching
cat src/main/scala/TransformStage1.scala          # Closures & currying
cat src/main/scala/ValidatedLoader.scala | head -40 # Error handling
```

## **Demo Output Interpretation**

### **What to Expect in Output:**
```
DEBUG: ===== STARTING END-TO-END MOVIE RECOMMENDATION PIPELINE =====
DEBUG: Configuration:
DEBUG: - Movies: data/ml-25m/movies.csv
DEBUG: - Ratings: data/ml-25m/ratings.csv
DEBUG: - Min Rating: 4.5
DEBUG: - Min Count: 10000
DEBUG: - Top N: 5

DEBUG: Movies loaded - Success: 62423, Failures: 0
DEBUG: Ratings loaded - Success: 25000095, Failures: 0
DEBUG: Filtered ratings (>= 4.5) - Count: 4156934
DEBUG: Movie statistics computed - Count: 39143
DEBUG: Top-N movies computed - Count: 5

Rank | Movie Title                           | Count  | Average
----------------------------------------------------------------
1.   | The Shawshank Redemption (1994)      | 63,366 |   4.587
2.   | The Godfather (1972)                 | 41,355 |   4.524
3.   | Schindler's List (1993)              | 50,054 |   4.520
4.   | 12 Angry Men (1957)                  | 11,611 |   4.610
5.   | The Godfather: Part II (1974)        | 28,881 |   4.517

SUCCESS: Pipeline completed successfully. Results written to: demo-quick/
```

### **Key Points to Highlight:**
1. **Scale**: Processing 25M+ ratings, 62k+ movies in minutes
2. **Functional Programming**: Pattern matching, closures, monads throughout
3. **Performance**: Spark's distributed processing with lazy evaluation
4. **Error Handling**: Graceful handling of malformed data
5. **Output Formats**: Both CSV (human-readable) and JSON (programmatic)

## **Alternative Spark-Submit Commands (If spark-submit Available)**
```bash
# Using spark-submit for cluster-like execution
spark-submit --class Driver --master local[*] \
  --driver-memory 2g --executor-memory 2g \
  target/scala-2.12/hit-fp-spark-assembly.jar \
  --movies data/ml-25m/movies.csv \
  --ratings data/ml-25m/ratings.csv \
  --minRating 4.0 --minCount 5000 --topN 10 \
  --output spark-demo/
```

## Dependencies

- **Apache Spark Core 3.5.1** - Core Spark functionality (provided scope)
- **Apache Spark SQL 3.5.1** - Spark SQL API (provided scope)
- **ScalaTest 3.2.18** - Testing framework (test scope)

## Development Notes

This project is configured for development in VS Code with Metals. The Spark dependencies are marked as "provided" since they will be available in the Spark runtime environment.

## **🚀 QUICK DEFENSE PREPARATION**

### **Pre-Demo Checklist (5 minutes before defense):**
```bash
# 1. Verify everything builds and tests pass
sbt clean test assembly

# 2. Create a quick demo run to verify data access
java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     --add-opens=java.base/java.net=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     -Xmx2g \
     -jar target/scala-2.12/hit-fp-spark-assembly.jar \
     --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv \
     --minRating 4.5 --minCount 20000 --topN 3 --output verify-demo/

# 3. Check outputs were generated
ls -la verify-demo/
head -5 verify-demo/top_movies.csv/part-*.csv
```

### **Defense Talking Points:**
1. **Functional Programming Techniques Used:**
   - Pattern matching with case classes (`Movie`, `Rating`)
   - Closures in Spark transformations (`filterHighRatings`)
   - Monadic error handling with `Try`/`Either`
   - Higher-order functions and currying
   - Immutable data structures throughout

2. **Spark Integration:**
   - DataFrames and Datasets for type safety
   - Lazy evaluation and distributed processing
   - Memory-efficient aggregations and joins

3. **Software Engineering Best Practices:**
   - Comprehensive test suite (129 tests)
   - ScalaDoc documentation
   - Modular architecture with separation of concerns
   - Error handling and data validation

### **If Demo Fails - Backup Plan:**
```bash
# Show test results (proves code correctness)
sbt test | grep -A5 -B5 "succeeded\|failed"

# Show documentation generation
ls -la target/scala-2.12/api/index.html

# Walk through code architecture
find src/main/scala -name "*.scala" -exec echo "=== {} ===" \; -exec head -10 {} \;
```

## Getting Started

1. Clone or extract the project
2. Ensure sbt and JDK 11+ are installed
3. Run `sbt test` to verify the setup
4. Build with `sbt assembly` for production deployment
5. Follow the **PROJECT DEFENSE DEMO COMMANDS** above for demonstrations