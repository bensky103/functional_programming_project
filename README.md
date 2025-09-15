# HIT-FP-Spark-Project

An Apache Spark analytics project built with Scala for functional programming coursework.

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

To run the analytics pipeline, execute the following command in PowerShell:

```bash
# Windows PowerShell
$env:SBT_OPTS="-Xmx4g"; sbt "runMain Driver --movies data/ml-25m/movies.csv --ratings data/ml-25m/ratings.csv --minRating 4.0 --minCount 1000 --topN 10"
```

This command will:
1.  Start the Spark application.
2.  Load movie and rating data from the `data/ml-25m/` directory.
3.  Filter for movies with a minimum rating of `4.0` and at least `1000` ratings.
4.  Calculate the top `10` movies based on these criteria.
5.  Write the results to the `output/` directory in both CSV and JSON formats.

**Expected Processing Time:**
- With the provided parameters, the job should take approximately 2-5 minutes.

## Verifying the Output

After the run completes, you can find the results in the `output/` directory.

- **CSV results**: `output/top_movies.csv/`
- **JSON results**: `output/top_movies.json/`

You can view the top movies by inspecting the part-files inside these directories.

## Development

### Testing
To run the test suite:
```bash
sbt test
```

### Building the JAR
To build a fat JAR for deployment:
```bash
sbt assembly
```

## Dependencies

- **Apache Spark Core 3.5.1** - Core Spark functionality (provided scope)
- **Apache Spark SQL 3.5.1** - Spark SQL API (provided scope)
- **ScalaTest 3.2.18** - Testing framework (test scope)