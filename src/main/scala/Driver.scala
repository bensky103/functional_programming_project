import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}

/**
 * End-to-End Driver for the Movie Recommendation Analytics Pipeline
 *
 * This object orchestrates the complete data processing pipeline from raw CSV ingestion
 * through to final Top-N movie recommendations with comprehensive error handling and
 * output generation in multiple formats.
 *
 * == CLI Usage ==
 * {{{
 * Driver --movies data/movies.csv --ratings data/ratings.csv --minRating 3.5 --minCount 50 --topN 10 --output results/
 * }}}
 *
 * == System Properties Usage ==
 * {{{
 * -Dmovies.path=data/movies.csv -Dratings.path=data/ratings.csv -DminRating=3.5 -DminCount=50 -DtopN=10 -Doutput.path=results/
 * }}}
 *
 * == Pipeline Stages ==
 * 1. **Data Loading**: ValidatedLoader with functional error handling
 * 2. **Filtering**: TransformStage1 with closure-based high rating filtering
 * 3. **Aggregation**: TransformStage2 with groupBy/reduce for movie statistics
 * 4. **Joining & Ranking**: JoinStage with Top-N computation and tie-breaking
 * 5. **Output**: CSV and JSON format generation with coalesce(1) for local demo
 *
 * == Output Formats ==
 * - `results/top_movies.csv`: CSV format with headers (title,count,average)
 * - `results/top_movies.json`: JSON Lines format for programmatic consumption
 * - Both files are coalesced to single partitions for local demonstration
 *
 * @author HIT-FP-Spark-Project
 * @version 1.0
 */
object Driver extends App {

  /**
   * Configuration case class for pipeline parameters.
   *
   * @param moviesPath Path to movies CSV file
   * @param ratingsPath Path to ratings CSV file
   * @param minRating Minimum rating threshold for filtering (inclusive)
   * @param minCount Minimum number of ratings per movie for Top-N inclusion
   * @param topN Number of top movies to include in final results
   * @param outputPath Output directory path for results
   */
  case class Config(
    moviesPath: String = "",
    ratingsPath: String = "",
    minRating: Double = 3.0,
    minCount: Long = 10L,
    topN: Int = 10,
    outputPath: String = "C:/Users/Guy Bensky/Desktop/functional programming projecct/HIT-FP-Spark-Project/output/"
  )

  /**
   * Parses command line arguments into a Config object.
   *
   * Supports both CLI arguments and system properties for flexibility:
   * - CLI: --movies, --ratings, --minRating, --minCount, --topN, --output
   * - System Properties: movies.path, ratings.path, minRating, minCount, topN, output.path
   *
   * @param args Command line arguments array
   * @return Configuration object with parsed parameters
   */
  def parseArgs(args: Array[String]): Config = {
    def parseArgsRec(args: List[String], config: Config): Config = args match {
      case "--movies" :: path :: tail => parseArgsRec(tail, config.copy(moviesPath = path))
      case "--ratings" :: path :: tail => parseArgsRec(tail, config.copy(ratingsPath = path))
      case "--minRating" :: rating :: tail => parseArgsRec(tail, config.copy(minRating = rating.toDouble))
      case "--minCount" :: count :: tail => parseArgsRec(tail, config.copy(minCount = count.toLong))
      case "--topN" :: n :: tail => parseArgsRec(tail, config.copy(topN = n.toInt))
      case "--output" :: path :: tail => parseArgsRec(tail, config.copy(outputPath = path))
      case _ :: tail => parseArgsRec(tail, config)
      case Nil => config
    }

    val cliConfig = parseArgsRec(args.toList, Config())

    // Override with system properties if available
    Config(
      moviesPath = sys.props.getOrElse("movies.path", cliConfig.moviesPath),
      ratingsPath = sys.props.getOrElse("ratings.path", cliConfig.ratingsPath),
      minRating = sys.props.get("minRating").map(_.toDouble).getOrElse(cliConfig.minRating),
      minCount = sys.props.get("minCount").map(_.toLong).getOrElse(cliConfig.minCount),
      topN = sys.props.get("topN").map(_.toInt).getOrElse(cliConfig.topN),
      outputPath = sys.props.getOrElse("output.path", cliConfig.outputPath)
    )
  }

  /**
   * Validates the configuration parameters.
   *
   * @param config Configuration to validate
   * @return Either error message or validated config
   */
  def validateConfig(config: Config): Either[String, Config] = {
    val errors = List(
      if (config.moviesPath.isEmpty) Some("Movies path is required") else None,
      if (config.ratingsPath.isEmpty) Some("Ratings path is required") else None,
      if (config.minRating < 0.0 || config.minRating > 5.0) Some("minRating must be between 0.0 and 5.0") else None,
      if (config.minCount < 1) Some("minCount must be >= 1") else None,
      if (config.topN < 1) Some("topN must be >= 1") else None,
      if (config.outputPath.isEmpty) Some("Output path is required") else None
    ).flatten

    if (errors.nonEmpty) {
      Left(s"Configuration errors: ${errors.mkString(", ")}")
    } else {
      Right(config)
    }
  }

  /**
   * Creates output directory if it doesn't exist.
   *
   * @param outputPath Path to create
   */
  def ensureOutputDirectory(outputPath: String): Unit = {
    val dir = new File(outputPath)
    if (!dir.exists()) {
      dir.mkdirs()
      println(s"DEBUG: Created output directory: $outputPath")
    } else {
      println(s"DEBUG: Output directory exists: $outputPath")
    }
  }

  /**
   * Writes the results to both CSV and JSON formats with console preview.
   *
   * Generates output in two formats for different consumption patterns:
   * - CSV format with headers for spreadsheet applications and human readability
   * - JSON Lines format for programmatic consumption and data pipelines
   *
   * == Coalesce Caveat ==
   * Uses coalesce(1) for single-file output suitable for local demonstration and small datasets.
   * This approach collects all data to a single partition, which can cause memory issues and
   * performance bottlenecks on production clusters with large datasets. For production use:
   * - Remove coalesce(1) to maintain natural partitioning
   * - Use repartition() if specific partition count is needed
   * - Consider partitionBy() for columnar partitioning strategies
   *
   * == Console Output ==
   * Includes formatted console preview of Top-N results using ConsolePrettyPrinter utility
   * for aligned tabular display and summary information.
   *
   * @param results Dataset of top movies with (title, count, average) tuples
   * @param outputPath Output directory path
   * @param spark SparkSession for operations
   */
  def writeResults(results: Dataset[(String, Long, Double)], outputPath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    println(s"DEBUG: ===== STAGE 6: OUTPUT GENERATION =====")
    println(s"DEBUG: Writing results to: $outputPath")

    // Create DataFrame with column names for better output formatting
    val resultsDF = results.toDF("title", "count", "average")

    // Try Spark's native file writing first, fallback to manual writing on Windows
    val csvPath = s"$outputPath/top_movies.csv"
    val jsonPath = s"$outputPath/top_movies.json"

    val sparkWriteSuccess = Try {
      println(s"DEBUG: Attempting Spark native file writing...")

      // Clean up any existing directories from failed attempts
      val csvDir = new File(csvPath)
      if (csvDir.exists() && csvDir.isDirectory()) {
        println(s"DEBUG: Removing existing directory: $csvPath")
        csvDir.delete()
      }

      resultsDF
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(csvPath)

      resultsDF
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(jsonPath)

      true
    }.recover {
      case ex: Exception =>
        println(s"DEBUG: Spark native writing failed: ${ex.getMessage}")
        false
    }.getOrElse(false)

    if (sparkWriteSuccess) {
      println(s"DEBUG: Spark native file writing successful")
      println(s"DEBUG: - CSV: $csvPath")
      println(s"DEBUG: - JSON: $jsonPath")
    } else {
      println(s"DEBUG: Using Windows-compatible manual file writing...")
      writeResultsManually(results.collect().toList, csvPath, jsonPath)
    }
  }

  /**
   * Manually writes results to CSV and JSON files using standard Java IO.
   * This method provides Windows-compatible file writing when Spark's native
   * file writing fails due to Hadoop compatibility issues.
   *
   * @param results List of (title, count, average) tuples
   * @param csvPath Path for CSV output file
   * @param jsonPath Path for JSON output file
   */
  def writeResultsManually(results: List[(String, Long, Double)], csvPath: String, jsonPath: String): Unit = {
    Try {
      // Clean up any directories that Spark may have created
      val csvFile_path = new File(csvPath)
      if (csvFile_path.exists() && csvFile_path.isDirectory()) {
        println(s"DEBUG: Removing existing Spark directory: $csvPath")
        deleteDirectory(csvFile_path)
      }

      val jsonFile_path = new File(jsonPath)
      if (jsonFile_path.exists() && jsonFile_path.isDirectory()) {
        println(s"DEBUG: Removing existing Spark directory: $jsonPath")
        deleteDirectory(jsonFile_path)
      }

      // Ensure parent directory exists
      csvFile_path.getParentFile.mkdirs()

      // Write CSV file
      val csvFile = new PrintWriter(new File(csvPath))
      try {
        csvFile.println("title,count,average")
        results.foreach { case (title, count, avg) =>
          csvFile.println(s""""$title",$count,$avg""")
        }
        println(s"DEBUG: Manual CSV writing successful: $csvPath")
      } finally {
        csvFile.close()
      }

      // Write JSON file
      val jsonFile = new PrintWriter(new File(jsonPath))
      try {
        results.foreach { case (title, count, avg) =>
          jsonFile.println(s"""{"title":"$title","count":$count,"average":$avg}""")
        }
        println(s"DEBUG: Manual JSON writing successful: $jsonPath")
      } finally {
        jsonFile.close()
      }

      println(s"DEBUG: Manual file writing completed successfully")
      println(s"DEBUG: - CSV: $csvPath (${results.length} records)")
      println(s"DEBUG: - JSON: $jsonPath (${results.length} records)")

    } match {
      case Success(_) =>
        println(s"DEBUG: All files written successfully using manual approach")
      case Failure(ex) =>
        println(s"DEBUG: ERROR - Manual file writing also failed: ${ex.getMessage}")
        println(s"DEBUG: Results are available in console output above")
    }
  }

  /**
   * Recursively deletes a directory and all its contents.
   * @param directory The directory to delete
   */
  def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      directory.listFiles().foreach { file =>
        if (file.isDirectory()) {
          deleteDirectory(file)
        } else {
          file.delete()
        }
      }
      directory.delete()
    }
  }

  /**
   * Runs the complete end-to-end movie recommendation pipeline.
   *
   * @param config Pipeline configuration
   * @return Either error message or success message with output location
   */
  def runPipeline(config: Config): Either[String, String] = {
    implicit val spark: SparkSession = SparkBootstrap.session("MovieRecommendationDriver")

    try {
      println("DEBUG: ===== STARTING END-TO-END MOVIE RECOMMENDATION PIPELINE =====")
      println(s"DEBUG: Configuration:")
      println(s"DEBUG: - Movies: ${config.moviesPath}")
      println(s"DEBUG: - Ratings: ${config.ratingsPath}")
      println(s"DEBUG: - Min Rating: ${config.minRating}")
      println(s"DEBUG: - Min Count: ${config.minCount}")
      println(s"DEBUG: - Top N: ${config.topN}")
      println(s"DEBUG: - Output: ${config.outputPath}")

      // Ensure output directory exists
      ensureOutputDirectory(config.outputPath)

      // Stage 1: Data Loading with Validation
      println(s"DEBUG: ===== STAGE 1: DATA LOADING =====")
      val (moviesDS, movieFailures) = ValidatedLoader.loadMovies(spark, config.moviesPath)
      val movieCount = moviesDS.count()
      println(s"DEBUG: Movies loaded - Success: $movieCount, Failures: $movieFailures")

      val (ratingsDS, ratingFailures) = ValidatedLoader.loadRatings(spark, config.ratingsPath)
      val ratingCount = ratingsDS.count()
      println(s"DEBUG: Ratings loaded - Success: $ratingCount, Failures: $ratingFailures")

      if (movieCount == 0) {
        return Left("No valid movies loaded - pipeline cannot continue")
      }
      if (ratingCount == 0) {
        return Left("No valid ratings loaded - pipeline cannot continue")
      }

      // Stage 2: High Rating Filtering
      println(s"DEBUG: ===== STAGE 2: RATING FILTERING =====")
      val filteredRatings = TransformStage1.filterHighRatings(ratingsDS, config.minRating)
      val filteredCount = filteredRatings.count()
      println(s"DEBUG: Filtered ratings (>= ${config.minRating}) - Count: $filteredCount")

      if (filteredCount == 0) {
        return Left(s"No ratings >= ${config.minRating} found - pipeline cannot continue")
      }

      // Stage 3: Movie Statistics Aggregation
      println(s"DEBUG: ===== STAGE 3: STATISTICS AGGREGATION =====")
      val movieStats = TransformStage2.movieStats(filteredRatings)
      val statsCount = movieStats.count()
      println(s"DEBUG: Movie statistics computed - Count: $statsCount")

      if (statsCount == 0) {
        return Left("No movie statistics computed - pipeline cannot continue")
      }

      // Stage 4: Join and Top-N Computation
      println(s"DEBUG: ===== STAGE 4: JOIN & TOP-N COMPUTATION =====")
      val topMovies = JoinStage.topNMovies(moviesDS, movieStats, config.topN, config.minCount)
      val topCount = topMovies.count()
      println(s"DEBUG: Top-N movies computed - Count: $topCount")

      if (topCount == 0) {
        return Left(s"No movies meet criteria (minCount >= ${config.minCount}) - try lower minCount")
      }

      // Stage 5: Results Preview with Pretty Printing
      println(s"DEBUG: ===== STAGE 5: RESULTS PREVIEW =====")
      val topMoviesList = topMovies.collect().toList
      println(s"DEBUG: Top ${topCount} Movies (meeting minCount >= ${config.minCount}):")

      // Use pretty-printer for formatted console output
      ConsolePrettyPrinter.printTopNMovies(topMoviesList, maxPreviewCount = config.topN)

      // Also provide compact summary for logging
      ConsolePrettyPrinter.printTopNSummary(topMoviesList)

      // Stage 6: Output Generation
      writeResults(topMovies, config.outputPath)

      Right(s"Pipeline completed successfully. Results written to: ${config.outputPath}")

    } catch {
      case ex: Exception =>
        println(s"DEBUG: ERROR in pipeline: ${ex.getMessage}")
        ex.printStackTrace()
        Left(s"Pipeline failed: ${ex.getMessage}")
    } finally {
      spark.stop()
      println("DEBUG: ===== PIPELINE COMPLETED =====")
    }
  }

  /**
   * Main entry point for the Driver application.
   */
  def main(): Unit = {
    println("DEBUG: Movie Recommendation Driver started")

    val config = parseArgs(args)
    println(s"DEBUG: Parsed configuration: $config")

    validateConfig(config) match {
      case Left(error) =>
        println(s"ERROR: $error")
        println()
        println("Usage: Driver --movies <path> --ratings <path> [options]")
        println("Options:")
        println("  --movies <path>      Path to movies CSV file (required)")
        println("  --ratings <path>     Path to ratings CSV file (required)")
        println("  --minRating <double> Minimum rating threshold (default: 3.0)")
        println("  --minCount <long>    Minimum rating count per movie (default: 10)")
        println("  --topN <int>         Number of top movies (default: 10)")
        println("  --output <path>      Output directory (default: output/)")
        println()
        println("Alternative: Use system properties (-Dmovies.path=... etc.)")
        System.exit(1)

      case Right(validConfig) =>
        runPipeline(validConfig) match {
          case Left(error) =>
            println(s"ERROR: $error")
            System.exit(1)
          case Right(message) =>
            println(s"SUCCESS: $message")
            System.exit(0)
        }
    }
  }

  // Execute main logic
  main()
}