import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
 * Dataset ingestion utilities for loading CSV/TSV files into Spark DataFrames.
 * 
 * This object provides pure functions for reading structured data files with
 * automatic schema inference, validation, and debugging capabilities.
 * All functions include comprehensive logging and error handling.
 */
object DatasetIngestion {

  /**
   * Schema definition for movies dataset.
   * Expected columns: movieId (integer), title (string), genres (string)
   */
  private val moviesSchema = StructType(Array(
    StructField("movieId", IntegerType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("genres", StringType, nullable = true)
  ))

  /**
   * Schema definition for ratings dataset.
   * Expected columns: userId (integer), movieId (integer), rating (double), timestamp (long)
   */
  private val ratingsSchema = StructType(Array(
    StructField("userId", IntegerType, nullable = false),
    StructField("movieId", IntegerType, nullable = false),
    StructField("rating", DoubleType, nullable = false),
    StructField("timestamp", LongType, nullable = false)
  ))

  /**
   * Loads movies dataset from CSV file.
   * 
   * This function reads a CSV file containing movie information with predefined schema.
   * Includes comprehensive debugging output including path validation, schema information,
   * sample data preview, and record count statistics.
   * 
   * @param spark The SparkSession to use for data loading
   * @param path The file path to the movies CSV file
   * @return DataFrame containing movies data with columns: movieId, title, genres
   */
  def loadMovies(spark: SparkSession, path: String): DataFrame = {
    println(s"DEBUG: Loading movies dataset from path: $path")
    
    try {
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(moviesSchema)
        .csv(path)

      println("DEBUG: Movies schema:")
      df.printSchema()

      val count = df.count()
      println(s"DEBUG: Movies dataset loaded successfully. Record count: $count")

      if (count > 0) {
        println("DEBUG: Sample movies data (first 5 rows):")
        df.show(5, truncate = false)
      } else {
        println("DEBUG: WARNING - Movies dataset is empty!")
      }

      // Validate required columns
      val expectedColumns = Set("movieId", "title", "genres")
      val actualColumns = df.columns.toSet
      if (!expectedColumns.subsetOf(actualColumns)) {
        val missingColumns = expectedColumns -- actualColumns
        println(s"DEBUG: WARNING - Missing expected columns: ${missingColumns.mkString(", ")}")
      }

      df
    } catch {
      case e: Exception =>
        println(s"DEBUG: ERROR loading movies dataset: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Loads ratings dataset from CSV file.
   * 
   * This function reads a CSV file containing user rating information with predefined schema.
   * Includes comprehensive debugging output including path validation, schema information,
   * sample data preview, and record count statistics.
   * 
   * @param spark The SparkSession to use for data loading
   * @param path The file path to the ratings CSV file
   * @return DataFrame containing ratings data with columns: userId, movieId, rating, timestamp
   */
  def loadRatings(spark: SparkSession, path: String): DataFrame = {
    println(s"DEBUG: Loading ratings dataset from path: $path")
    
    try {
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(ratingsSchema)
        .csv(path)

      println("DEBUG: Ratings schema:")
      df.printSchema()

      val count = df.count()
      println(s"DEBUG: Ratings dataset loaded successfully. Record count: $count")

      if (count > 0) {
        println("DEBUG: Sample ratings data (first 5 rows):")
        df.show(5, truncate = false)

        // Skip expensive statistics for large datasets to improve performance
        if (count < 100000) {
          println("DEBUG: Rating statistics:")
          df.select("rating").describe().show()
        } else {
          println("DEBUG: Skipping rating statistics for large dataset (performance optimization)")
        }
      } else {
        println("DEBUG: WARNING - Ratings dataset is empty!")
      }

      // Validate required columns
      val expectedColumns = Set("userId", "movieId", "rating", "timestamp")
      val actualColumns = df.columns.toSet
      if (!expectedColumns.subsetOf(actualColumns)) {
        val missingColumns = expectedColumns -- actualColumns
        println(s"DEBUG: WARNING - Missing expected columns: ${missingColumns.mkString(", ")}")
      }

      df
    } catch {
      case e: Exception =>
        println(s"DEBUG: ERROR loading ratings dataset: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Generic CSV loader with customizable options and schema inference.
   * 
   * This utility function can load any CSV file with flexible configuration options.
   * Useful for loading additional datasets or custom file formats.
   * 
   * @param spark The SparkSession to use for data loading
   * @param path The file path to the CSV file
   * @param header Whether the file has a header row (default: true)
   * @param delimiter The field delimiter (default: comma)
   * @param inferSchema Whether to automatically infer column types (default: true)
   * @return DataFrame containing the loaded data
   */
  def loadCSV(spark: SparkSession, 
             path: String, 
             header: Boolean = true, 
             delimiter: String = ",", 
             inferSchema: Boolean = true): DataFrame = {
    println(s"DEBUG: Loading generic CSV from path: $path")
    println(s"DEBUG: Options - header: $header, delimiter: '$delimiter', inferSchema: $inferSchema")
    
    val df = spark.read
      .option("header", header.toString)
      .option("delimiter", delimiter)
      .option("inferSchema", inferSchema.toString)
      .csv(path)

    println("DEBUG: Generic CSV schema:")
    df.printSchema()

    val count = df.count()
    println(s"DEBUG: Generic CSV loaded. Record count: $count")

    if (count > 0) {
      println("DEBUG: Sample data (first 5 rows):")
      df.show(5)
    }

    df
  }
}