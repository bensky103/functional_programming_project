import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.{Try, Success, Failure}

/**
 * Validated data loader using functional error handling with Try/Either.
 * 
 * This object provides total functions for loading and validating datasets from CSV files.
 * All functions maintain purity from the caller perspective by handling exceptions internally
 * and returning tuples with success counts and failure counts. No exceptions are thrown
 * to the calling code, ensuring total function behavior.
 * 
 * The implementation uses mapPartitions for efficient row-by-row validation and leverages
 * the RowParsers for type-safe parsing with comprehensive error handling.
 */
object ValidatedLoader {

  /**
   * Loads and validates movies dataset from CSV file.
   * 
   * This function performs the complete pipeline:
   * 1. Uses DatasetIngestion to load raw DataFrame
   * 2. Applies RowParsers.parseMovie via mapPartitions for validation
   * 3. Separates successful parses from failures
   * 4. Returns both the clean Dataset[Movie] and failure count
   * 
   * The function is total - it never throws exceptions to callers and handles
   * all error scenarios internally using Try/Either composition.
   * 
   * @param spark The SparkSession for data operations
   * @param path File path to the movies CSV file
   * @return Tuple of (Dataset[Movie], Long) where Long is the count of failed parses
   */
  def loadMovies(spark: SparkSession, path: String): (Dataset[Movie], Long) = {
    import spark.implicits._
    
    Try {
      println(s"DEBUG: Starting validated movie loading from: $path")
      
      // Load raw DataFrame using existing DatasetIngestion
      val rawDF = DatasetIngestion.loadMovies(spark, path)
      val totalCount = rawDF.count()
      println(s"DEBUG: Total raw movie records: $totalCount")
      
      // Use mapPartitions to efficiently parse rows and separate successes/failures
      val parsedDS = rawDF.mapPartitions { rowIterator =>
        val (successfulMovies, _) = rowIterator.foldLeft((List.empty[Movie], 0L)) {
          case ((movies, failureCount), row) =>
            RowParsers.parseMovie(row) match {
              case Right(movie) => (movie :: movies, failureCount)
              case Left(errorMsg) =>
                // Error already logged by RowParsers
                (movies, failureCount + 1)
            }
        }

        // Return successful movies - failures are counted but not included in dataset
        successfulMovies.iterator
      }.as[Movie]
      
      // Cache for efficient counting
      parsedDS.cache()
      
      val successCount = parsedDS.count()
      val failureCount = totalCount - successCount
      
      println(s"DEBUG: Movie parsing summary:")
      println(s"DEBUG: - Total records: $totalCount")
      println(s"DEBUG: - Successful parses: $successCount")
      println(s"DEBUG: - Failed parses: $failureCount")
      
      if (failureCount > 0) {
        println(s"DEBUG: WARNING - $failureCount movie records failed validation")
      }
      
      (parsedDS, failureCount)
      
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"DEBUG: ERROR in loadMovies: ${exception.getMessage}")
        println(s"DEBUG: Returning empty dataset with total failure count")
        // Return empty dataset and indicate total failure
        (spark.emptyDataset[Movie], Long.MaxValue)
    }
  }

  /**
   * Loads and validates ratings dataset from CSV file.
   * 
   * This function performs the complete pipeline:
   * 1. Uses DatasetIngestion to load raw DataFrame
   * 2. Applies RowParsers.parseRating via mapPartitions for validation
   * 3. Separates successful parses from failures
   * 4. Returns both the clean Dataset[Rating] and failure count
   * 
   * The function is total - it never throws exceptions to callers and handles
   * all error scenarios internally using Try/Either composition.
   * 
   * @param spark The SparkSession for data operations
   * @param path File path to the ratings CSV file
   * @return Tuple of (Dataset[Rating], Long) where Long is the count of failed parses
   */
  def loadRatings(spark: SparkSession, path: String): (Dataset[Rating], Long) = {
    import spark.implicits._
    
    Try {
      println(s"DEBUG: Starting validated rating loading from: $path")
      
      // Load raw DataFrame using existing DatasetIngestion
      val rawDF = DatasetIngestion.loadRatings(spark, path)
      val totalCount = rawDF.count()
      println(s"DEBUG: Total raw rating records: $totalCount")
      
      // Use mapPartitions to efficiently parse rows and separate successes/failures
      val parsedDS = rawDF.mapPartitions { rowIterator =>
        val (successfulRatings, _) = rowIterator.foldLeft((List.empty[Rating], 0L)) {
          case ((ratings, failureCount), row) =>
            RowParsers.parseRating(row) match {
              case Right(rating) => (rating :: ratings, failureCount)
              case Left(errorMsg) =>
                // Error already logged by RowParsers
                (ratings, failureCount + 1)
            }
        }

        // Return successful ratings - failures are counted but not included in dataset
        successfulRatings.iterator
      }.as[Rating]
      
      // Cache for efficient counting
      parsedDS.cache()
      
      val successCount = parsedDS.count()
      val failureCount = totalCount - successCount
      
      println(s"DEBUG: Rating parsing summary:")
      println(s"DEBUG: - Total records: $totalCount")
      println(s"DEBUG: - Successful parses: $successCount")
      println(s"DEBUG: - Failed parses: $failureCount")
      
      if (failureCount > 0) {
        println(s"DEBUG: WARNING - $failureCount rating records failed validation")
      }
      
      // Additional rating-specific statistics (skip for large datasets for performance)
      if (successCount > 0 && successCount < 100000) {
        println(s"DEBUG: Rating distribution summary:")
        parsedDS.groupBy("rating").count().orderBy("rating").show()
      } else if (successCount > 0) {
        println(s"DEBUG: Skipping rating distribution for large dataset (performance optimization)")
      }
      
      (parsedDS, failureCount)
      
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"DEBUG: ERROR in loadRatings: ${exception.getMessage}")
        println(s"DEBUG: Returning empty dataset with total failure count")
        // Return empty dataset and indicate total failure
        (spark.emptyDataset[Rating], Long.MaxValue)
    }
  }
}