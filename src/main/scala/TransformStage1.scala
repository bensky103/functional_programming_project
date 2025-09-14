import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * TransformStage1 demonstrates the use of closures in Spark transformations.
 * 
 * This object implements initial data transformation operations that capture variables
 * in closures, showcasing functional programming concepts in distributed computing.
 * 
 * Closures in Spark:
 * A closure is a function that captures variables from its enclosing scope. In Spark,
 * closures are automatically serialized and sent to worker nodes where transformations
 * are executed. This allows the captured variables (like 'min' and 'maxRating') to be
 * available on remote nodes during distributed processing.
 */
object TransformStage1 {

  /**
   * Filters ratings to include only those above or equal to a minimum threshold.
   * 
   * This method demonstrates closure usage by capturing the 'min' parameter in the
   * filter predicate. The captured variable is serialized and available on all
   * worker nodes during distributed execution.
   * 
   * @param ds The input Dataset of Rating objects to filter
   * @param min The minimum rating threshold (inclusive)
   * @return A Dataset containing only ratings >= min
   */
  def filterHighRatings(ds: Dataset[Rating], min: Double): Dataset[Rating] = {
    // Using FPUtils curried function for predicate building
    val isHighRating = FPUtils.greaterEq(min) _
    println(s"DEBUG: Starting to filter ratings with threshold >= $min")

    ds.filter { rating =>
      val isHigh = isHighRating(rating.rating)
      isHigh
    }
  }

  /**
   * Normalizes ratings to a 0-1 scale based on a maximum rating value.
   * 
   * This method demonstrates closure usage by capturing the 'maxRating' parameter
   * in the map transformation. Returns tuples of (movieId, normalizedRating).
   * 
   * @param ds The input Dataset of Rating objects to normalize
   * @param maxRating The maximum possible rating value for normalization
   * @return A Dataset of tuples (movieId, normalizedRating) where normalizedRating is in [0,1]
   */
  def normalizeRatings(ds: Dataset[Rating], maxRating: Double): Dataset[(Long, Double)] = {
    import ds.sparkSession.implicits._

    println(s"DEBUG: Starting to normalize ratings with maxRating: $maxRating")

    ds.map { rating =>
      // The 'maxRating' variable is captured in this closure
      val normalized = rating.rating / maxRating
      (rating.movieId, normalized)
    }
  }
}