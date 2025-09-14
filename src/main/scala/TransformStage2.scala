import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * TransformStage2 demonstrates groupBy/reduce style aggregations in Spark.
 *
 * This object implements per-movie aggregate computations using Spark's groupBy/agg
 * operations to compute statistics such as rating counts and averages. The implementation
 * showcases functional programming approaches to distributed data aggregation.
 *
 * Aggregation Strategy:
 * The movieStats method uses a map-aggregate-reduce pattern:
 * 1. Map each rating to (movieId, (ratingSum, count))
 * 2. Group by movieId and aggregate sums and counts
 * 3. Compute final averages functionally from the aggregated values
 *
 * This approach ensures efficient distributed processing by leveraging Spark's
 * built-in aggregation optimizations while maintaining functional programming principles.
 */
object TransformStage2 {

  /**
   * Computes per-movie statistics including count and average rating.
   *
   * This method demonstrates Spark's groupBy/agg operations for computing aggregates.
   * It maps ratings to intermediate tuples, groups by movie ID, and aggregates to
   * compute both count and average rating per movie. Debug prints are included to
   * show partition-level processing and final aggregate results.
   *
   * Implementation Details:
   * - Maps each rating to (movieId, ratingValue) for aggregation
   * - Groups by movieId using groupBy operation
   * - Aggregates using sum and count functions
   * - Computes average functionally as sum/count
   * - Includes debug output at partition and aggregate levels
   *
   * @param ratings The input Dataset of Rating objects to aggregate
   * @return A Dataset of tuples (movieId, (count, average)) where count is the number
   *         of ratings and average is the mean rating value for each movie
   */
  def movieStats(ratings: Dataset[Rating]): Dataset[(Long, (Long, Double))] = {
    import ratings.sparkSession.implicits._

    println("DEBUG: Starting movieStats aggregation - mapping ratings to (movieId, rating) tuples")

    // Map to (movieId, rating)
    val mappedRatings = ratings.map { rating =>
      (rating.movieId, rating.rating)
    }

    // Group by movieId and aggregate sum and count
    println("DEBUG: Grouping ratings by movieId and computing statistics")
    val aggregated = mappedRatings
      .groupByKey(_._1) // Group by movieId
      .mapGroups { (movieId, ratings) =>
        val ratingsList = ratings.map(_._2).toList
        val sum = ratingsList.sum
        val count = ratingsList.length.toLong

        // Compute average functionally
        val average = if (count > 0) sum / count else 0.0

        (movieId, (count, average))
      }

    println("DEBUG: Movie statistics aggregation completed")
    aggregated
  }
}