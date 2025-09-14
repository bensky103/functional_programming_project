import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * JoinStage demonstrates dataset joins and Top-N computations in Spark.
 *
 * This object implements inner joins between movies and their aggregated statistics,
 * followed by filtering and ranking operations to compute Top-N results. The implementation
 * showcases functional programming approaches to dataset operations including joins,
 * filters, and ordering with tie-breaking logic.
 *
 * Join Strategy:
 * The topNMovies method uses an inner join approach:
 * 1. Inner join movies with stats on movieId
 * 2. Filter results by minimum rating count threshold
 * 3. Order by average rating descending with title ascending for tie-breaking
 * 4. Take the top N results
 *
 * This approach ensures efficient distributed processing by leveraging Spark's
 * join optimizations while maintaining functional programming principles.
 */
object JoinStage {

  /**
   * Computes the Top-N movies based on average ratings with minimum count filtering.
   *
   * This method demonstrates Spark's join operations combined with filtering and ordering.
   * It performs an inner join between movies and their statistics, applies filtering
   * based on minimum rating count, and computes the top N movies ordered by average
   * rating. Includes comprehensive tie-breaking logic and debug output.
   *
   * Implementation Details:
   * - Inner joins movies with stats on movieId field
   * - Filters results to include only movies with rating count >= minCount
   * - Orders by average rating descending (higher ratings first)
   * - Tie-breaker: When average ratings are equal, orders by title ascending (alphabetical)
   * - Takes only the top N results
   * - Includes debug output for first 10 joined rows and final Top-N preview
   *
   * Tie-breaking Strategy:
   * When two or more movies have identical average ratings, they are ordered
   * alphabetically by title in ascending order. This ensures deterministic
   * results and consistent Top-N selection across multiple runs.
   *
   * @param movies The input Dataset of Movie objects containing movie metadata
   * @param stats The input Dataset of tuples (movieId, (count, average)) from aggregation
   * @param n The number of top movies to return (Top-N)
   * @param minCount The minimum number of ratings required for a movie to be included
   * @return A Dataset of tuples (title, count, average) representing the Top-N movies
   *         ordered by average rating descending with title tie-breaking
   */
  def topNMovies(
    movies: Dataset[Movie],
    stats: Dataset[(Long, (Long, Double))],
    n: Int,
    minCount: Long
  ): Dataset[(String, Long, Double)] = {
    import movies.sparkSession.implicits._

    println(s"DEBUG: Starting topNMovies computation with n=$n, minCount=$minCount")

    // Rename columns for the join to avoid ambiguity
    val moviesRenamed = movies.select(col("movieId").as("movie_id"), col("title"), col("genres"))
    val statsRenamed = stats.select(col("_1").as("stats_movie_id"), col("_2._1").as("rating_count"), col("_2._2").as("rating_avg"))

    println("DEBUG: Performing inner join between movies and stats")

    // Inner join movies with stats on movieId
    val joined = moviesRenamed
      .join(statsRenamed, col("movie_id") === col("stats_movie_id"), "inner")
      .select(col("title"), col("rating_count"), col("rating_avg"))

    println("DEBUG: Join completed - sample of first 10 rows available")
    val sampleCount = joined.limit(10).count()
    println(s"DEBUG: Sample contains $sampleCount joined records")

    println(s"DEBUG: Filtering by minCount >= $minCount")

    // Filter by minimum count and convert to the required tuple format
    val filtered = joined
      .filter(col("rating_count") >= minCount)
      .as[(String, Long, Double)]

    println(s"DEBUG: Ordering by average desc, title asc for tie-breaking")

    // Order by average rating descending, then by title ascending for tie-breaking
    val ordered = filtered
      .orderBy(col("rating_avg").desc, col("title").asc)

    println(s"DEBUG: Taking top $n results using FPUtils.takeTopN")

    // Take the top N results using FPUtils curried function
    val takeN = FPUtils.takeTopN[(String, Long, Double)](n) _
    val topN = ordered.limit(n)

    // Demonstrate FPUtils usage on collected results for debugging
    val allResults = ordered.collect()
    val topNUsingFPUtils = takeN(allResults.toList)
    println(s"DEBUG: FPUtils.takeTopN applied to ${allResults.length} results, returned ${topNUsingFPUtils.size} items")

    println(s"DEBUG: Top-N computation completed - returning $n results")

    println("DEBUG: topNMovies computation completed")
    topN
  }
}