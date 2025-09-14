import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, Dataset}

/**
 * Test suite for TransformStage2 aggregation operations.
 *
 * This test class verifies that movieStats method correctly implements
 * groupBy/aggregate operations and produces expected counts and averages
 * with tiny datasets.
 */
class TransformStage2Spec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkBootstrap.session("TransformStage2Test")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  /**
   * Creates a tiny in-memory test dataset for Rating objects with multiple ratings per movie.
   */
  def createTestRatings(): Dataset[Rating] = {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      // Movie 101: 3 ratings (1.0, 2.0, 3.0) -> count=3, avg=2.0
      Rating(1L, 101L, 1.0, 1000L),
      Rating(2L, 101L, 2.0, 1100L),
      Rating(3L, 101L, 3.0, 1200L),

      // Movie 102: 2 ratings (4.0, 5.0) -> count=2, avg=4.5
      Rating(4L, 102L, 4.0, 2000L),
      Rating(5L, 102L, 5.0, 2100L),

      // Movie 103: 1 rating (3.5) -> count=1, avg=3.5
      Rating(6L, 103L, 3.5, 3000L),

      // Movie 104: 4 ratings (2.0, 2.0, 3.0, 5.0) -> count=4, avg=3.0
      Rating(7L, 104L, 2.0, 4000L),
      Rating(8L, 104L, 2.0, 4100L),
      Rating(9L, 104L, 3.0, 4200L),
      Rating(10L, 104L, 5.0, 4300L)
    )

    spark.createDataset(testData)
  }

  /**
   * Creates a minimal test dataset with single ratings per movie.
   */
  def createMinimalTestRatings(): Dataset[Rating] = {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      Rating(1L, 201L, 4.0, 1000L),
      Rating(2L, 202L, 3.0, 2000L)
    )

    spark.createDataset(testData)
  }

  "TransformStage2.movieStats" should "compute correct counts and averages for multiple movies" in {
    val testRatings = createTestRatings()

    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect().toMap

    // Should have stats for 4 different movies
    results.size should equal(4)

    // Movie 101: 3 ratings (1.0, 2.0, 3.0) -> count=3, avg=2.0
    results should contain key 101L
    val (count101, avg101) = results(101L)
    count101 should equal(3L)
    avg101 should equal(2.0 +- 0.001)

    // Movie 102: 2 ratings (4.0, 5.0) -> count=2, avg=4.5
    results should contain key 102L
    val (count102, avg102) = results(102L)
    count102 should equal(2L)
    avg102 should equal(4.5 +- 0.001)

    // Movie 103: 1 rating (3.5) -> count=1, avg=3.5
    results should contain key 103L
    val (count103, avg103) = results(103L)
    count103 should equal(1L)
    avg103 should equal(3.5 +- 0.001)

    // Movie 104: 4 ratings (2.0, 2.0, 3.0, 5.0) -> count=4, avg=3.0
    results should contain key 104L
    val (count104, avg104) = results(104L)
    count104 should equal(4L)
    avg104 should equal(3.0 +- 0.001)
  }

  it should "handle single rating per movie correctly" in {
    val testRatings = createMinimalTestRatings()

    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect().toMap

    results.size should equal(2)

    // Movie 201: 1 rating (4.0) -> count=1, avg=4.0
    val (count201, avg201) = results(201L)
    count201 should equal(1L)
    avg201 should equal(4.0 +- 0.001)

    // Movie 202: 1 rating (3.0) -> count=1, avg=3.0
    val (count202, avg202) = results(202L)
    count202 should equal(1L)
    avg202 should equal(3.0 +- 0.001)
  }

  it should "handle empty dataset correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyRatings = spark.createDataset(Seq.empty[Rating])

    val stats = TransformStage2.movieStats(emptyRatings)
    val results = stats.collect()

    results.length should equal(0)
  }

  it should "correctly aggregate duplicate ratings for same movie" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Create dataset with same rating values for same movie
    val testData = Seq(
      Rating(1L, 301L, 2.0, 1000L),
      Rating(2L, 301L, 2.0, 1100L),
      Rating(3L, 301L, 2.0, 1200L)
    )
    val testRatings = spark.createDataset(testData)

    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect().toMap

    results.size should equal(1)

    // Movie 301: 3 identical ratings (2.0) -> count=3, avg=2.0
    val (count301, avg301) = results(301L)
    count301 should equal(3L)
    avg301 should equal(2.0 +- 0.001)
  }

  it should "handle movies with extreme rating values" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      // Movie with very low ratings
      Rating(1L, 401L, 0.5, 1000L),
      Rating(2L, 401L, 0.5, 1100L),

      // Movie with maximum ratings
      Rating(3L, 402L, 5.0, 2000L),
      Rating(4L, 402L, 5.0, 2100L),

      // Movie with mixed extreme values
      Rating(5L, 403L, 0.5, 3000L),
      Rating(6L, 403L, 5.0, 3100L)
    )
    val testRatings = spark.createDataset(testData)

    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect().toMap

    results.size should equal(3)

    // Movie 401: 2 ratings (0.5, 0.5) -> count=2, avg=0.5
    val (count401, avg401) = results(401L)
    count401 should equal(2L)
    avg401 should equal(0.5 +- 0.001)

    // Movie 402: 2 ratings (5.0, 5.0) -> count=2, avg=5.0
    val (count402, avg402) = results(402L)
    count402 should equal(2L)
    avg402 should equal(5.0 +- 0.001)

    // Movie 403: 2 ratings (0.5, 5.0) -> count=2, avg=2.75
    val (count403, avg403) = results(403L)
    count403 should equal(2L)
    avg403 should equal(2.75 +- 0.001)
  }

  it should "preserve movieId correctly in aggregation" in {
    val testRatings = createTestRatings()

    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect()

    // Extract all movieIds from results
    val movieIds = results.map(_._1).toSet
    val expectedMovieIds = Set(101L, 102L, 103L, 104L)

    movieIds should equal(expectedMovieIds)

    // Verify each movieId appears exactly once
    results.map(_._1) should have size 4
    results.map(_._1).distinct should have size 4
  }

  it should "compute functional averages correctly from sum and count" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Create a dataset where we can easily verify sum/count -> average computation
    val testData = Seq(
      Rating(1L, 501L, 1.0, 1000L),  // sum = 1.0
      Rating(2L, 501L, 2.0, 1100L),  // sum = 3.0
      Rating(3L, 501L, 3.0, 1200L),  // sum = 6.0, count = 3, avg = 2.0
      Rating(4L, 501L, 4.0, 1300L)   // sum = 10.0, count = 4, avg = 2.5
    )
    val testRatings = spark.createDataset(testData)

    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect().toMap

    val (count501, avg501) = results(501L)

    // Verify that average is correctly computed as sum/count
    count501 should equal(4L)
    avg501 should equal(2.5 +- 0.001)  // (1.0 + 2.0 + 3.0 + 4.0) / 4 = 2.5

    // Verify by reconstructing the sum from average and count
    val reconstructedSum = avg501 * count501
    reconstructedSum should equal(10.0 +- 0.001)
  }

  "Integration test" should "demonstrate groupBy/agg aggregation pattern" in {
    val testRatings = createTestRatings()

    // This test verifies the complete aggregation pipeline
    val stats = TransformStage2.movieStats(testRatings)
    val results = stats.collect()

    // Should aggregate all 10 input ratings into 4 movie statistics
    results should have size 4

    // Total count across all movies should equal input rating count
    val totalRatingsCount = results.map(_._2._1).sum
    totalRatingsCount should equal(10L)  // 3 + 2 + 1 + 4 from test data

    // Verify each result has positive count and reasonable average
    results.foreach { case (movieId, (count, average)) =>
      movieId should be > 0L
      count should be > 0L
      average should be >= 0.5  // Minimum rating in test data
      average should be <= 5.0  // Maximum rating in test data
    }

    // Verify specific aggregation results match expected values
    val resultMap = results.toMap
    val movieIds = resultMap.keys.toSet
    movieIds should contain theSameElementsAs Set(101L, 102L, 103L, 104L)
  }

  /**
   * Test empty datasets (0 rows) to verify TransformStage2 handles gracefully.
   * This edge case ensures movieStats function doesn't fail with empty input.
   */
  it should "handle empty ratings dataset gracefully in movieStats" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyRatings = spark.createDataset(Seq.empty[Rating])

    println("DEBUG: Testing movieStats with empty dataset")
    val stats = TransformStage2.movieStats(emptyRatings)
    val results = stats.collect()

    results.length should equal(0)
    println(s"DEBUG: Empty dataset movieStats test - result size: ${results.length}")
  }
}