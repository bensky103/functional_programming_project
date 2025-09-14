import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, Dataset}

/**
 * Test suite for JoinStage join and Top-N operations.
 *
 * This test class verifies that topNMovies method correctly implements
 * inner joins between movies and stats, filtering by minimum count,
 * and ordering with tie-breaking logic using tiny datasets.
 */
class JoinStageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkBootstrap.session("JoinStageTest")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  /**
   * Creates test movies dataset with varied titles for tie-breaking tests.
   */
  def createTestMovies(): Dataset[Movie] = {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      Movie(101L, "Zebra Movie", List("Action")),        // Alphabetically last for tie-breaking
      Movie(102L, "Alpha Movie", List("Drama")),         // Alphabetically first for tie-breaking
      Movie(103L, "Beta Movie", List("Comedy")),         // Middle alphabetically
      Movie(104L, "Charlie Movie", List("Thriller")),    // Another middle option
      Movie(105L, "Delta Movie", List("Sci-Fi")),        // High rating, few ratings
      Movie(106L, "Echo Movie", List("Romance")),        // No stats - should not appear in results
      Movie(107L, "Foxtrot Movie", List("Horror"))       // Low count - for minCount filtering tests
    )

    spark.createDataset(testData)
  }

  /**
   * Creates test stats dataset with specific count/average combinations for testing.
   * Designed to test join correctness, filtering, and tie-breaking scenarios.
   */
  def createTestStats(): Dataset[(Long, (Long, Double))] = {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      // movieId -> (count, average)
      (101L, (10L, 4.5)),  // High count, high rating - should rank high
      (102L, (8L, 4.5)),   // High count, same rating as 101 - tie-breaker test (Alpha < Zebra)
      (103L, (12L, 4.2)),  // Highest count, slightly lower rating
      (104L, (6L, 4.8)),   // Lower count, highest rating
      (105L, (2L, 5.0)),   // Very low count, perfect rating - filtered out with minCount=3
      (107L, (1L, 3.0))    // Very low count, medium rating - filtered out with minCount=3
      // Note: movieId 106 (Echo Movie) has no stats - should not appear in join
    )

    spark.createDataset(testData)
  }

  /**
   * Creates minimal test datasets for basic functionality testing.
   */
  def createMinimalTestData(): (Dataset[Movie], Dataset[(Long, (Long, Double))]) = {
    val sparkSession = spark
    import sparkSession.implicits._

    val movies = spark.createDataset(Seq(
      Movie(201L, "Test Movie A", List("Action")),
      Movie(202L, "Test Movie B", List("Drama"))
    ))

    val stats = spark.createDataset(Seq(
      (201L, (5L, 4.0)),
      (202L, (3L, 3.5))
    ))

    (movies, stats)
  }

  "JoinStage.topNMovies" should "perform correct inner join between movies and stats" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    // Use large N and minCount=1 to see all joined results
    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 1L)
    val resultList = result.collect()

    // Should have 6 results (movies 101,102,103,104,105,107 have stats, 106 doesn't)
    resultList should have size 6

    // Extract titles to verify join correctness
    val resultTitles = resultList.map(_._1).toSet
    val expectedTitles = Set("Zebra Movie", "Alpha Movie", "Beta Movie", "Charlie Movie", "Delta Movie", "Foxtrot Movie")

    resultTitles should equal(expectedTitles)

    // Movie 106 (Echo Movie) should not appear since it has no stats
    resultTitles should not contain "Echo Movie"
  }

  it should "filter by minimum count correctly" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    // Filter with minCount=3 - should exclude movies 105 (count=2) and 107 (count=1)
    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 3L)
    val resultList = result.collect()

    // Should have 4 results (movies 101,102,103,104 have count >= 3)
    resultList should have size 4

    val resultTitles = resultList.map(_._1).toSet
    val expectedTitles = Set("Zebra Movie", "Alpha Movie", "Beta Movie", "Charlie Movie")

    resultTitles should equal(expectedTitles)

    // Movies with low counts should be filtered out
    resultTitles should not contain "Delta Movie"  // count=2
    resultTitles should not contain "Foxtrot Movie"  // count=1

    // Verify all results meet the minimum count requirement
    resultList.foreach { case (_, count, _) =>
      count should be >= 3L
    }
  }

  it should "order by average rating descending" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 1L)
    val resultList = result.collect()

    // Extract averages in result order
    val averages = resultList.map(_._3)

    // Should be in descending order
    averages.sliding(2).foreach { case Array(a, b) =>
      a should be >= b
    }

    // First result should have highest average (Delta Movie with 5.0)
    val firstResult = resultList.head
    firstResult._1 should equal("Delta Movie")
    firstResult._3 should equal(5.0 +- 0.001)
  }

  it should "use title ascending for tie-breaking when averages are equal" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 1L)
    val resultList = result.collect()

    // Find the two movies with rating 4.5 (Zebra Movie and Alpha Movie)
    val tieMovies = resultList.filter(_._3 == 4.5).sortBy(_._1)

    tieMovies should have size 2

    // Alpha Movie should come before Zebra Movie (alphabetical tie-breaking)
    tieMovies(0)._1 should equal("Alpha Movie")
    tieMovies(1)._1 should equal("Zebra Movie")

    // In the overall result, Alpha Movie should appear before Zebra Movie
    val alphaIndex = resultList.indexWhere(_._1 == "Alpha Movie")
    val zebraIndex = resultList.indexWhere(_._1 == "Zebra Movie")

    alphaIndex should be < zebraIndex
  }

  it should "limit results to Top-N correctly" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    // Test with N=3
    val result = JoinStage.topNMovies(movies, stats, n = 3, minCount = 1L)
    val resultList = result.collect()

    resultList should have size 3

    // Should get the top 3 by rating: Delta(5.0), Charlie(4.8), Alpha(4.5)
    val expectedOrder = List("Delta Movie", "Charlie Movie", "Alpha Movie")
    val actualTitles = resultList.map(_._1).toList

    actualTitles should equal(expectedOrder)
  }

  it should "return correct tuple format (title, count, average)" in {
    val (movies, stats) = createMinimalTestData()

    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 1L)
    val resultList = result.collect()

    resultList should have size 2

    // Verify actual values - pattern matching confirms correct tuple structure
    resultList.foreach { case (title, count, average) =>
      title shouldBe a [String]
      title should not be empty
      count should be > 0L
      average should be >= 0.0
      average should be <= 5.0
    }
  }

  it should "handle empty datasets correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyMovies = spark.createDataset(Seq.empty[Movie])
    val emptyStats = spark.createDataset(Seq.empty[(Long, (Long, Double))])

    val result = JoinStage.topNMovies(emptyMovies, emptyStats, n = 5, minCount = 1L)
    val resultList = result.collect()

    resultList should have size 0
  }

  it should "handle no matching joins correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Movies and stats with different movieIds - no join matches
    val movies = spark.createDataset(Seq(
      Movie(301L, "Movie A", List("Action")),
      Movie(302L, "Movie B", List("Drama"))
    ))

    val stats = spark.createDataset(Seq(
      (401L, (5L, 4.0)),
      (402L, (3L, 3.5))
    ))

    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 1L)
    val resultList = result.collect()

    resultList should have size 0
  }

  it should "preserve data accuracy through the pipeline" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    val result = JoinStage.topNMovies(movies, stats, n = 10, minCount = 1L)
    val resultList = result.collect()

    // Convert to map for easier lookup - create map from title to (count, avg)
    val resultMap = resultList.map { case (title, count, avg) => (title, (count, avg)) }.toMap

    // Verify specific movie data is preserved correctly
    resultMap should contain key "Charlie Movie"
    val (charlieCount, charlieAvg) = resultMap("Charlie Movie")
    charlieCount should equal(6L)
    charlieAvg should equal(4.8 +- 0.001)

    resultMap should contain key "Beta Movie"
    val (betaCount, betaAvg) = resultMap("Beta Movie")
    betaCount should equal(12L)
    betaAvg should equal(4.2 +- 0.001)
  }

  "Integration test" should "demonstrate complete join + filter + order + limit pipeline" in {
    val movies = createTestMovies()
    val stats = createTestStats()

    // Complete pipeline test: join, filter minCount>=5, order by avg desc/title asc, top 2
    val result = JoinStage.topNMovies(movies, stats, n = 2, minCount = 5L)
    val resultList = result.collect()

    // Should get exactly 2 results
    resultList should have size 2

    // Only movies with count >= 5: Charlie(4.8,6), Alpha(4.5,8), Zebra(4.5,10), Beta(4.2,12)
    // Top 2 by rating: Charlie(4.8), then Alpha(4.5) due to tie-breaking
    val expectedResults = List(
      ("Charlie Movie", 6L, 4.8),
      ("Alpha Movie", 8L, 4.5)
    )

    resultList.zip(expectedResults).foreach { case ((actualTitle, actualCount, actualAvg), (expectedTitle, expectedCount, expectedAvg)) =>
      actualTitle should equal(expectedTitle)
      actualCount should equal(expectedCount)
      actualAvg should equal(expectedAvg +- 0.001)
    }

    // Verify ordering properties
    resultList(0)._3 should be > resultList(1)._3  // First has higher average
    resultList.foreach { case (_, count, _) =>
      count should be >= 5L  // All meet minimum count requirement
    }
  }

  /**
   * Test Top-N ties to verify stable secondary sort by title.
   * This edge case ensures deterministic ordering when average ratings are equal.
   */
  "JoinStage.topNMovies" should "handle multiple ties with stable secondary sort by title" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Create movies with varied alphabetical titles for tie-breaking
    val tieMovies = spark.createDataset(Seq(
      Movie(501L, "Zebra Tied Movie", List("Action")),        // Last alphabetically
      Movie(502L, "Alpha Tied Movie", List("Drama")),         // First alphabetically
      Movie(503L, "Beta Tied Movie", List("Comedy")),         // Second alphabetically
      Movie(504L, "Charlie Tied Movie", List("Thriller")),    // Third alphabetically
      Movie(505L, "Delta Tied Movie", List("Sci-Fi")),        // Fourth alphabetically
      Movie(506L, "Echo Tied Movie", List("Romance"))         // Fifth alphabetically
    ))

    // Create stats where multiple movies have identical ratings
    val tieStats = spark.createDataset(Seq(
      (501L, (10L, 4.0)),  // Zebra Tied Movie - 4.0 rating (tie)
      (502L, (10L, 4.0)),  // Alpha Tied Movie - 4.0 rating (tie)
      (503L, (10L, 4.0)),  // Beta Tied Movie - 4.0 rating (tie)
      (504L, (10L, 4.5)),  // Charlie Tied Movie - 4.5 rating (higher)
      (505L, (10L, 4.0)),  // Delta Tied Movie - 4.0 rating (tie)
      (506L, (10L, 3.5))   // Echo Tied Movie - 3.5 rating (lower)
    ))

    println("DEBUG: Testing Top-N with multiple ties and stable secondary sort")
    val result = JoinStage.topNMovies(tieMovies, tieStats, n = 6, minCount = 5L)
    val resultList = result.collect()

    resultList should have size 6

    // Expected order: Charlie (4.5), then Alpha, Beta, Delta, Zebra (all 4.0, alphabetical), then Echo (3.5)
    val expectedOrder = List(
      ("Charlie Tied Movie", 10L, 4.5),
      ("Alpha Tied Movie", 10L, 4.0),
      ("Beta Tied Movie", 10L, 4.0),
      ("Delta Tied Movie", 10L, 4.0),
      ("Zebra Tied Movie", 10L, 4.0),
      ("Echo Tied Movie", 10L, 3.5)
    )

    resultList.zip(expectedOrder).foreach { case ((actualTitle, actualCount, actualAvg), (expectedTitle, expectedCount, expectedAvg)) =>
      actualTitle should equal(expectedTitle)
      actualCount should equal(expectedCount)
      actualAvg should equal(expectedAvg +- 0.001)
    }

    // Verify the tied movies (rating 4.0) are in alphabetical order
    val tiedMovies = resultList.filter(_._3 == 4.0).map(_._1)
    val expectedTiedOrder = List("Alpha Tied Movie", "Beta Tied Movie", "Delta Tied Movie", "Zebra Tied Movie")
    tiedMovies.toList should equal(expectedTiedOrder)

    println(s"DEBUG: Tied movies verified in alphabetical order: ${tiedMovies.mkString(", ")}")
  }

  /**
   * Test Top-N selection with many ties to verify stable ordering.
   * This edge case tests that when N < number of tied items, selection is stable.
   */
  it should "maintain stable Top-N selection with many identical ratings" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Create many movies with same rating to test Top-N cutoff behavior
    val manyTieMovies = spark.createDataset(Seq(
      Movie(601L, "Movie A", List("Action")),
      Movie(602L, "Movie B", List("Drama")),
      Movie(603L, "Movie C", List("Comedy")),
      Movie(604L, "Movie D", List("Thriller")),
      Movie(605L, "Movie E", List("Sci-Fi")),
      Movie(606L, "Movie F", List("Romance")),
      Movie(607L, "Movie G", List("Horror")),
      Movie(608L, "Movie H", List("Adventure"))
    ))

    // All movies have same rating - tie-breaking by title is critical
    val sameTieStats = spark.createDataset(Seq(
      (601L, (10L, 4.0)), (602L, (10L, 4.0)), (603L, (10L, 4.0)), (604L, (10L, 4.0)),
      (605L, (10L, 4.0)), (606L, (10L, 4.0)), (607L, (10L, 4.0)), (608L, (10L, 4.0))
    ))

    println("DEBUG: Testing Top-N selection with many identical ratings")
    // Select top 3 out of 8 movies with identical ratings
    val result = JoinStage.topNMovies(manyTieMovies, sameTieStats, n = 3, minCount = 5L)
    val resultList = result.collect()

    resultList should have size 3

    // Should get the first 3 alphabetically: Movie A, Movie B, Movie C
    val expectedTop3 = List("Movie A", "Movie B", "Movie C")
    val actualTop3 = resultList.map(_._1).toList

    actualTop3 should equal(expectedTop3)

    // All should have same rating and count
    resultList.foreach { case (_, count, avg) =>
      count should equal(10L)
      avg should equal(4.0 +- 0.001)
    }

    println(s"DEBUG: Top-3 from ties selected: ${actualTop3.mkString(", ")}")
  }

  /**
   * Test empty datasets (0 rows) to verify JoinStage handles gracefully.
   * This edge case ensures topNMovies function doesn't fail with empty input.
   */
  it should "handle empty datasets gracefully in topNMovies" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyMovies = spark.createDataset(Seq.empty[Movie])
    val emptyStats = spark.createDataset(Seq.empty[(Long, (Long, Double))])

    println("DEBUG: Testing topNMovies with empty datasets")
    val result = JoinStage.topNMovies(emptyMovies, emptyStats, n = 5, minCount = 1L)
    val resultList = result.collect()

    resultList should have size 0
    println(s"DEBUG: Empty dataset topNMovies test - result size: ${resultList.length}")
  }
}