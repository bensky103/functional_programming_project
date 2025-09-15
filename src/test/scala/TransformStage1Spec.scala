import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Dataset

/**
 * Test suite for TransformStage1 closure-based transformations.
 *
 * This test class verifies that filterHighRatings and normalizeRatings methods
 * correctly implement closure capture and produce expected results with tiny datasets.
 */
class TransformStage1Spec extends AnyFlatSpec with Matchers with SparkTestBase {

  /**
   * Creates a tiny in-memory test dataset for Rating objects.
   */
  def createTestRatings(): Dataset[Rating] = {
    val sparkSession = spark
    import sparkSession.implicits._
    
    val testData = Seq(
      Rating(1L, 101L, 1.0, 1000L),   // Low rating
      Rating(2L, 102L, 2.5, 2000L),   // Medium rating
      Rating(3L, 103L, 4.0, 3000L),   // High rating
      Rating(4L, 104L, 5.0, 4000L),   // Maximum rating
      Rating(5L, 105L, 3.5, 5000L),   // Medium-high rating
      Rating(6L, 106L, 0.5, 6000L)    // Minimum rating
    )
    
    spark.createDataset(testData)
  }

  "TransformStage1.filterHighRatings" should "filter ratings above minimum threshold" in {
    val testRatings = createTestRatings()
    val minThreshold = 3.0
    
    val filtered = TransformStage1.filterHighRatings(testRatings, minThreshold)
    val results = filtered.collect()
    
    // Should include ratings >= 3.0: [4.0, 5.0, 3.5] = 3 ratings
    results.length should equal(3)
    
    // Verify all filtered ratings meet the minimum threshold
    results.foreach { rating =>
      rating.rating should be >= minThreshold
    }
    
    // Verify specific expected ratings are present
    val ratingValues = results.map(_.rating).sorted
    ratingValues should contain theSameElementsAs Array(3.5, 4.0, 5.0)
  }

  it should "return empty dataset when minimum threshold is too high" in {
    val testRatings = createTestRatings()
    val minThreshold = 6.0  // Higher than any rating in test data
    
    val filtered = TransformStage1.filterHighRatings(testRatings, minThreshold)
    val results = filtered.collect()
    
    results.length should equal(0)
  }

  it should "return all ratings when minimum threshold is very low" in {
    val testRatings = createTestRatings()
    val minThreshold = 0.0
    
    val filtered = TransformStage1.filterHighRatings(testRatings, minThreshold)
    val results = filtered.collect()
    
    results.length should equal(6)  // All test ratings
  }

  it should "correctly capture minimum value in closure" in {
    val testRatings = createTestRatings()
    val minThreshold = 2.5
    
    val filtered = TransformStage1.filterHighRatings(testRatings, minThreshold)
    val results = filtered.collect()
    
    // Should include ratings >= 2.5: [2.5, 4.0, 5.0, 3.5] = 4 ratings
    results.length should equal(4)
    
    // Verify closure captured the exact threshold
    results.foreach { rating =>
      rating.rating should be >= minThreshold
    }
  }

  "TransformStage1.normalizeRatings" should "normalize ratings correctly to 0-1 scale" in {
    val testRatings = createTestRatings()
    val maxRating = 5.0
    
    val normalized = TransformStage1.normalizeRatings(testRatings, maxRating)
    val results = normalized.collect()
    
    results.length should equal(6)  // Same number as input
    
    // Verify normalization: rating/maxRating
    val expected = Map(
      101L -> 1.0/5.0,    // 0.2
      102L -> 2.5/5.0,    // 0.5
      103L -> 4.0/5.0,    // 0.8
      104L -> 5.0/5.0,    // 1.0
      105L -> 3.5/5.0,    // 0.7
      106L -> 0.5/5.0     // 0.1
    )
    
    results.foreach { case (movieId, normalizedRating) =>
      normalizedRating should equal(expected(movieId) +- 0.001)
      normalizedRating should be >= 0.0
      normalizedRating should be <= 1.0
    }
  }

  it should "handle different maximum rating values" in {
    val testRatings = createTestRatings()
    val maxRating = 10.0  // Different scale
    
    val normalized = TransformStage1.normalizeRatings(testRatings, maxRating)
    val results = normalized.collect()
    
    results.length should equal(6)
    
    // All normalized values should be <= 0.5 since original max was 5.0
    results.foreach { case (_, normalizedRating) =>
      normalizedRating should be >= 0.0
      normalizedRating should be <= 0.5
    }
    
    // Verify specific normalization with 10.0 scale
    val movieId104Result = results.find(_._1 == 104L).get._2
    movieId104Result should equal(5.0/10.0 +- 0.001)  // 0.5
  }

  it should "correctly capture maxRating value in closure" in {
    val testRatings = createTestRatings()
    val maxRating = 4.0  // Custom max rating
    
    val normalized = TransformStage1.normalizeRatings(testRatings, maxRating)
    val results = normalized.collect()
    
    // Rating of 5.0 should normalize to > 1.0 when maxRating is 4.0
    val movie104Result = results.find(_._1 == 104L).get._2
    movie104Result should equal(5.0/4.0 +- 0.001)  // 1.25
    movie104Result should be > 1.0
  }

  it should "return tuples with correct movieId mapping" in {
    val testRatings = createTestRatings()
    val maxRating = 5.0
    
    val normalized = TransformStage1.normalizeRatings(testRatings, maxRating)
    val results = normalized.collect()
    
    // Verify movieId preservation
    val movieIds = results.map(_._1).sorted
    movieIds should contain theSameElementsAs Array(101L, 102L, 103L, 104L, 105L, 106L)
    
    // Verify each movieId maps to its correct normalized rating
    val resultMap = results.toMap
    resultMap(101L) should equal(0.2 +- 0.001)  // 1.0/5.0
    resultMap(104L) should equal(1.0 +- 0.001)  // 5.0/5.0
  }

  "Integration test" should "combine filtering and normalization operations" in {
    val testRatings = createTestRatings()

    // First filter high ratings (>= 3.0)
    val filtered = TransformStage1.filterHighRatings(testRatings, 3.0)

    // Convert back to Dataset[Rating] for normalization
    val sparkSession = spark
    import sparkSession.implicits._
    val filteredRatings = sparkSession.createDataset(filtered.collect().toSeq)

    // Then normalize the filtered results
    val normalized = TransformStage1.normalizeRatings(filteredRatings, 5.0)
    val results = normalized.collect()

    // Should have 3 results from filtering step
    results.length should equal(3)

    // All normalized values should correspond to ratings >= 3.0
    val expectedMovies = Set(103L, 104L, 105L)  // Movies with ratings >= 3.0
    results.map(_._1).toSet should equal(expectedMovies)

    // Verify normalization values
    val resultMap = results.toMap
    resultMap(103L) should equal(0.8 +- 0.001)  // 4.0/5.0
    resultMap(104L) should equal(1.0 +- 0.001)  // 5.0/5.0
    resultMap(105L) should equal(0.7 +- 0.001)  // 3.5/5.0
  }

  /**
   * Test empty datasets (0 rows) to verify stage handles gracefully.
   * This edge case ensures TransformStage1 functions don't fail with empty input.
   */
  "TransformStage1" should "handle empty datasets gracefully in filterHighRatings" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyRatings = spark.createDataset(Seq.empty[Rating])

    println("DEBUG: Testing filterHighRatings with empty dataset")
    val filtered = TransformStage1.filterHighRatings(emptyRatings, 3.0)
    val results = filtered.collect()

    results.length should equal(0)
    println(s"DEBUG: Empty dataset filterHighRatings test - result size: ${results.length}")
  }

  /**
   * Test empty datasets (0 rows) to verify stage handles gracefully.
   * This edge case ensures TransformStage1 functions don't fail with empty input.
   */
  it should "handle empty datasets gracefully in normalizeRatings" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyRatings = spark.createDataset(Seq.empty[Rating])

    println("DEBUG: Testing normalizeRatings with empty dataset")
    val normalized = TransformStage1.normalizeRatings(emptyRatings, 5.0)
    val results = normalized.collect()

    results.length should equal(0)
    println(s"DEBUG: Empty dataset normalizeRatings test - result size: ${results.length}")
  }
}