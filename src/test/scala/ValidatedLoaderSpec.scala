import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for ValidatedLoader functionality.
 *
 * Tests the complete pipeline of loading, parsing, and validating datasets
 * with mixed good and bad data to verify proper error handling and counts.
 */
class ValidatedLoaderSpec extends AnyFlatSpec with Matchers with SparkTestBase {
  
  "loadMovies" should "successfully load valid movies and count failures" in {
    val moviesContent = """movieId,title,genres
1,Toy Story (1995),Animation|Children|Comedy
2,Jumanji (1995),Adventure|Children|Fantasy
3,Grumpier Old Men (1995),Comedy|Romance
4,Waiting to Exhale (1995),Comedy|Drama|Romance
5,Father of the Bride Part II (1995),Comedy
invalid_id,Bad Movie,Action
7,,Horror
8,Good Movie With Empty Genres,
9,Good Movie With Null Genres,
"""
    
    val filePath = createTempFile(moviesContent, "test_movies.csv")
    val (movieDS, failureCount) = ValidatedLoader.loadMovies(spark, filePath)
    
    // Collect results for verification
    val movies = movieDS.collect()
    
    // Should successfully parse most movies
    movies.length should be >= 5
    failureCount should be >= 2L  // At least invalid_id and empty title
    
    // Verify specific successful parses
    val toyStory = movies.find(_.movieId == 1L)
    toyStory should be (defined)
    toyStory.get.title shouldBe "Toy Story (1995)"
    toyStory.get.genres should contain allOf("Animation", "Children", "Comedy")
    
    // Verify empty genres handling
    val emptyGenresMovie = movies.find(_.movieId == 8L)
    emptyGenresMovie should be (defined)
    emptyGenresMovie.get.genres shouldBe empty
  }
  
  it should "handle completely invalid data gracefully" in {
    val invalidContent = """movieId,title,genres
not_a_number,Invalid Movie,Action
,Another Bad Movie,Comedy
12345678901234567890,Numeric Overflow Movie,Drama
"""
    
    val filePath = createTempFile(invalidContent, "invalid_movies.csv")
    val (movieDS, failureCount) = ValidatedLoader.loadMovies(spark, filePath)
    
    val movies = movieDS.collect()
    movies.length shouldBe 0
    failureCount shouldBe 3
  }
  
  it should "return empty dataset on file loading failure" in {
    val nonExistentPath = "/path/that/does/not/exist/movies.csv"
    val (movieDS, failureCount) = ValidatedLoader.loadMovies(spark, nonExistentPath)
    
    val movies = movieDS.collect()
    movies.length shouldBe 0
    failureCount shouldBe Long.MaxValue  // Indicates total failure
  }
  
  "loadRatings" should "successfully load valid ratings and count failures" in {
    val ratingsContent = """userId,movieId,rating,timestamp
1,31,2.5,1260759144
1,1029,3.0,1260759179
1,1061,3.0,1260759182
1,1129,2.0,1260759185
invalid_user,1172,4.0,1260759205
2,1172,-1.0,1260759208
3,1339,6.0,1260759246
4,1341,,1260759207
5,1342,3.5,invalid_timestamp
6,1343,3.0,1260759210
"""
    
    val filePath = createTempFile(ratingsContent, "test_ratings.csv")
    val (ratingDS, failureCount) = ValidatedLoader.loadRatings(spark, filePath)
    
    // Collect results for verification
    val ratings = ratingDS.collect()
    
    // Should successfully parse some ratings
    ratings.length should be >= 5
    failureCount should be >= 4L  // Invalid user, negative rating, high rating, empty rating, invalid timestamp
    
    // Verify specific successful parses
    val firstRating = ratings.find(r => r.userId == 1L && r.movieId == 31L)
    firstRating should be (defined)
    firstRating.get.rating shouldBe 2.5
    firstRating.get.timestamp shouldBe 1260759144L
    
    // Verify all successful ratings are within bounds
    ratings.foreach { rating =>
      rating.rating should (be >= 0.0 and be <= 5.0)
      rating.userId should be > 0L
      rating.movieId should be > 0L
      rating.timestamp should be > 0L
    }
  }
  
  it should "handle boundary rating values correctly" in {
    val boundaryContent = """userId,movieId,rating,timestamp
1,1,0.0,1000000000
2,2,5.0,1000000001
3,3,0.5,1000000002
4,4,4.5,1000000003
"""
    
    val filePath = createTempFile(boundaryContent, "boundary_ratings.csv")
    val (ratingDS, failureCount) = ValidatedLoader.loadRatings(spark, filePath)
    
    val ratings = ratingDS.collect()
    ratings.length shouldBe 4
    failureCount shouldBe 0
    
    val ratingValues = ratings.map(_.rating).sorted
    ratingValues should contain allOf(0.0, 0.5, 4.5, 5.0)
  }
  
  it should "reject out-of-bounds ratings" in {
    val outOfBoundsContent = """userId,movieId,rating,timestamp
1,1,-0.5,1000000000
2,2,5.5,1000000001
3,3,10.0,1000000002
4,4,-10.0,1000000003
"""
    
    val filePath = createTempFile(outOfBoundsContent, "out_of_bounds_ratings.csv")
    val (ratingDS, failureCount) = ValidatedLoader.loadRatings(spark, filePath)
    
    val ratings = ratingDS.collect()
    ratings.length shouldBe 0
    failureCount shouldBe 4
  }
  
  it should "return empty dataset on file loading failure" in {
    val nonExistentPath = "/path/that/does/not/exist/ratings.csv"
    val (ratingDS, failureCount) = ValidatedLoader.loadRatings(spark, nonExistentPath)

    val ratings = ratingDS.collect()
    ratings.length shouldBe 0
    failureCount shouldBe Long.MaxValue  // Indicates total failure
  }

  /**
   * Test empty datasets (0 rows) to verify all stages handle gracefully.
   * This edge case ensures the loading pipeline doesn't fail with empty input.
   */
  it should "handle completely empty movies file gracefully" in {
    val emptyContent = """movieId,title,genres"""

    val filePath = createTempFile(emptyContent, "empty_movies.csv")
    val (movieDS, failureCount) = ValidatedLoader.loadMovies(spark, filePath)

    println("DEBUG: Testing empty movies dataset handling")
    val movies = movieDS.collect()
    movies.length shouldBe 0
    failureCount shouldBe 0  // No failures, just empty

    println(s"DEBUG: Empty movies test - dataset size: ${movies.length}, failures: $failureCount")
  }

  /**
   * Test empty datasets (0 rows) to verify all stages handle gracefully.
   * This edge case ensures the loading pipeline doesn't fail with empty input.
   */
  it should "handle completely empty ratings file gracefully" in {
    val emptyContent = """userId,movieId,rating,timestamp"""

    val filePath = createTempFile(emptyContent, "empty_ratings.csv")
    val (ratingDS, failureCount) = ValidatedLoader.loadRatings(spark, filePath)

    println("DEBUG: Testing empty ratings dataset handling")
    val ratings = ratingDS.collect()
    ratings.length shouldBe 0
    failureCount shouldBe 0  // No failures, just empty

    println(s"DEBUG: Empty ratings test - dataset size: ${ratings.length}, failures: $failureCount")
  }

  /**
   * Test all-bad rows in ValidatedLoader to verify failures counted == input size.
   * This edge case ensures comprehensive error tracking and graceful degradation.
   */
  it should "count all failures when every movie row is invalid" in {
    val allBadContent = """movieId,title,genres
invalid_id1,,Action
invalid_id2,Movie Without Valid ID,Comedy
,Good Title But No ID,Drama
not_a_number,Another Bad Movie,Horror
12345678901234567890123456789,Overflow ID Movie,Sci-Fi"""

    val filePath = createTempFile(allBadContent, "all_bad_movies.csv")
    val (movieDS, failureCount) = ValidatedLoader.loadMovies(spark, filePath)

    println("DEBUG: Testing all-bad movies rows handling")
    val movies = movieDS.collect()
    movies.length shouldBe 0
    failureCount shouldBe 5  // All 5 data rows should fail

    println(s"DEBUG: All-bad movies test - dataset size: ${movies.length}, failures: $failureCount")
  }

  /**
   * Test all-bad rows in ValidatedLoader to verify failures counted == input size.
   * This edge case ensures comprehensive error tracking and graceful degradation.
   */
  it should "count all failures when every rating row is invalid" in {
    val allBadContent = """userId,movieId,rating,timestamp
invalid_user,1,4.0,1000000000
1,invalid_movie,3.5,1000000001
2,2,-1.0,1000000002
3,3,6.0,1000000003
4,4,3.0,not_a_timestamp
,5,4.5,1000000004
5,,2.5,1000000005"""

    val filePath = createTempFile(allBadContent, "all_bad_ratings.csv")
    val (ratingDS, failureCount) = ValidatedLoader.loadRatings(spark, filePath)

    println("DEBUG: Testing all-bad ratings rows handling")
    val ratings = ratingDS.collect()
    ratings.length shouldBe 0
    failureCount shouldBe 7  // All 7 data rows should fail

    println(s"DEBUG: All-bad ratings test - dataset size: ${ratings.length}, failures: $failureCount")
  }

  "ValidatedLoader" should "maintain referential transparency" in {
    val moviesContent = """movieId,title,genres
1,Test Movie,Action
invalid,Bad Movie,Comedy
"""
    
    val filePath = createTempFile(moviesContent, "referential_test.csv")
    
    // Multiple calls should return identical results
    val (movies1, failures1) = ValidatedLoader.loadMovies(spark, filePath)
    val (movies2, failures2) = ValidatedLoader.loadMovies(spark, filePath)
    
    movies1.collect().length shouldBe movies2.collect().length
    failures1 shouldBe failures2
    
    // Results should be deterministic
    val movieList1 = movies1.collect().sortBy(_.movieId)
    val movieList2 = movies2.collect().sortBy(_.movieId)
    movieList1 shouldBe movieList2
  }
}