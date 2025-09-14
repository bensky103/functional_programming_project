import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Test suite for RowParsers functionality.
 * 
 * Tests both success and failure scenarios for parseMovie and parseRating functions,
 * ensuring pattern matching branches are properly executed and Either results are correct.
 */
class RowParsersSpec extends AnyFlatSpec with Matchers {

  "parseMovie" should "successfully parse a valid movie row" in {
    val validRow = Row(1L, "Toy Story (1995)", "Animation|Children|Comedy")
    val result = RowParsers.parseMovie(validRow)
    
    result shouldBe Right(Movie(1L, "Toy Story (1995)", List("Animation", "Children", "Comedy")))
  }

  it should "handle integer movieId by converting to Long" in {
    val rowWithInt = Row(42, "Test Movie", "Action")
    val result = RowParsers.parseMovie(rowWithInt)
    
    result shouldBe Right(Movie(42L, "Test Movie", List("Action")))
  }

  it should "handle empty genres string" in {
    val rowWithEmptyGenres = Row(1L, "Movie Title", "")
    val result = RowParsers.parseMovie(rowWithEmptyGenres)
    
    result shouldBe Right(Movie(1L, "Movie Title", List.empty[String]))
  }

  it should "handle null genres field" in {
    val rowWithNullGenres = Row(1L, "Movie Title", null)
    val result = RowParsers.parseMovie(rowWithNullGenres)
    
    result shouldBe Right(Movie(1L, "Movie Title", List.empty[String]))
  }

  it should "filter empty genre segments after splitting" in {
    val rowWithExtraDelimiters = Row(1L, "Movie", "Action||Comedy|")
    val result = RowParsers.parseMovie(rowWithExtraDelimiters)
    
    result shouldBe Right(Movie(1L, "Movie", List("Action", "Comedy")))
  }

  it should "trim whitespace from title and genres" in {
    val rowWithWhitespace = Row(1L, "  Trimmed Title  ", " Action | Comedy ")
    val result = RowParsers.parseMovie(rowWithWhitespace)
    
    result shouldBe Right(Movie(1L, "Trimmed Title", List("Action", "Comedy")))
  }

  it should "fail with null movieId" in {
    val rowWithNullId = Row(null, "Movie Title", "Action")
    val result = RowParsers.parseMovie(rowWithNullId)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("movieId cannot be null")
  }

  it should "fail with invalid movieId type" in {
    val rowWithInvalidId = Row("not_a_number", "Movie Title", "Action")
    val result = RowParsers.parseMovie(rowWithInvalidId)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("movieId must be numeric")
  }

  it should "fail with null title" in {
    val rowWithNullTitle = Row(1L, null, "Action")
    val result = RowParsers.parseMovie(rowWithNullTitle)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("title cannot be null")
  }

  it should "fail with empty title" in {
    val rowWithEmptyTitle = Row(1L, "", "Action")
    val result = RowParsers.parseMovie(rowWithEmptyTitle)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("title cannot be empty")
  }

  it should "fail with invalid title type" in {
    val rowWithInvalidTitle = Row(1L, 123, "Action")
    val result = RowParsers.parseMovie(rowWithInvalidTitle)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("title must be String")
  }

  "parseRating" should "successfully parse a valid rating row" in {
    val validRow = Row(1L, 31L, 2.5, 1260759144L)
    val result = RowParsers.parseRating(validRow)
    
    result shouldBe Right(Rating(1L, 31L, 2.5, 1260759144L))
  }

  it should "handle integer userId and movieId by converting to Long" in {
    val rowWithInts = Row(42, 100, 4.0, 1234567890L)
    val result = RowParsers.parseRating(rowWithInts)
    
    result shouldBe Right(Rating(42L, 100L, 4.0, 1234567890L))
  }

  it should "handle float rating by converting to Double" in {
    val rowWithFloat = Row(1L, 2L, 3.5f, 1000000000L)
    val result = RowParsers.parseRating(rowWithFloat)
    
    result shouldBe Right(Rating(1L, 2L, 3.5, 1000000000L))
  }

  it should "handle integer timestamp by converting to Long" in {
    val rowWithIntTimestamp = Row(1L, 2L, 4.0, 999999999)
    val result = RowParsers.parseRating(rowWithIntTimestamp)
    
    result shouldBe Right(Rating(1L, 2L, 4.0, 999999999L))
  }

  it should "accept rating at boundary values" in {
    val minRating = Row(1L, 2L, 0.0, 1000000000L)
    val maxRating = Row(1L, 2L, 5.0, 1000000000L)
    
    RowParsers.parseRating(minRating) shouldBe Right(Rating(1L, 2L, 0.0, 1000000000L))
    RowParsers.parseRating(maxRating) shouldBe Right(Rating(1L, 2L, 5.0, 1000000000L))
  }

  it should "fail with null userId" in {
    val rowWithNullUserId = Row(null, 2L, 3.0, 1000000000L)
    val result = RowParsers.parseRating(rowWithNullUserId)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("userId cannot be null")
  }

  it should "fail with null movieId" in {
    val rowWithNullMovieId = Row(1L, null, 3.0, 1000000000L)
    val result = RowParsers.parseRating(rowWithNullMovieId)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("movieId cannot be null")
  }

  it should "fail with null rating" in {
    val rowWithNullRating = Row(1L, 2L, null, 1000000000L)
    val result = RowParsers.parseRating(rowWithNullRating)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("rating cannot be null")
  }

  it should "fail with null timestamp" in {
    val rowWithNullTimestamp = Row(1L, 2L, 3.0, null)
    val result = RowParsers.parseRating(rowWithNullTimestamp)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("timestamp cannot be null")
  }

  it should "fail with rating below minimum bound" in {
    val rowWithLowRating = Row(1L, 2L, -0.5, 1000000000L)
    val result = RowParsers.parseRating(rowWithLowRating)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("rating must be between 0.0 and 5.0")
  }

  it should "fail with rating above maximum bound" in {
    val rowWithHighRating = Row(1L, 2L, 5.5, 1000000000L)
    val result = RowParsers.parseRating(rowWithHighRating)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("rating must be between 0.0 and 5.0")
  }

  it should "fail with invalid userId type" in {
    val rowWithInvalidUserId = Row("invalid", 2L, 3.0, 1000000000L)
    val result = RowParsers.parseRating(rowWithInvalidUserId)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("userId must be numeric")
  }

  it should "fail with invalid movieId type" in {
    val rowWithInvalidMovieId = Row(1L, "invalid", 3.0, 1000000000L)
    val result = RowParsers.parseRating(rowWithInvalidMovieId)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("movieId must be numeric")
  }

  it should "fail with invalid rating type" in {
    val rowWithInvalidRating = Row(1L, 2L, "invalid", 1000000000L)
    val result = RowParsers.parseRating(rowWithInvalidRating)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("rating must be numeric")
  }

  it should "fail with invalid timestamp type" in {
    val rowWithInvalidTimestamp = Row(1L, 2L, 3.0, "invalid")
    val result = RowParsers.parseRating(rowWithInvalidTimestamp)
    
    result.isLeft shouldBe true
    result.left.getOrElse("") should include("timestamp must be numeric")
  }
}