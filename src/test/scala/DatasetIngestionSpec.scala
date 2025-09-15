import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetIngestionSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  "DatasetIngestion.loadMovies" should "load movies with correct schema" in {
    val moviesContent =
      """movieId,title,genres
        |1,Test Movie,Action|Comedy
        |2,Another Movie,Drama""".stripMargin

    val filePath = createTempFile(moviesContent, "test_movies.csv")
    val dfEither = DatasetIngestion.loadMovies(spark, filePath)

    dfEither should be('right)
    val df = dfEither.getOrElse(fail("Should have loaded movies successfully"))

    df.columns should contain allOf ("movieId", "title", "genres")
    df.count() should be(2)
  }

  "DatasetIngestion.loadRatings" should "load ratings with correct schema" in {
    val ratingsContent =
      """userId,movieId,rating,timestamp
        |1,1,4.5,1234567890
        |2,1,3.0,1234567891""".stripMargin

    val filePath = createTempFile(ratingsContent, "test_ratings.csv")
    val dfEither = DatasetIngestion.loadRatings(spark, filePath)

    dfEither should be('right)
    val df = dfEither.getOrElse(fail("Should have loaded ratings successfully"))

    df.columns should contain allOf ("userId", "movieId", "rating", "timestamp")
    df.count() should be(2)
  }

  "DatasetIngestion.loadCSV" should "load generic CSV with custom options" in {
    val csvContent = "col1,col2\nvalue1,value2\nvalue3,value4"
    val filePath = createTempFile(csvContent, "test_generic.csv")

    val df = DatasetIngestion.loadCSV(spark, filePath, header = true, delimiter = ",", inferSchema = true)

    df.columns should contain allOf ("col1", "col2")
    df.count() should be(2)
  }

  it should "handle missing files gracefully" in {
    val nonExistentPath = "/path/that/does/not/exist.csv"
    val dfEither = DatasetIngestion.loadMovies(spark, nonExistentPath)

    dfEither should be('left)
    val error = dfEither.left.getOrElse(fail("Should have returned Left with error"))
    error should not be empty
  }

  it should "handle empty files" in {
    val emptyContent = "movieId,title,genres\n"
    val filePath = createTempFile(emptyContent, "empty_movies.csv")
    val dfEither = DatasetIngestion.loadMovies(spark, filePath)

    dfEither should be('right)
    val df = dfEither.getOrElse(fail("Should have loaded empty file successfully"))
    df.count() should be(0)
  }
}