import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession

/**
 * Test suite for Spark bootstrap and dataset ingestion components.
 * 
 * This test class verifies that SparkBootstrap, Paths, and DatasetIngestion
 * components work correctly with test data files and proper SparkSession management.
 */
class SparkComponentsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkBootstrap.session("SparkComponentsTest")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  "SparkBootstrap" should "create a valid SparkSession" in {
    spark should not be null
    spark.sparkContext.appName should equal("SparkComponentsTest")
    spark.sparkContext.master should startWith("local")
  }

  it should "create SparkSession with default app name" in {
    // Stop the existing session to test new session creation
    spark.stop()
    val defaultSpark = SparkBootstrap.session()
    defaultSpark should not be null
    defaultSpark.sparkContext.appName should equal("HIT-FP-Spark")
    // Reassign the test spark session for other tests
    spark = defaultSpark
  }

  "Paths" should "provide default path configuration" in {
    Paths.ratingsPath should equal("data/ratings.csv")
    Paths.moviesPath should equal("data/movies.csv")
  }

  it should "support system property overrides" in {
    System.setProperty("ratings.path", "/custom/ratings.csv")
    System.setProperty("movies.path", "/custom/movies.csv")
    
    Paths.ratingsPath should equal("/custom/ratings.csv")
    Paths.moviesPath should equal("/custom/movies.csv")
    
    // Clean up
    System.clearProperty("ratings.path")
    System.clearProperty("movies.path")
  }

  it should "configure paths from command line arguments" in {
    val args = Array("--ratings-path=/test/ratings.csv", "--movies-path=/test/movies.csv")
    Paths.configureFromArgs(args)
    
    Paths.ratingsPath should equal("/test/ratings.csv")
    Paths.moviesPath should equal("/test/movies.csv")
    
    // Clean up
    System.clearProperty("ratings.path")
    System.clearProperty("movies.path")
  }

  it should "generate summary of current configuration" in {
    val summary = Paths.summary
    summary should include("Path Configuration:")
    summary should include("Base Directory:")
    summary should include("Ratings Path:")
    summary should include("Movies Path:")
  }

  "DatasetIngestion" should "load movies test data successfully" in {
    val testMoviesPath = "src/test/resources/test_movies.csv"
    val moviesDF = DatasetIngestion.loadMovies(spark, testMoviesPath)
    
    moviesDF should not be null
    moviesDF.count() should be > 0L
    
    val columns = moviesDF.columns.toSet
    columns should contain("movieId")
    columns should contain("title")
    columns should contain("genres")
  }

  it should "load ratings test data successfully" in {
    val testRatingsPath = "src/test/resources/test_ratings.csv"
    val ratingsDF = DatasetIngestion.loadRatings(spark, testRatingsPath)
    
    ratingsDF should not be null
    ratingsDF.count() should be > 0L
    
    val columns = ratingsDF.columns.toSet
    columns should contain("userId")
    columns should contain("movieId")
    columns should contain("rating")
    columns should contain("timestamp")
  }

  it should "validate movies dataset schema and content" in {
    val testMoviesPath = "src/test/resources/test_movies.csv"
    val moviesDF = DatasetIngestion.loadMovies(spark, testMoviesPath)
    
    val schema = moviesDF.schema
    val movieIdField = schema.find(_.name == "movieId")
    movieIdField should be(defined)
    movieIdField.get.dataType should be(org.apache.spark.sql.types.IntegerType)
    
    val titleField = schema.find(_.name == "title")
    titleField should be(defined)
    titleField.get.dataType should be(org.apache.spark.sql.types.StringType)
    
    // Verify we can collect data without errors
    val rows = moviesDF.collect()
    rows.length should be > 0
    
    // Verify first row has expected data
    val firstRow = rows(0)
    firstRow.getAs[Int]("movieId") should equal(1)
    firstRow.getAs[String]("title") should include("Toy Story")
  }

  it should "validate ratings dataset schema and content" in {
    val testRatingsPath = "src/test/resources/test_ratings.csv"
    val ratingsDF = DatasetIngestion.loadRatings(spark, testRatingsPath)
    
    val schema = ratingsDF.schema
    val userIdField = schema.find(_.name == "userId")
    userIdField should be(defined)
    userIdField.get.dataType should be(org.apache.spark.sql.types.IntegerType)
    
    val ratingField = schema.find(_.name == "rating")
    ratingField should be(defined)
    ratingField.get.dataType should be(org.apache.spark.sql.types.DoubleType)
    
    // Verify we can collect data without errors
    val rows = ratingsDF.collect()
    rows.length should be > 0
    
    // Verify first row has expected data
    val firstRow = rows(0)
    firstRow.getAs[Int]("userId") should equal(1)
    firstRow.getAs[Double]("rating") should equal(4.0)
  }

  it should "load generic CSV files with custom options" in {
    val testMoviesPath = "src/test/resources/test_movies.csv"
    val df = DatasetIngestion.loadCSV(spark, testMoviesPath, header = true, delimiter = ",", inferSchema = true)
    
    df should not be null
    df.count() should be > 0L
    df.columns should contain("movieId")
  }

  "Integration test" should "load both datasets and perform basic operations" in {
    val testMoviesPath = "src/test/resources/test_movies.csv"
    val testRatingsPath = "src/test/resources/test_ratings.csv"
    
    val moviesDF = DatasetIngestion.loadMovies(spark, testMoviesPath)
    val ratingsDF = DatasetIngestion.loadRatings(spark, testRatingsPath)
    
    // Join operation to verify both datasets work together
    val joinedDF = ratingsDF.join(moviesDF, "movieId")
    
    joinedDF should not be null
    joinedDF.count() should be > 0L
    
    val columns = joinedDF.columns.toSet
    columns should contain("movieId")
    columns should contain("title")
    columns should contain("userId")
    columns should contain("rating")
  }
}