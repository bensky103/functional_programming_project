import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PathsSpec extends AnyFlatSpec with Matchers {

  "Paths.ratingsPath" should "return default path when no system property set" in {
    System.clearProperty("ratings.path")
    Paths.ratingsPath should be("data/ratings.csv")
  }

  it should "return custom path when system property is set" in {
    System.setProperty("ratings.path", "/custom/ratings.csv")
    Paths.ratingsPath should be("/custom/ratings.csv")
    System.clearProperty("ratings.path") // cleanup
  }

  "Paths.moviesPath" should "return default path when no system property set" in {
    System.clearProperty("movies.path")
    Paths.moviesPath should be("data/movies.csv")
  }

  it should "return custom path when system property is set" in {
    System.setProperty("movies.path", "/custom/movies.csv")
    Paths.moviesPath should be("/custom/movies.csv")
    System.clearProperty("movies.path") // cleanup
  }

  "Paths.configureFromArgs" should "set system properties from command line arguments" in {
    val args = Array("--ratings-path=/test/ratings.csv", "--movies-path=/test/movies.csv")
    Paths.configureFromArgs(args)

    System.getProperty("ratings.path") should be("/test/ratings.csv")
    System.getProperty("movies.path") should be("/test/movies.csv")

    // cleanup
    System.clearProperty("ratings.path")
    System.clearProperty("movies.path")
  }

  "Paths.summary" should "return formatted configuration summary" in {
    System.clearProperty("ratings.path")
    System.clearProperty("movies.path")
    val summary = Paths.summary

    summary should include("data/ratings.csv")
    summary should include("data/movies.csv")
    summary should include("Path Configuration:")
  }
}