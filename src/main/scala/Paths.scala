/**
 * Configuration object for dataset file paths.
 * 
 * This case object provides centralized path management for data files,
 * with support for runtime configuration via system properties and command-line arguments.
 * Paths can be customized by setting system properties or passing arguments to the application.
 */
case object Paths {

  /**
   * Default base directory for data files.
   * Can be overridden via system property 'data.base.dir' or command-line argument.
   */
  private val baseDir: String = sys.props.getOrElse("data.base.dir", "data")

  /**
   * Path to the ratings dataset file.
   * 
   * Default location is "data/ratings.csv", but can be customized via:
   * - System property: ratings.path
   * - Command-line argument: --ratings-path
   * 
   * @return The path to the ratings CSV file
   */
  def ratingsPath: String = {
    sys.props.getOrElse("ratings.path", s"$baseDir/ratings.csv")
  }

  /**
   * Path to the movies dataset file.
   * 
   * Default location is "data/movies.csv", but can be customized via:
   * - System property: movies.path  
   * - Command-line argument: --movies-path
   * 
   * @return The path to the movies CSV file
   */
  def moviesPath: String = {
    sys.props.getOrElse("movies.path", s"$baseDir/movies.csv")
  }

  /**
   * Utility method to configure paths from command-line arguments.
   * 
   * This method parses common argument patterns and sets corresponding system properties.
   * Supported argument formats:
   * - --ratings-path=/path/to/ratings.csv
   * - --movies-path=/path/to/movies.csv
   * - --data-dir=/path/to/data
   * 
   * @param args Array of command-line arguments
   */
  def configureFromArgs(args: Array[String]): Unit = {
    args.foreach { arg =>
      arg match {
        case arg if arg.startsWith("--ratings-path=") =>
          System.setProperty("ratings.path", arg.substring("--ratings-path=".length))
        case arg if arg.startsWith("--movies-path=") =>
          System.setProperty("movies.path", arg.substring("--movies-path=".length))
        case arg if arg.startsWith("--data-dir=") =>
          System.setProperty("data.base.dir", arg.substring("--data-dir=".length))
        case _ => // Ignore other arguments
      }
    }
  }

  /**
   * Returns a summary of current path configuration.
   * 
   * @return A formatted string showing current paths
   */
  def summary: String = {
    s"""Path Configuration:
       |  Base Directory: $baseDir
       |  Ratings Path: $ratingsPath
       |  Movies Path: $moviesPath""".stripMargin
  }
}