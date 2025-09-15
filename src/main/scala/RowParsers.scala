import org.apache.spark.sql.Row
import scala.util.{Try, Success, Failure}

/**
 * Row parsing utilities using pattern matching and functional error handling.
 * 
 * This object provides pure functions for converting Spark SQL Row objects into
 * domain case classes with comprehensive error handling and validation.
 * All functions return Either[String, T] for safe error propagation.
 */
object RowParsers {

  /**
   * Parses a Spark SQL Row into a Movie case class using pattern matching.
   * 
   * Expected row structure: [movieId: Long/Int, title: String, genres: String]
   * The genres field is split by "|" delimiter and converted to List[String].
   * Handles malformed data by returning Left with descriptive error messages.
   * 
   * @param row The Spark SQL Row to parse
   * @return Either[String, Movie] - Left with error message on failure, Right with Movie on success
   */
  def parseMovie(row: Row): Either[String, Movie] = {
    Try {
      // Pattern match on row structure and extract fields
      val movieId = row.get(0) match {
        case id: Long => id
        case id: Int => id.toLong
        case null => return Left("movieId cannot be null")
        case other => return Left(s"movieId must be numeric, got: ${other.getClass.getSimpleName}")
      }

      val title = row.get(1) match {
        case title: String if title.nonEmpty => title.trim
        case title: String if title.isEmpty =>
          println(s"DEBUG: Empty title found for movieId: $movieId")
          return Left("title cannot be empty")
        case null =>
          println(s"DEBUG: Null title found for movieId: $movieId")
          return Left("title cannot be null")
        case other => return Left(s"title must be String, got: ${other.getClass.getSimpleName}")
      }

      val genres = row.get(2) match {
        case genresStr: String if genresStr.nonEmpty =>
          genresStr.split("\\|").map(_.trim).filter(_.nonEmpty).toList
        case genresStr: String if genresStr.isEmpty =>
          println(s"DEBUG: Empty genres field for movieId: $movieId, title: '$title'")
          List.empty[String]
        case null =>
          println(s"DEBUG: Null genres field for movieId: $movieId, title: '$title'")
          List.empty[String]
        case other => return Left(s"genres must be String, got: ${other.getClass.getSimpleName}")
      }

      Movie(movieId, title, genres)
    } match {
      case Success(movie) => Right(movie)
      case Failure(exception) =>
        val errorMsg = s"Failed to parse Movie row: ${exception.getMessage}"
        println(s"DEBUG: $errorMsg")
        Left(errorMsg)
    }
  }

  /**
   * Parses a Spark SQL Row into a Rating case class using pattern matching.
   * 
   * Expected row structure: [userId: Long/Int, movieId: Long/Int, rating: Double, timestamp: Long]
   * Validates that rating is within reasonable bounds and all required fields are present.
   * 
   * @param row The Spark SQL Row to parse
   * @return Either[String, Rating] - Left with error message on failure, Right with Rating on success
   */
  def parseRating(row: Row): Either[String, Rating] = {
    Try {
      val userId = row.get(0) match {
        case id: Long => id
        case id: Int => id.toLong
        case null => return Left("userId cannot be null")
        case other => return Left(s"userId must be numeric, got: ${other.getClass.getSimpleName}")
      }

      val movieId = row.get(1) match {
        case id: Long => id
        case id: Int => id.toLong
        case null => return Left("movieId cannot be null")
        case other => return Left(s"movieId must be numeric, got: ${other.getClass.getSimpleName}")
      }

      val rating = row.get(2) match {
        case r: Double => 
          if (r >= 0.0 && r <= 5.0) r
          else {
            println(s"DEBUG: Rating out of bounds for userId: $userId, movieId: $movieId, rating: $r")
            return Left(s"rating must be between 0.0 and 5.0, got: $r")
          }
        case r: Float => 
          val doubleRating = r.toDouble
          if (doubleRating >= 0.0 && doubleRating <= 5.0) doubleRating
          else {
            println(s"DEBUG: Rating out of bounds for userId: $userId, movieId: $movieId, rating: $doubleRating")
            return Left(s"rating must be between 0.0 and 5.0, got: $doubleRating")
          }
        case null => return Left("rating cannot be null")
        case other => return Left(s"rating must be numeric, got: ${other.getClass.getSimpleName}")
      }

      val timestamp = row.get(3) match {
        case ts: Long => ts
        case ts: Int => ts.toLong
        case null => return Left("timestamp cannot be null")
        case other => return Left(s"timestamp must be numeric, got: ${other.getClass.getSimpleName}")
      }

      Rating(userId, movieId, rating, timestamp)
    } match {
      case Success(rating) => Right(rating)
      case Failure(exception) =>
        val errorMsg = s"Failed to parse Rating row: ${exception.getMessage}"
        println(s"DEBUG: $errorMsg")
        Left(errorMsg)
    }
  }
}