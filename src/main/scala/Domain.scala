/**
 * Domain model case classes for the movie recommendation system.
 * 
 * This module defines the core domain entities used throughout the application:
 * - Movie: Represents a movie with metadata
 * - Rating: Represents a user's rating of a movie
 */

/**
 * Represents a movie in the system.
 * 
 * @param movieId Unique identifier for the movie
 * @param title The title of the movie
 * @param genres List of genres associated with the movie (e.g., ["Action", "Comedy"])
 */
case class Movie(
  movieId: Long,
  title: String,
  genres: List[String]
)

/**
 * Represents a user's rating of a movie.
 * 
 * @param userId Unique identifier for the user who made the rating
 * @param movieId Unique identifier for the movie being rated
 * @param rating The numerical rating given by the user (typically 0.5 to 5.0)
 * @param timestamp Unix timestamp when the rating was made
 */
case class Rating(
  userId: Long,
  movieId: Long,
  rating: Double,
  timestamp: Long
)