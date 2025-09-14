/**
 * Console Pretty Printer Utility
 *
 * Provides formatted console output for Top-N results with aligned columns
 * and DEBUG preview functionality. This utility formats movie ranking data
 * in a tabular format for clear console presentation during local demonstrations.
 *
 * == Usage Example ==
 * {{{
 * val topMovies = List(
 *   ("The Shawshank Redemption", 15L, 4.833),
 *   ("The Godfather", 12L, 4.708),
 *   ("Pulp Fiction", 10L, 4.500)
 * )
 * ConsolePrettyPrinter.printTopNMovies(topMovies, maxPreviewCount = 10)
 * }}}
 *
 * @author HIT-FP-Spark-Project
 * @version 1.0
 */
object ConsolePrettyPrinter {

  /**
   * Prints Top-N movie results in a formatted table with aligned columns.
   *
   * Creates a console-friendly table with proper column alignment and formatting.
   * Includes a header row and separator lines for enhanced readability.
   * Truncates long movie titles to fit within console width constraints.
   *
   * @param results List of tuples containing (title, count, average) for each movie
   * @param maxPreviewCount Maximum number of results to display (for DEBUG preview)
   * @param titleMaxWidth Maximum width for title column (default: 40 chars)
   */
  def printTopNMovies(
    results: List[(String, Long, Double)],
    maxPreviewCount: Int = 10,
    titleMaxWidth: Int = 40
  ): Unit = {

    if (results.isEmpty) {
      println("DEBUG: No movies to display")
      return
    }

    val limitedResults = results.take(maxPreviewCount)
    val totalCount = results.length

    if (totalCount > maxPreviewCount) {
      println(s"DEBUG: Showing first $maxPreviewCount of $totalCount total results")
    } else {
      println(s"DEBUG: Showing all $totalCount results")
    }

    println()

    // Calculate column widths
    val rankWidth = Math.max(4, limitedResults.length.toString.length + 1)
    val countWidth = Math.max(5, if (limitedResults.nonEmpty) limitedResults.map(_._2.toString.length).max else 5)
    val avgWidth = 7 // "4.833" format
    val actualTitleWidth = Math.min(titleMaxWidth,
      if (limitedResults.nonEmpty) limitedResults.map(_._1.length).max else titleMaxWidth)

    // Header
    val headerFormat = s"%-${rankWidth}s | %-${actualTitleWidth}s | %${countWidth}s | %${avgWidth}s"
    val separatorLength = rankWidth + 3 + actualTitleWidth + 3 + countWidth + 3 + avgWidth

    printf(headerFormat + "\n", "Rank", "Movie Title", "Count", "Average")
    println("-" * separatorLength)

    // Data rows
    limitedResults.zipWithIndex.foreach { case ((title, count, average), index) =>
      val rank = (index + 1).toString + "."
      val truncatedTitle = if (title.length > actualTitleWidth) {
        title.take(actualTitleWidth - 3) + "..."
      } else {
        title
      }

      val rowFormat = s"%-${rankWidth}s | %-${actualTitleWidth}s | %,${countWidth}d | %${avgWidth}.3f"
      printf(rowFormat + "\n", rank, truncatedTitle, count, average)
    }

    println("-" * separatorLength)

    if (totalCount > maxPreviewCount) {
      println(s"... and ${totalCount - maxPreviewCount} more results (see output files for complete data)")
    }

    println()
  }

  /**
   * Prints a compact single-line summary of Top-N results.
   *
   * Provides a brief summary format suitable for logging and status updates.
   * Shows the top result and total count for quick verification.
   *
   * @param results List of tuples containing (title, count, average) for each movie
   */
  def printTopNSummary(results: List[(String, Long, Double)]): Unit = {
    if (results.isEmpty) {
      println("DEBUG: No results to summarize")
    } else if (results.length == 1) {
      val (title, count, avg) = results.head
      println(f"DEBUG: Top movie: '$title' (count=$count%,d, avg=$avg%.3f)")
    } else {
      val (title, count, avg) = results.head
      println(f"DEBUG: Top movie: '$title' (count=$count%,d, avg=$avg%.3f) + ${results.length - 1} others")
    }
  }

  /**
   * Prints detailed DEBUG information for each Top-N result.
   *
   * Provides verbose debugging output with full precision values and
   * additional metadata. Useful for development and troubleshooting.
   *
   * @param results List of tuples containing (title, count, average) for each movie
   * @param prefix Optional prefix for each debug line (default: "DEBUG")
   */
  def printDebugDetails(results: List[(String, Long, Double)], prefix: String = "DEBUG"): Unit = {
    if (results.isEmpty) {
      println(s"$prefix: No results to display in debug mode")
    } else {
      println(s"$prefix: === DETAILED TOP-N RESULTS ===")
      results.zipWithIndex.foreach { case ((title, count, average), index) =>
        println(f"$prefix: ${index + 1}%2d. '$title' (count=$count%,d, avg=$average%.6f)")
      }
      println(s"$prefix: === END TOP-N RESULTS ===")
    }
  }
}