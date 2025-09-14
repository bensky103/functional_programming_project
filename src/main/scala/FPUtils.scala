/**
 * FPUtils provides functional programming utilities including currying and function composition.
 *
 * This object demonstrates functional programming concepts in Scala by providing:
 * - Curried functions that allow partial application
 * - Function composition utilities for building complex operations from simple ones
 * - Generic utilities that can be reused across different data processing stages
 *
 * These utilities exemplify the functional programming paradigm by treating functions
 * as first-class citizens and enabling higher-order function patterns.
 */
object FPUtils {

  /**
   * Curried function that creates a predicate for checking if a value is greater than or equal to a threshold.
   *
   * This curried function demonstrates partial application - you can apply the threshold first
   * to create a reusable predicate function, then apply that predicate to multiple values.
   *
   * Example usage:
   * {{{
   * val isHighRating = FPUtils.greaterEq(4.0) _
   * val result1 = isHighRating(4.5) // true
   * val result2 = isHighRating(3.2) // false
   * }}}
   *
   * @param threshold The minimum value for the comparison (inclusive)
   * @param x The value to compare against the threshold
   * @return true if x >= threshold, false otherwise
   */
  def greaterEq(threshold: Double)(x: Double): Boolean = x >= threshold

  /**
   * Composes two functions into a single function.
   *
   * This function demonstrates function composition, a fundamental concept in functional programming.
   * Given functions f: B => C and g: A => B, it creates a new function h: A => C where h(x) = f(g(x)).
   *
   * Example usage:
   * {{{
   * val addOne = (x: Int) => x + 1
   * val toString = (x: Int) => x.toString
   * val addOneThenToString = FPUtils.compose2(toString, addOne)
   * val result = addOneThenToString(5) // "6"
   * }}}
   *
   * @tparam A The input type of the composed function
   * @tparam B The intermediate type (output of g, input of f)
   * @tparam C The output type of the composed function
   * @param f The second function to apply (B => C)
   * @param g The first function to apply (A => B)
   * @return A composed function that applies g then f
   */
  def compose2[A, B, C](f: B => C, g: A => B): A => C = (a: A) => f(g(a))

  /**
   * Curried function that takes the top N elements from an iterable.
   *
   * This curried function allows partial application of the limit parameter,
   * creating reusable functions for taking specific numbers of elements.
   *
   * Example usage:
   * {{{
   * val takeTop5 = FPUtils.takeTopN(5) _
   * val result = takeTop5(List(1, 2, 3, 4, 5, 6, 7, 8)) // List(1, 2, 3, 4, 5)
   * }}}
   *
   * @tparam T The type of elements in the iterable
   * @param n The number of elements to take from the beginning
   * @param it The iterable to take elements from
   * @return An iterable containing the first n elements
   */
  def takeTopN[T](n: Int)(it: Iterable[T]): Iterable[T] = it.take(n)
}