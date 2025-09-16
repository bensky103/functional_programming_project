import scala.annotation.tailrec

/**
 * FPUtils provides functional programming utilities including currying, function composition,
 * tail recursion, and custom combinators.
 *
 * This object demonstrates functional programming concepts in Scala by providing:
 * - Curried functions that allow partial application
 * - Function composition utilities for building complex operations from simple ones
 * - Tail recursive functions for efficient recursive computations
 * - Custom combinators for advanced functional programming patterns
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

  // ========== TAIL RECURSION FUNCTIONS ==========

  /**
   * Tail recursive function to calculate the sum of a list of integers.
   *
   * This function demonstrates tail recursion optimization using @tailrec annotation.
   * Tail recursion ensures constant stack space usage for large lists, preventing
   * stack overflow errors that would occur with naive recursion.
   *
   * Example usage:
   * {{{
   * val result = FPUtils.sumTailRec(List(1, 2, 3, 4, 5)) // 15
   * val bigList = (1 to 100000).toList
   * val bigSum = FPUtils.sumTailRec(bigList) // Works without stack overflow
   * }}}
   *
   * @param numbers The list of integers to sum
   * @return The sum of all integers in the list
   */
  def sumTailRec(numbers: List[Int]): Long = {
    @tailrec
    def sumHelper(remaining: List[Int], accumulator: Long): Long = {
      remaining match {
        case Nil => accumulator
        case head :: tail => sumHelper(tail, accumulator + head)
      }
    }
    sumHelper(numbers, 0L)
  }

  /**
   * Tail recursive function to calculate the factorial of a number.
   *
   * This function uses tail recursion with an accumulator to compute factorial
   * efficiently without stack overflow for large numbers.
   *
   * Example usage:
   * {{{
   * val result = FPUtils.factorialTailRec(5) // 120
   * val bigFactorial = FPUtils.factorialTailRec(20) // Works efficiently
   * }}}
   *
   * @param n The number to calculate factorial for (must be non-negative)
   * @return The factorial of n, or 1 if n <= 0
   */
  def factorialTailRec(n: Long): Long = {
    @tailrec
    def factorialHelper(remaining: Long, accumulator: Long): Long = {
      if (remaining <= 1) accumulator
      else factorialHelper(remaining - 1, accumulator * remaining)
    }
    if (n <= 0) 1L else factorialHelper(n, 1L)
  }

  /**
   * Tail recursive function to find the length of a list.
   *
   * This demonstrates tail recursion for list traversal without using the built-in
   * length method, showing how to process lists recursively with constant stack usage.
   *
   * Example usage:
   * {{{
   * val result = FPUtils.lengthTailRec(List(1, 2, 3, 4)) // 4
   * val emptyLength = FPUtils.lengthTailRec(List()) // 0
   * }}}
   *
   * @tparam T The type of elements in the list
   * @param list The list to measure
   * @return The number of elements in the list
   */
  def lengthTailRec[T](list: List[T]): Int = {
    @tailrec
    def lengthHelper(remaining: List[T], count: Int): Int = {
      remaining match {
        case Nil => count
        case _ :: tail => lengthHelper(tail, count + 1)
      }
    }
    lengthHelper(list, 0)
  }

  // ========== CUSTOM COMBINATORS ==========

  /**
   * Maybe combinator that applies a function only if the input is not null.
   *
   * This combinator provides safe function application with null checking,
   * returning None for null inputs and Some(result) for valid inputs.
   *
   * Example usage:
   * {{{
   * val addOne = (x: Int) => x + 1
   * val result1 = FPUtils.maybe(addOne)(5) // Some(6)
   * val result2 = FPUtils.maybe(addOne)(null) // None
   * }}}
   *
   * @tparam A The input type
   * @tparam B The output type
   * @param f The function to apply conditionally
   * @param input The input value (may be null)
   * @return Some(f(input)) if input is not null, None otherwise
   */
  def maybe[A, B](f: A => B)(input: A): Option[B] = {
    if (input != null) Some(f(input)) else None
  }

  /**
   * Retry combinator that attempts to apply a function multiple times on failure.
   *
   * This combinator provides resilience by retrying operations that may fail,
   * returning the first successful result or the final failure.
   *
   * Example usage:
   * {{{
   * val unreliableFunc = (x: Int) => if (scala.util.Random.nextBoolean()) x * 2 else throw new RuntimeException("Failed")
   * val result = FPUtils.retry(3)(unreliableFunc)(5) // Tries up to 3 times
   * }}}
   *
   * @tparam A The input type
   * @tparam B The output type
   * @param maxAttempts The maximum number of attempts
   * @param f The function to retry
   * @param input The input value
   * @return Either the successful result or the final exception
   */
  def retry[A, B](maxAttempts: Int)(f: A => B)(input: A): Either[Throwable, B] = {
    @tailrec
    def retryHelper(attemptsLeft: Int, lastError: Option[Throwable]): Either[Throwable, B] = {
      if (attemptsLeft <= 0) {
        Left(lastError.getOrElse(new RuntimeException("No attempts remaining")))
      } else {
        try {
          Right(f(input))
        } catch {
          case e: Throwable => retryHelper(attemptsLeft - 1, Some(e))
        }
      }
    }
    retryHelper(maxAttempts, None)
  }

  /**
   * Conditional combinator that applies different functions based on a predicate.
   *
   * This combinator provides branching logic in a functional style, allowing
   * different transformations based on input conditions.
   *
   * Example usage:
   * {{{
   * val isPositive = (x: Int) => x > 0
   * val negate = (x: Int) => -x
   * val identity = (x: Int) => x
   * val result = FPUtils.conditional(isPositive, negate, identity)(5) // -5
   * }}}
   *
   * @tparam A The input/output type
   * @param predicate The condition to test
   * @param ifTrue Function to apply if predicate is true
   * @param ifFalse Function to apply if predicate is false
   * @param input The input value
   * @return The result of applying the appropriate function
   */
  def conditional[A](predicate: A => Boolean, ifTrue: A => A, ifFalse: A => A)(input: A): A = {
    if (predicate(input)) ifTrue(input) else ifFalse(input)
  }

  /**
   * Pipeline combinator that chains multiple functions in sequence.
   *
   * This combinator allows building complex transformations by composing
   * a sequence of functions, applying them from left to right.
   *
   * Example usage:
   * {{{
   * val addOne = (x: Int) => x + 1
   * val multiplyTwo = (x: Int) => x * 2
   * val toString = (x: Int) => x.toString
   * val pipeline = List(addOne, multiplyTwo)
   * val result = FPUtils.pipeline(pipeline)(5) // ((5 + 1) * 2) = 12
   * }}}
   *
   * @tparam A The input/output type (functions must have same input/output type)
   * @param functions The list of functions to apply in sequence
   * @param input The initial input value
   * @return The result after applying all functions in sequence
   */
  def pipeline[A](functions: List[A => A])(input: A): A = {
    @tailrec
    def pipelineHelper(remaining: List[A => A], current: A): A = {
      remaining match {
        case Nil => current
        case head :: tail => pipelineHelper(tail, head(current))
      }
    }
    pipelineHelper(functions, input)
  }
}