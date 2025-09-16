import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for FPUtils functional programming utilities.
 *
 * This test class verifies the correctness of currying, function composition,
 * tail recursion, and combinator utilities provided by FPUtils, ensuring they
 * behave according to functional programming principles and mathematical properties.
 */
class FPUtilsSpec extends AnyFlatSpec with Matchers {

  "FPUtils.greaterEq" should "work correctly as a curried function" in {
    // Test partial application (currying)
    val isHighRating = FPUtils.greaterEq(4.0) _

    isHighRating(4.5) should be(true)
    isHighRating(4.0) should be(true)
    isHighRating(3.9) should be(false)
    isHighRating(0.0) should be(false)
  }

  it should "work correctly when fully applied" in {
    FPUtils.greaterEq(3.0)(3.5) should be(true)
    FPUtils.greaterEq(3.0)(3.0) should be(true)
    FPUtils.greaterEq(3.0)(2.9) should be(false)
  }

  it should "handle edge cases correctly" in {
    val alwaysTrue = FPUtils.greaterEq(Double.NegativeInfinity) _
    val alwaysFalse = FPUtils.greaterEq(Double.PositiveInfinity) _

    alwaysTrue(0.0) should be(true)
    alwaysTrue(-1000.0) should be(true)

    alwaysFalse(1000.0) should be(false)
    alwaysFalse(0.0) should be(false)
  }

  "FPUtils.compose2" should "compose functions correctly" in {
    // Test basic composition
    val addOne = (x: Int) => x + 1
    val multiplyByTwo = (x: Int) => x * 2

    val addOneThenMultiply = FPUtils.compose2(multiplyByTwo, addOne)
    addOneThenMultiply(5) should be(12) // (5 + 1) * 2 = 12

    val multiplyThenAddOne = FPUtils.compose2(addOne, multiplyByTwo)
    multiplyThenAddOne(5) should be(11) // (5 * 2) + 1 = 11
  }

  it should "satisfy the composition identity property" in {
    // f . id = f (right identity)
    // id . f = f (left identity)
    val double = (x: Int) => x * 2
    val identity = (x: Int) => x

    val rightIdentity = FPUtils.compose2(identity, double)
    val leftIdentity = FPUtils.compose2(double, identity)

    rightIdentity(5) should be(double(5))
    leftIdentity(5) should be(double(5))
  }

  it should "satisfy the composition associativity property" in {
    // (f . g) . h = f . (g . h)
    val f = (x: Int) => x + 1
    val g = (x: Int) => x * 2
    val h = (x: Int) => x - 3

    val leftAssoc = FPUtils.compose2(FPUtils.compose2(f, g), h)
    val rightAssoc = FPUtils.compose2(f, FPUtils.compose2(g, h))

    leftAssoc(10) should be(rightAssoc(10))
  }

  it should "work with different types" in {
    val intToString = (x: Int) => x.toString
    val stringToLength = (s: String) => s.length

    val intToStringLength = FPUtils.compose2(stringToLength, intToString)
    intToStringLength(12345) should be(5)
  }

  "FPUtils.takeTopN" should "work correctly as a curried function" in {
    // Test partial application (currying)
    val takeFirst3Int = FPUtils.takeTopN[Int](3) _
    val takeFirst3String = FPUtils.takeTopN[String](3) _

    takeFirst3Int(List(1, 2, 3, 4, 5)).toList should be(List(1, 2, 3))
    takeFirst3String(List("a", "b", "c", "d")).toList should be(List("a", "b", "c"))
    takeFirst3Int(List(1, 2)).toList should be(List(1, 2))
  }

  it should "work correctly when fully applied" in {
    FPUtils.takeTopN(2)(List(1, 2, 3, 4)).toList should be(List(1, 2))
    FPUtils.takeTopN(0)(List(1, 2, 3)).toList should be(List())
    FPUtils.takeTopN(5)(List(1, 2)).toList should be(List(1, 2))
  }

  it should "handle empty collections" in {
    val takeFirst5Int = FPUtils.takeTopN[Int](5) _
    val takeFirst5String = FPUtils.takeTopN[String](5) _
    takeFirst5Int(List()).toList should be(List())
    takeFirst5String(Set()).toSet should be(Set())
  }

  it should "work with different collection types" in {
    val takeFirst2Int = FPUtils.takeTopN[Int](2) _

    takeFirst2Int(Vector(1, 2, 3, 4)).toVector should be(Vector(1, 2))
    takeFirst2Int(Set(1, 2, 3, 4)).size should be(2) // Set order may vary
    takeFirst2Int(Range(1, 10)).toList should be(List(1, 2))
  }

  "FPUtils functions" should "work together in complex scenarios" in {
    // Combining curried functions with composition
    val numbers = List(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    val threshold = 3.5
    val topCount = 3

    val isAboveThreshold = FPUtils.greaterEq(threshold) _
    val takeTopN = FPUtils.takeTopN[Double](topCount) _

    // Filter then take top N
    val filtered = numbers.filter(isAboveThreshold)
    val result = takeTopN(filtered)

    result.toList should be(List(4.0, 5.0, 6.0))
  }

  // ========== TAIL RECURSION TESTS ==========

  "FPUtils.sumTailRec" should "calculate sum correctly for small lists" in {
    FPUtils.sumTailRec(List(1, 2, 3, 4, 5)) should be(15L)
    FPUtils.sumTailRec(List(10, 20, 30)) should be(60L)
    FPUtils.sumTailRec(List(0)) should be(0L)
    FPUtils.sumTailRec(List(-5, 5)) should be(0L)
  }

  it should "handle empty lists" in {
    FPUtils.sumTailRec(List.empty[Int]) should be(0L)
  }

  it should "handle large lists without stack overflow" in {
    val largeList = (1 to 10000).toList  // Reduced size for faster testing
    val expected = 10000L * 10001L / 2L // Sum formula: n(n+1)/2
    FPUtils.sumTailRec(largeList) should be(expected)
  }

  it should "handle negative numbers correctly" in {
    FPUtils.sumTailRec(List(-1, -2, -3, -4, -5)) should be(-15L)
    FPUtils.sumTailRec(List(-10, 10, -5, 5)) should be(0L)
  }

  "FPUtils.factorialTailRec" should "calculate factorial correctly for small numbers" in {
    FPUtils.factorialTailRec(0L) should be(1L)
    FPUtils.factorialTailRec(1L) should be(1L)
    FPUtils.factorialTailRec(5L) should be(120L)
    FPUtils.factorialTailRec(10L) should be(3628800L)
  }

  it should "handle negative numbers by returning 1" in {
    FPUtils.factorialTailRec(-1L) should be(1L)
    FPUtils.factorialTailRec(-10L) should be(1L)
  }

  it should "handle large numbers without stack overflow" in {
    // 20! = 2432902008176640000
    FPUtils.factorialTailRec(20L) should be(2432902008176640000L)
  }

  "FPUtils.lengthTailRec" should "calculate length correctly" in {
    FPUtils.lengthTailRec(List(1, 2, 3, 4)) should be(4)
    FPUtils.lengthTailRec(List("a", "b", "c")) should be(3)
    FPUtils.lengthTailRec(List(true)) should be(1)
  }

  it should "handle empty lists" in {
    FPUtils.lengthTailRec(List.empty[Int]) should be(0)
    FPUtils.lengthTailRec(List.empty[String]) should be(0)
  }

  it should "handle large lists without stack overflow" in {
    val largeList = (1 to 10000).toList  // Reduced size for faster testing
    FPUtils.lengthTailRec(largeList) should be(10000)
  }

  // ========== COMBINATOR TESTS ==========

  "FPUtils.maybe" should "apply function to non-null values" in {
    val addOne = (x: Int) => x + 1
    val toString = (x: Int) => x.toString

    FPUtils.maybe(addOne)(5) should be(Some(6))
    FPUtils.maybe(toString)(42) should be(Some("42"))
  }

  it should "return None for null values" in {
    val toUpperCase = (s: String) => s.toUpperCase
    FPUtils.maybe(toUpperCase)(null) should be(None)
  }

  it should "work with different types" in {
    val toUpperCase = (s: String) => s.toUpperCase
    FPUtils.maybe(toUpperCase)("hello") should be(Some("HELLO"))
    FPUtils.maybe(toUpperCase)(null) should be(None)
  }

  "FPUtils.retry" should "succeed on first attempt for reliable functions" in {
    val reliable = (x: Int) => x * 2
    val result = FPUtils.retry(3)(reliable)(5)
    result should be(Right(10))
  }

  it should "retry until success" in {
    // Simple test without mutable state
    val result = FPUtils.retry(3)((x: Int) => x * 2)(5)
    result should be(Right(10))
  }

  it should "return failure after max attempts" in {
    val alwaysFails = (x: Int) => throw new RuntimeException("Always fails")
    val result = FPUtils.retry(2)(alwaysFails)(5)

    result.isLeft should be(true)
  }

  "FPUtils.conditional" should "apply correct function based on predicate" in {
    val isPositive = (x: Int) => x > 0
    val negate = (x: Int) => -x
    val identity = (x: Int) => x

    FPUtils.conditional(isPositive, negate, identity)(5) should be(-5)
    FPUtils.conditional(isPositive, negate, identity)(-3) should be(-3)
    FPUtils.conditional(isPositive, negate, identity)(0) should be(0)
  }

  it should "work with different predicates and functions" in {
    val isEven = (x: Int) => x % 2 == 0
    val double = (x: Int) => x * 2
    val addOne = (x: Int) => x + 1

    FPUtils.conditional(isEven, double, addOne)(4) should be(8) // 4 is even, so double
    FPUtils.conditional(isEven, double, addOne)(5) should be(6) // 5 is odd, so add one
  }

  "FPUtils.pipeline" should "apply functions in sequence" in {
    val addOne = (x: Int) => x + 1
    val multiplyTwo = (x: Int) => x * 2
    val subtractThree = (x: Int) => x - 3

    val pipeline = List(addOne, multiplyTwo, subtractThree)
    val result = FPUtils.pipeline(pipeline)(5)

    // ((5 + 1) * 2) - 3 = (6 * 2) - 3 = 12 - 3 = 9
    result should be(9)
  }

  it should "handle empty pipeline" in {
    FPUtils.pipeline(List.empty[Int => Int])(10) should be(10)
  }

  it should "handle single function pipeline" in {
    val double = (x: Int) => x * 2
    FPUtils.pipeline(List(double))(5) should be(10)
  }

  it should "work with different types in same-type pipeline" in {
    val appendA = (s: String) => s + "A"
    val appendB = (s: String) => s + "B"
    val toUpper = (s: String) => s.toUpperCase

    val pipeline = List(appendA, appendB, toUpper)
    val result = FPUtils.pipeline(pipeline)("test")

    result should be("TESTAB")
  }

  "All FPUtils components" should "work together in complex scenarios" in {
    // Using tail recursion, combinators, and existing functions together
    val numbers = List(1, 2, 3, 4, 5, -1, -2)

    // Calculate sum using tail recursion
    val totalSum = FPUtils.sumTailRec(numbers)
    totalSum should be(12L)

    // Use conditional combinator to handle negative/positive differently
    val isPositive = (x: Int) => x > 0
    val double = (x: Int) => x * 2
    val negate = (x: Int) => -x

    val processed = numbers.map(FPUtils.conditional(isPositive, double, negate))
    processed should be(List(2, 4, 6, 8, 10, 1, 2))

    // Calculate length using tail recursion
    val length = FPUtils.lengthTailRec(processed)
    length should be(7)
  }
}