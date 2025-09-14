import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for FPUtils functional programming utilities.
 *
 * This test class verifies the correctness of currying and function composition
 * utilities provided by FPUtils, ensuring they behave according to functional
 * programming principles and mathematical properties.
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
}