import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test suite for the Main application object.
 * 
 * This test class verifies that the application can bootstrap successfully
 * and execute without throwing exceptions.
 */
class MainSpec extends AnyFlatSpec with Matchers {

  /**
   * Test that verifies the Main object can be instantiated and executed.
   * 
   * This test ensures basic project functionality and that all dependencies
   * are correctly configured for the application lifecycle.
   */
  "Main object" should "execute without throwing exceptions" in {
    noException should be thrownBy {
      Main.main()
    }
  }

  /**
   * Test that verifies a trivial pure function for project validation.
   * 
   * @return the sum of two integers
   */
  def addNumbers(a: Int, b: Int): Int = a + b

  "addNumbers function" should "correctly add two positive numbers" in {
    addNumbers(2, 3) should equal(5)
  }

  it should "correctly add negative numbers" in {
    addNumbers(-2, -3) should equal(-5)
  }

  it should "correctly add zero" in {
    addNumbers(5, 0) should equal(5)
  }
}