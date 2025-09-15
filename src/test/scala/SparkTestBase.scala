import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}

/**
 * Base trait for Spark-based tests providing common setup and utilities.
 *
 * Provides:
 * - Automatic SparkSession lifecycle management
 * - Temporary directory creation and cleanup
 * - Utility methods for test file creation
 */
trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>

  var spark: SparkSession = _
  var tempDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Convert class name from XxxSpec to XxxTest format for consistency with original tests
    val appName = getClass.getSimpleName.replace("Spec", "Test")
    spark = SparkBootstrap.session(appName)
    tempDir = Files.createTempDirectory(s"${getClass.getSimpleName}-test")
  }

  override def afterAll(): Unit = {
    if (tempDir != null) {
      deleteRecursively(tempDir.toFile)
    }
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  /**
   * Creates a temporary file with specified content.
   *
   * @param content The content to write to the file
   * @param filename The name of the file to create
   * @return Absolute path to the created file
   */
  def createTempFile(content: String, filename: String): String = {
    val file = new File(tempDir.toFile, filename)
    val writer = new PrintWriter(file)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    file.getAbsolutePath
  }

  /**
   * Recursively deletes a directory and all its contents.
   *
   * @param f The file or directory to delete
   */
  def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) {
      f.listFiles().foreach(deleteRecursively)
    }
    f.delete()
  }
}