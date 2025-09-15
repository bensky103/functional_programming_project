# Code Review - HIT Functional Programming Course

**Date:** September 14, 2025
**Reviewer:** Claude (Scala/Spark TA)
**Project:** HIT-FP-Spark-Project

---

## 🎯 Executive Summary

**Current Grade:** B+ → **Potential Grade:** A (after fixes)

Project demonstrates solid functional programming foundations with comprehensive documentation and testing. **6 critical issues** identified that need resolution to achieve exemplary quality.

---

## ❌ CRITICAL ISSUES TO FIX

### **ISSUE #1: Empty Resources Directory**
- **Location:** `src/main/resources/`
- **Problem:** Empty directory serves no purpose
- **Fix:** Remove empty directory
```bash
rmdir src/main/resources
```

### **ISSUE #2: Inefficient Import Statement**
- **File:** `src/main/scala/DatasetIngestion.scala:2`
- **Current:** `import org.apache.spark.sql.types._`
- **Problem:** Imports entire types package when only specific types are used
- **Fix:** Replace with specific imports:
```scala
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, DoubleType, LongType}
```

### **ISSUE #3: Duplicate Test Setup Pattern (CRITICAL)**
- **Files:** 8 test files have identical Spark session setup/teardown code
  - `JoinStageSpec.scala:17-27`
  - `ValidatedLoaderSpec.scala:22-32`
  - `TransformStage1Spec.scala:17-27`
  - `TransformStage2Spec.scala:17-27`
  - `DriverSpec.scala:29-43`
  - `AdvancedFPAuditSpec.scala:25-35`
  - `SparkComponentsSpec.scala:16-26`
- **Problem:** Major code duplication violates DRY principle
- **Fix:** Create `SparkTestBase` trait

**CREATE FILE:** `src/test/scala/SparkTestBase.scala`
```scala
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
    spark = SparkBootstrap.session(getClass.getSimpleName)
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
```

**THEN UPDATE ALL TEST FILES:** Replace `BeforeAndAfterAll` with `SparkTestBase` and remove duplicated setup code:

**Example for `JoinStageSpec.scala`:**
```scala
// BEFORE:
class JoinStageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkBootstrap.session("JoinStageTest")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }
  // ... rest of class

// AFTER:
class JoinStageSpec extends AnyFlatSpec with Matchers with SparkTestBase {
  // Remove all setup/teardown code - it's now handled by SparkTestBase
  // ... rest of class remains the same
```

**Apply this pattern to ALL 8 test files.**

### **ISSUE #4: Non-Functional Error Handling**
- **File:** `src/main/scala/RowParsers.scala`
- **Lines:** 29, 30, 37, 40, 41, 53, 80, 81, 87, 88, 96, 103, 105, 106, 112, 113
- **Problem:** Using `throw` exceptions instead of functional error handling within functions that return `Either`
- **Fix:** Replace throw statements with Try wrapping:

**CURRENT PATTERN:**
```scala
case null => throw new IllegalArgumentException("movieId cannot be null")
case other => throw new IllegalArgumentException(s"movieId must be numeric, got: ${other.getClass.getSimpleName}")
```

**FUNCTIONAL PATTERN:**
```scala
case null => Left("movieId cannot be null")
case other => Left(s"movieId must be numeric, got: ${other.getClass.getSimpleName}")
```

**Apply this to all throw statements in `RowParsers.scala`**

### **ISSUE #5: Missing Test Coverage**
- **Problem:** No dedicated test files for `DatasetIngestion.scala` and `Paths.scala`
- **Current Coverage:** 11 test files for 13 source files (85%)
- **Target Coverage:** 13 test files for 13 source files (100%)

**CREATE FILE:** `src/test/scala/DatasetIngestionSpec.scala`
```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetIngestionSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  "DatasetIngestion.loadMovies" should "load movies with correct schema" in {
    val moviesContent =
      """movieId,title,genres
        |1,Test Movie,Action|Comedy
        |2,Another Movie,Drama""".stripMargin

    val filePath = createTempFile(moviesContent, "test_movies.csv")
    val df = DatasetIngestion.loadMovies(spark, filePath)

    df.columns should contain allOf ("movieId", "title", "genres")
    df.count() should be(2)
  }

  "DatasetIngestion.loadRatings" should "load ratings with correct schema" in {
    val ratingsContent =
      """userId,movieId,rating,timestamp
        |1,1,4.5,1234567890
        |2,1,3.0,1234567891""".stripMargin

    val filePath = createTempFile(ratingsContent, "test_ratings.csv")
    val df = DatasetIngestion.loadRatings(spark, filePath)

    df.columns should contain allOf ("userId", "movieId", "rating", "timestamp")
    df.count() should be(2)
  }

  "DatasetIngestion.loadCSV" should "load generic CSV with custom options" in {
    val csvContent = "col1,col2\nvalue1,value2\nvalue3,value4"
    val filePath = createTempFile(csvContent, "test_generic.csv")

    val df = DatasetIngestion.loadCSV(spark, filePath, header = true, delimiter = ",", inferSchema = true)

    df.columns should contain allOf ("col1", "col2")
    df.count() should be(2)
  }
}
```

**CREATE FILE:** `src/test/scala/PathsSpec.scala`
```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PathsSpec extends AnyFlatSpec with Matchers {

  "Paths.ratingsPath" should "return default path when no system property set" in {
    System.clearProperty("ratings.path")
    Paths.ratingsPath should be("data/ratings.csv")
  }

  it should "return custom path when system property is set" in {
    System.setProperty("ratings.path", "/custom/ratings.csv")
    Paths.ratingsPath should be("/custom/ratings.csv")
    System.clearProperty("ratings.path") // cleanup
  }

  "Paths.moviesPath" should "return default path when no system property set" in {
    System.clearProperty("movies.path")
    Paths.moviesPath should be("data/movies.csv")
  }

  it should "return custom path when system property is set" in {
    System.setProperty("movies.path", "/custom/movies.csv")
    Paths.moviesPath should be("/custom/movies.csv")
    System.clearProperty("movies.path") // cleanup
  }

  "Paths.configureFromArgs" should "set system properties from command line arguments" in {
    val args = Array("--ratings-path=/test/ratings.csv", "--movies-path=/test/movies.csv")
    Paths.configureFromArgs(args)

    System.getProperty("ratings.path") should be("/test/ratings.csv")
    System.getProperty("movies.path") should be("/test/movies.csv")

    // cleanup
    System.clearProperty("ratings.path")
    System.clearProperty("movies.path")
  }

  "Paths.summary" should "return formatted configuration summary" in {
    System.clearProperty("ratings.path")
    System.clearProperty("movies.path")
    val summary = Paths.summary

    summary should include("data/ratings.csv")
    summary should include("data/movies.csv")
    summary should include("Path Configuration:")
  }
}
```

### **ISSUE #6: DatasetIngestion Exception Handling**
- **File:** `src/main/scala/DatasetIngestion.scala`
- **Lines:** 80, 138
- **Problem:** Re-throwing exceptions instead of functional error handling
- **Fix:** Return `Either[String, DataFrame]` instead of `DataFrame`:

**CURRENT:**
```scala
def loadMovies(spark: SparkSession, path: String): DataFrame = {
  try {
    // ... logic
  } catch {
    case e: Exception =>
      println(s"DEBUG: ERROR loading movies dataset: ${e.getMessage}")
      throw e
  }
}
```

**FUNCTIONAL:**
```scala
def loadMovies(spark: SparkSession, path: String): Either[String, DataFrame] = {
  Try {
    // ... existing logic
  } match {
    case Success(df) => Right(df)
    case Failure(e) =>
      println(s"DEBUG: ERROR loading movies dataset: ${e.getMessage}")
      Left(e.getMessage)
  }
}
```

**Note:** This change will require updating all callers of these methods to handle `Either` return types.

---

## 📋 IMPLEMENTATION CHECKLIST

**Priority Order:**

### 🔴 **HIGHEST PRIORITY**
- [ ] Create `SparkTestBase.scala` trait
- [ ] Update all 8 test files to extend `SparkTestBase`
- [ ] Remove duplicated setup/teardown code from test files

### 🟡 **HIGH PRIORITY**
- [ ] Fix error handling in `RowParsers.scala` (replace throws with Left returns)
- [ ] Fix error handling in `DatasetIngestion.scala` (return Either types)
- [ ] Update callers to handle new Either return types

### 🟢 **MEDIUM PRIORITY**
- [ ] Create `DatasetIngestionSpec.scala` test file
- [ ] Create `PathsSpec.scala` test file
- [ ] Run tests to ensure 100% pass rate

### 🔵 **LOW PRIORITY**
- [ ] Fix import statement in `DatasetIngestion.scala`
- [ ] Remove empty `src/main/resources/` directory

---

## ✅ VERIFICATION STEPS

After implementing all fixes:

1. **Run Tests:**
   ```bash
   sbt test
   ```

2. **Verify Coverage:**
   - Should have 13 test files for 13 source files
   - All tests should pass

3. **Verify No Code Duplication:**
   - Only one SparkSession setup pattern (in SparkTestBase)
   - No duplicated temp file management code

4. **Verify Functional Error Handling:**
   - No `throw` statements in main source code
   - All error handling uses Either/Try patterns

---

## 🎓 EXPECTED OUTCOME

**Before Fixes:** B+ (Good project with issues)
**After Fixes:** A (Exemplary functional programming implementation)

**Improvements:**
- ✅ Eliminates major code duplication
- ✅ Improves functional programming compliance
- ✅ Achieves 100% test coverage
- ✅ Follows clean code principles
- ✅ Maintains comprehensive documentation

---

**Total Estimated Time:** 2-3 hours
**Files to Modify:** 12 files
**Files to Create:** 3 new files