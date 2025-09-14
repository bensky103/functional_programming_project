import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * Bootstrap utilities for Apache Spark session initialization.
 * 
 * This object provides factory methods to create properly configured SparkSession
 * instances with optimized settings for local development and testing.
 */
object SparkBootstrap {

  /**
   * Creates a configured SparkSession for local execution.
   * 
   * This method initializes a SparkSession with local[*] master configuration,
   * reduced logging verbosity, and other optimizations suitable for development
   * and testing environments.
   * 
   * @param appName The name of the Spark application (default: "HIT-FP-Spark")
   * @return A configured SparkSession instance
   */
  def session(appName: String = "HIT-FP-Spark"): SparkSession = {
    println(s"DEBUG: Initializing Spark session with appName: $appName")

    // Handle Windows Hadoop compatibility issues
    if (System.getProperty("os.name").toLowerCase.contains("windows")) {
      println("DEBUG: Windows detected - setting Hadoop home directory for compatibility")
      System.setProperty("hadoop.home.dir", System.getProperty("java.io.tmpdir"))
    }

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      // Additional Windows-specific settings
      .set("spark.sql.execution.arrow.pyspark.enabled", "false")

    // Reduce log verbosity
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    println("DEBUG: Spark session initialized successfully")
    println(s"DEBUG: Master: ${spark.sparkContext.master}")
    println(s"DEBUG: App ID: ${spark.sparkContext.applicationId}")
    
    spark
  }
}