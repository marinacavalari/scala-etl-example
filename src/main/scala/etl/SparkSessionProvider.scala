import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  lazy val spark: SparkSession = {
    val numberCpus = sys.env.getOrElse("NUM_CPUS", "*")
    val availableMemoryMaybe = sys.env.get("AVAILABLE_MEMORY_MB")

    val builder = SparkSession
      .builder()
      .master(s"local[$numberCpus]")
      .appName("nestle")

    availableMemoryMaybe.foreach { availableMemory =>
      builder.config("spark.driver.memory", s"{availableMemory}m")
    }
    builder.getOrCreate()
  }

}