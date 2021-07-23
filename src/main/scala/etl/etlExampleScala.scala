import com.crealytics.spark.excel._
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object etlExampleScalaApp extends App with SparkSessionProvider {

  val resourcePath: String = "src/main/resources/"
  val filepath: String     = "src/main/data/"

  def readExcel(file: String): DataFrame = spark.read
    .excel(
      header = true,
      treatEmptyValuesAsNulls = true, 
      addColorColumns = false
    )
    .load(file)

  def toCsv(name: String)(df: DataFrame): DataFrame = {

    df.coalesce(1).write.format("csv").option("header","true").save(filepath + name)

    val hadoopConfig = new Configuration()
    val hdfs         = FileSystem.get(hadoopConfig)
    val srcPath      = new Path(filepath + name)
    val destPath     = new Path(filepath + name + ".csv")
    FileUtil.copyMerge(hdfs, srcPath, hdfs, destPath, true, hadoopConfig, null)
    hdfs.delete(new Path("." + name + ".csv.crc"), true)

    df
  }

  def staffLayout(df: DataFrame): DataFrame =
    df.select(
      "Estado Civil",
      "Data de Contratacao",
      "Data de Demissao",
    )

  val staff = readExcel(resourcePath + "BaseFuncionarios.xlsx")


  staff
  .transform(staffLayout)
  .transform(toCsv("staff"))

}
