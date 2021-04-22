import com.crealytics.spark.excel._
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object NestleApp extends App with SparkSessionProvider {

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

  def layoutBase(df: DataFrame): DataFrame =
    df.select(
      "Cliente",
      "Valor Contrato Anual",
      "Quantidade de Serviços",
      "Cargo Responsável",
      "CEP",
      "Data Início Contrato",
      "Cargo",
      "Área",
      "COD Área",
      "COD Nível",
      "Quadro",
      "Bonus",
      "Contratacao",
      "Descrição Nível",
      "Tempo no Nível",
      "Plano de Carreira"
    )

  def staffLayout(df: DataFrame): DataFrame =
    df.select(
      "Estado Civil",
      "Data de Contratacao",
      "Data de Demissao",
      "Salario Base",
      "Beneficios",
      "Área",
      "COD Nível",
      "Quadro",
      "Bonus",
      "Contratacao",
      "Descrição Nível",
      "Tempo no Nível",
      "Setor Responsável",
      "Plano de Carreira"
    )
  val cargos       = readExcel(resourcePath + "BaseCargos.xlsx")
  val cep          = readExcel(resourcePath + "BaseCEP.xlsx")
  val clientes     = readExcel(resourcePath + "BaseClientes.xlsx")
  val funcionarios = readExcel(resourcePath + "BaseFuncionarios.xlsx")
  val nivel        = readExcel(resourcePath + "BaseN6vel.xlsx")

  val demografico = clientes.join(cep, clientes("CEP") === cep("CEP"), "left")
  val staff = funcionarios
    .join(cargos, funcionarios("Cargo") === cargos("Cargo"), "left")
    .join(nivel, cargos("COD Nível") === nivel("Nível"), "left")

  val base_remuneracao = clientes
    .join(cargos, clientes("Cargo Responsável") === cargos("Cargo"), "left")
    .join(nivel, cargos("COD Nível") === nivel("Nível"), "left")

  staff.transform(staffLayout).transform(toCsv("staff"))
  base_remuneracao.transform(layoutBase).transform(toCsv("base_remuneracao"))
}
