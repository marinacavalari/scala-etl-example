name := "ScalaEtlExample"
version := "1.0"
scalaVersion := "2.12.10"
val sparkVersion = "3.0.0-preview2"
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
"com.crealytics" % "spark-excel_2.12" % "0.13.7" )
