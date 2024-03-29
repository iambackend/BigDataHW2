name := "IBD-HW2"
version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.4"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)
mainClass in(Compile, packageBin) := Some("Main")
