import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
object StructDataframe extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession.builder().master("local[*]").appName("StructDataframe").getOrCreate()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._
  import spark.sql
  val studentrdd=sc.parallelize(Array(Row(1,"Maths",450),Row(2,"Hindi",402),Row(3,"English",420)))
  val schema007=StructType(
    Array(
      StructField("Rollno",IntegerType,true),
      StructField("Subject",StringType,true),
      StructField("Marks",IntegerType,true)
    ))
  studentrdd.collect.foreach(println)
  val studentdf = spark.createDataFrame(studentrdd,schema007)
  studentdf.printSchema()
  studentdf.show
spark.stop()
}
