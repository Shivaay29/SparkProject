import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object StructData extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession.builder().master("local[*]").appName("StructData").getOrCreate()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits
  import spark.sql
  val studentrdd=sc.parallelize(Array(Row(1,"Maths",400),Row(2,"Hindi",402),Row(3,"English",410)))
  studentrdd.collect.foreach(println)
  val schema008=StructType(
    Array(
      StructField("Rollno",IntegerType,true),
      StructField("Subject",StringType,true),
      StructField("Marks",IntegerType,true)
    )
  )
  val studentdf=spark.createDataFrame(studentrdd,schema008)
  studentdf.printSchema()
  studentdf.show(false)
}
