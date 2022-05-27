import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object count {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[*]").appName("count").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val rdd=sc.textFile("C:\\Users\\user\\Desktop\\Shared_Folder\\test.txt")
    val flatmat=rdd.flatMap(x=>x.split(" "))
    val map=flatmat.map(x=>(x,1))
    val reducef=map.reduceByKey(_+_)
    reducef.foreach(println)

    spark.stop()
  }
}
