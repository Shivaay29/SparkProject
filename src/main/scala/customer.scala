import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object customer extends App {
   Logger.getLogger("org").setLevel(Level.ERROR)
   val spark = SparkSession.builder.master("local[*]").appName("customer").getOrCreate()
   val sc = spark.sparkContext
   sc.setLogLevel("ERROR")
   val customerrdd = sc.textFile("C:\\Users\\user\\Desktop\\Shared_Folder\\products-detail.csv")
  //customerrdd.collect.foreach(println)
   val mapcustomer = customerrdd.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  //mapcustomer.collect.foreach(println)
  val customreducef = mapcustomer.reduceByKey(_+_)
  //customreducef.collect.foreach(println)
  val sortedcust= customreducef.sortBy(x=>x._2,false)
  sortedcust.collect.foreach(println)


}
