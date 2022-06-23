import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Casechange extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","Casechange")
  val rdd=sc.textFile("C:\\Users\\user\\Desktop\\Shared_Folder\\hello.txt")
  // import spark.implicits._
  rdd.collect.foreach(println)
  val lowercase=rdd.map(x=>x.toLowerCase())
  val uppercase=rdd.map(x=>x.toUpperCase())
  val flatmap=lowercase.flatMap(x=>x.split(" "))
  val mapf=flatmap.map(x=>(x,1))
  val reducef=mapf.reduceByKey(_+_)
  reducef.collect.foreach(println)
  // reducef.saveAsTextFile("C:\\Users\\user\\Desktop\\Shared_Folder\\new")
  //scala.io.StdIn.readLine()
  }

