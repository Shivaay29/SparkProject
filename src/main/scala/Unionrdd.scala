import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object Unionrdd extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","Unionrdd")
  sc.setLogLevel("ERROR")
  val rdd4=sc.parallelize(List(1,2,3,4,5))
  val rdd5=sc.parallelize(List(4,5))
  val unioun=rdd4.union(rdd5)
  println("Union os the rdd is :")
  unioun.collect.foreach(println)
  val intersection=rdd4.intersection(rdd5)
  println("intersection os the rdd is :")
  intersection.collect.foreach(println)
  val subtract=rdd4.subtract(rdd5)
  println("subtract os the rdd is :")
  subtract.collect.foreach(println)
  }

