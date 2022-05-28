import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Partition {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Partition")
    val sc=new SparkContext(conf)
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //import spark.implicits._
    //import spark.sql
    val rdd=sc.parallelize(Range(1,20))
    println("println the number of partition available :"+rdd.getNumPartitions)
    val rdd1=sc.parallelize(Range(1,20),6)
    println("println the number of partition available after num Partition :"+rdd1.getNumPartitions)
    val rdd2=rdd1.repartition(7)
    println("println the number of partition available after re-Partition :"+rdd2.getNumPartitions)
    val rdd3=rdd1.coalesce(4)
    println("println the number of partition available after coalesce :"+rdd3.getNumPartitions)


    sc.stop()
  }
}

