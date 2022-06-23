import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object rdd2 {
  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder.master("local[*]").appName("rdd2").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")
    val rdd=sc.textFile("C:\\Users\\user\\Desktop\\Shared_Folder\\test.txt")
    val flatmapf=rdd.flatMap(x=>x.split(" ")).take(20)
    val mapf=rdd.map(x=>(x,1))
    val reducef=mapf.reduceByKey(_+_)
    reducef.foreach(println)

  }
}