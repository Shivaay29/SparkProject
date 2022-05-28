import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DFcaseclass extends App {
Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession.builder().master("local[*]").appName("DFcaseclass").getOrCreate()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._
  import spark.sql
  case class Movies(Rank:Int,Fav_Movie:String,Fav_Actor:String,Rel_year:Int)
  val favmovrdd=sc.textFile("C:\\Users\\user\\Desktop\\Shared_Folder\\favmovies.csv")
  favmovrdd.collect.foreach(println)
  val favmovdf=favmovrdd.map(x=>x.split(",")).map(x=>Movies(x(0).toInt,x(1),x(2),x(3).toInt)).toDF
  favmovdf
  favmovdf.printSchema()
  favmovdf.show()

}
