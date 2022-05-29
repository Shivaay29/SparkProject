import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DFcsv extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession.builder().master("local[*]").appName("DFcsv").getOrCreate()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._
  import spark.sql
  val moviesdf1=spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\user\\Desktop\\Shared_Folder\\favmovies1.csv")
  moviesdf1.printSchema()
  moviesdf1.show()
  val ticketdf1=spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\user\\Desktop\\Shared_Folder\\archive\\AnnualTicketSales.csv")
  ticketdf1.printSchema()
  ticketdf1.show()
  sc.stop()
}
