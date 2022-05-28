import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object toDF {
  def main(args:Array[String]) ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[*]").appName("toDF").getOrCreate()
    val sc=spark.sparkContext
    sc.setLogLevel("Error")
    import spark.implicits._
    val column=Seq("Seqno","Quote")
    val data=Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy."))
    val df=data.toDF(column:_*)
    df.show(false)
    println("Print the num partitions :"+df.rdd.getNumPartitions)
    spark.stop()
  }
}
