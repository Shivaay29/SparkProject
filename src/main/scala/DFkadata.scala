import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object DFkadata extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spar=SparkSession.builder().master("local[*]").appName("DFkadata ").getOrCreate()
  val sc=spar.sparkContext
  sc.setLogLevel("Error")
  import spar.implicits._
  val pehlardd=sc.parallelize(1 to 10).map(x=>(x,"DF ka Data"))
  pehlardd.collect.foreach(println)
  //val df=Seq("Sno.","Simple String")
  val pehladf=pehlardd.toDF("Sno.","Simple String")
  pehladf.show()

}
