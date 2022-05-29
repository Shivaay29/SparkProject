import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RDDBroadcast extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession.builder().master("local[*]").appName("RDDBroadcast").getOrCreate()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._
  import spark.sql
  val states=Map(("NY","New York"),("CA","Califormina"),("FL","Florida"))
  val Countries=Map(("USA","United State of America"),("IN","India"),("UK","United Kingdom"))
  val broadcaststates=sc.broadcast(states)
  val broadcastcountry=sc.broadcast(Countries)
  val data=Seq(("James","Smith","USA","CA"), ("Michael","Rose","USA","NY"), ("Robert","Williams","USA","CA"),("Maria","Jones","USA","FL"))
  val rdd=sc.parallelize(data)
  val rdd2=rdd.map(x=>{
    val state = x._4
    val country = x._3
    val fullcounty = broadcastcountry.value.get(country).get
    val fullstate = broadcaststates.value.get(state).get
    (x._1,x._2,fullcounty,fullstate)
  })
  println(rdd2.collect().mkString(","))
}
