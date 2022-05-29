import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RDD_Broadcast extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[*]").appName("RDD_Broadcast").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val states=Map(("DL","Delhi"),("CL","Columbo"),("ML","Melbourne"),("AUK","AUKLAND"))
    val countries=Map(("IN","INDIA"),("SL","Sri Lanka"),("Aus","Australia"),("NZ","New Zealand"))
    val broadcaststates=sc.broadcast(states)
    val broadcountries=sc.broadcast(countries)
    val data=Seq(("Rohit","Sharma","IN","DL"),("Thisara","Parera","SL","CL"),("David","Warner","Aus","ML"),("Kane","Williamson","NZ","AUK"))
    val rdd=sc.parallelize(data)
    val rdd2=rdd.map(x=>{
      val state=x._4
      val country=x._3
      val fullcountry=broadcountries.value.get(country).get
      val fullstates=broadcaststates.value.get(state).get
      (x._1,x._2,fullcountry,fullstates)
    })
    println(rdd2.collect().mkString(","))

    spark.stop()
}
