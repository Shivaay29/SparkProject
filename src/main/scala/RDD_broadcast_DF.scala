import RDD_Broadcast.sc
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RDD_broadcast_DF extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[*]").appName("RDD_broadcast_DF").getOrCreate()
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
  val column=Seq("Cricketer First_Name","Cricketer Last Name","County","State")
  val df = data.toDF(column:_*)
  df.show(false)
  val df2=df.map(x=>{
    val state=x.getString(3)
    val country=x.getString(2)
    val fullcountry=broadcountries.value.get(country).get
    val fullstate=broadcaststates.value.get(state).get
    (x.getString(0),x.getString(1),fullcountry,fullstate)
  }).toDF(column:_*)
  df2.printSchema()
  df2.show(false)
    spark.stop()
}
