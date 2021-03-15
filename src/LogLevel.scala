import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object LogLevel extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val myList = List("WARN: tue 4 sept 0405",
      "ERROR: tue sept 0408",
      "ERROR: tue sept 0408",
      "ERROR: tue sept 0408",
      "ERROR: tue sept 0408",
      "ERROR: tue sept 0408")
      
  val originalLogsRdd = sc.parallelize(myList).
  map(x=> x.split(":")).
  map(x=>(x(0),1)).
  reduceByKey((x,y)  => x+y).
  collect().
  foreach(println)
  
}