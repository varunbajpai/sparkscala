import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object LogLevelGrouping extends App {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","wordcount")
    val baseRdd = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/bigLog.txt")
    val mappedRdd = baseRdd.map(x => {
    val fields = x.split(":")
    (fields(0),1,fields(1))  
      })
//    val total = mappedRdd.reduceByKey((x,y) => (x+y))
    val filterrows = mappedRdd.filter(x=> x._3.length > 10)
//    filterrows.saveAsTextFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/out")
  
    scala.io.StdIn.readLine()
}