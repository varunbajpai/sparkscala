import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object GoogleCampaign extends App{
  
 def loadBoringWords():Set[String] = {
   var boringWords:Set[String] = Set()
   val lines = Source.fromFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/boringwords").getLines()
   for (i <- lines){
     boringWords = boringWords + i
     }
   boringWords
 } 
  
  
 def parseline(line: String) = {
    val linesplit = line.split(",")
    (linesplit(0).toLowerCase(), linesplit(10).toFloat)
  }
 
 def eval(key: String,  value: Float) = {
   var ans : List[(String,Float)] = List()
   val keys = key.split(" ")
   for(i <- keys){
     ans = ans:+((i,value))
     }
   ans
 }
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  val boring_words = loadBoringWords()
  print(boring_words)
  val sc = new SparkContext("local[*]","wordcount")
  var nameset = sc.broadcast(boring_words)
  val input = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/bigdatacampaigndata.csv")
  
  val mapped = input.map(parseline)
  val mapped_new = mapped.flatMap(x => eval(x._1,x._2))
  val filterRdd = mapped_new.filter(x => !nameset.value(x._1))
  val final_maped = filterRdd.reduceByKey((x,y)=>(x+y))
  
  val ans = final_maped.sortBy(x=> x._2,false)  
  ans.collect.foreach(println)
  
}