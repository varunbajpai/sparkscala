import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

//See the difference between prototyping and actual app that will be used for spark-submit
object WordCountTestInProduction{
  
  def main(args: Array[String]) {
        
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sc = new SparkContext()
      val input = sc.textFile(args(0))
      val lowerwords = input.flatMap(_.split(" ")).map(_.toLowerCase())
      val wordmap = lowerwords.map((_,1))
      val wordmap2 = wordmap.reduceByKey(_+_)
      val wordmaprev = wordmap2.map(x=> (x._2,x._1))
      val finalcount = wordmaprev.sortByKey(false).map(x=> (x._2,x._1))
      finalcount.collect.foreach(println)
      finalcount.saveAsTextFile("wcresult.txt")

  }
}