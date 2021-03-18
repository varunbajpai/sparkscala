import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataGrouping extends App{
  case class Logging(level:String,datetime:String)
  
  def mapper(line:String): Logging = {
    val fields = line.split(',')
    val logging:Logging = Logging(fields(0),fields(1))
    return logging
  }
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
    .set("spark.app.name", "My Application 1")
    .set("spark.master","local[2]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
 import spark.implicits._
//val myList = List("DEBUG,2015-2-6 16:24:07",
//"INFO,2016-7-26 18:54:43",
//"INFO,2012-7-18 14:35:19",
//"DEBUG,2012-4-26 14:26:50",
//"DEBUG,2013-9-28 20:27:13",
//"INFO,2017-8-20 13:17:27",
//"INFO,2015-4-13 09:28:17")
  
 val myDf = spark.read
              .format("csv")
              .option("header",true)
              .option("InferSchema",true)
              .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/biglog.txt")
              .load()


//val rdd1 = spark.sparkContext.parallelize(myList)
//val rdd2 = rdd1.map(mapper)
//val df1 = rdd2.toDF()
//df1.show()


myDf.createOrReplaceTempView("logging_table")

//spark.sql("select * from logging_table").show()

//spark.sql("select level,count(datetime) from logging_table group by level order by level").show(false)

val monthDf = spark.sql("select level,date_format(datetime,'MMMM') as month,date_format(datetime,'M') as month_number from logging_table")
monthDf.show()
monthDf.createOrReplaceTempView("new_logging_table")

spark.sql("select level,date_format(datetime,'MMMM') as month,cast(first(date_format(datetime,'M')) as int) as monthnum,count(1) from logging_table group by level, month order by monthnum,level").show(100)

//results.createOrReplaceTempView("resultstable")

spark.stop()
}