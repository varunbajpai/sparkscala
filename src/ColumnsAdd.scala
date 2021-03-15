
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, explode, udf, expr}
import org.apache.spark.sql.Dataset

case class Person(name:String,age:Int,city:String)
object ColumnsAdd extends App {
  def ageCheck(age:Int):String = {
    if(age>18) "Y" else "N"
  }
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

//  +-------+---+---------+
//|    _c0|_c1|      _c2|
//+-------+---+---------+
//|  sumit| 30|bangalore|
//|  kapil| 32|hyderabad|
//|sathish| 16|  chennai|
//|   ravi| 39|bangalore|
//| kavita| 12|hyderabad|
//|  kavya| 19|   mysore|
//+-------+---+---------+
 
   val df = spark.read
                      .format("csv")
                      .option("header",false)
                      .option("inferSchema", "true")
                      .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/dataset1")
                      .load()
  
  val df1: Dataset[Row] = df.toDF("name","age","city")  
//  df1.printSchema()
//  
//  import spark.implicits._
//  val  ds = df1.as[Person]
//  val parseAgeFunction = udf(ageCheck(_:Int):String)
//  
//  
//  val adultYN = ds.withColumn("adult",parseAgeFunction(col("age")))
//  adultYN.show()
  
  spark.udf.register("parseAgeFunction",(x:Int) => {if (x>18) "Y" else "N"})
  val df2 =df1.withColumn("adult",expr("parseAgeFunction(age)"))
//  df2.show()
  
  df1.createOrReplaceTempView("peopletable")
  spark.sql("select name,age,city,parseAgeFunction(age) as adult from peopletable").show()
  
//  scala.io.StdIn.readLine()
  spark.stop()
    
  
}


