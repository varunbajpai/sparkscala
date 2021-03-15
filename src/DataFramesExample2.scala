import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object DataFramesExample2 extends App {
  
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  
  val ordersDf = spark.read
                      .option("header",true)
                      .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/users.parquet")
                      .load
  ordersDf.printSchema
  ordersDf.show(false)

  
  scala.io.StdIn.readLine()
  spark.stop()
                              
  
}