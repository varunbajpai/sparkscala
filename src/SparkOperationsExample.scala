

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
import org.apache.spark.sql.functions.{col, explode, udf, expr,unix_timestamp,monotonically_increasing_id}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DateType

object SparkOperationsExample extends  App{
    val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val myList = List((1,"2013-07-25",11599,"CLOSED"),
      (2,"2014-07-25",256,"PENDING_PAYMENT"),
      (3,"2013-07-25",11599,"COMPLETE"),
      (4,"2019-07-25",8827,"CLOSED"))
      
  import spark.implicits._
  val myDf = myList.toDF("order_id","order_date","customer_id","status")    
  val  newDf = myDf.withColumn("order_date",unix_timestamp(col("order_date")
               .cast(DateType)))
               .withColumn("newid", monotonically_increasing_id)
               .dropDuplicates("order_date","customer_id")
               .drop("order_id")
               .sort("order_date")
  
  newDf.show()
 
  spark.stop()
  
}