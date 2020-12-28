package st1

import au.com.bytecode.opencsv.CSVParser
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{SQLContext, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

case class User(user_id:String,item_id:String,cat_id:String,seller_id:String,brand_id:String,time_stamp:String,action_type:String)

object SparkCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCount").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    val inputpath = "data/user_log_format1.csv"
    val outputpath = "out/out1"

    val data=sqlContext.read.format("csv")
      .option("header","true")
      .option("nullValue","\\N")
      .option("inferSchema", "true")
      .schema(ScalaReflection.schemaFor[User].dataType.asInstanceOf[StructType])
      .load(inputpath)
    data.createTempView("user_log")

    val df=data.toDF("user_id","item_id","cat_id","seller_id","brand_id","time_stamp","action_type")

    val newdf=df.toDF("item_id","action_type")

    newdf.show()
    newdf.printSchema()

    sc.stop()

  }
}