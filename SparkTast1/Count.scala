package st1

import au.com.bytecode.opencsv.CSVParser
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.{SQLContext, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

import scala.reflect.io.Path

//case class User(user_id: String, item_id: String, cat_id: String, seller_id: String, brand_id: String, time_stamp: String, action_type: String)

object SparkCount {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf().setAppName("SparkCount").setMaster("local[*]")
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

    sc.stop()*/

    val conf = new SparkConf().setAppName("SparkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputpath1 = "data/user_log_format1.csv"
    val inputpath2 = "data/user_info_format1.csv"
    val outputpath1 = "out/out1"
    val outputpath2 = "out/out2"

    val logrdd = sc.textFile(inputpath1)
    val header = logrdd.first
    val log = logrdd.filter(line => {
      line != header
    })

    val infordd = sc.textFile(inputpath2)
    val header2 = infordd.first
    val info = infordd.filter(line => {
      line != header2
    })

    val counts = log.filter(line => line.split(",")(6) != "0").map(line => line.split(",")(1))
      .map(goods => (goods, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    val result = sc.parallelize(counts.take(100))

    //result.coalesce(1).saveAsTextFile(outputpath1)

    val user_pair = info.filter(line => {
      line.split(",")(1) == "1" || line.split(",")(1) == "2" || line.split(",")(1) == "3"
    })
      .map(line => (line.split(",")(0), line.split(",")(1)))

    val out1 = sc.parallelize(user_pair.take(10))
    //val out1 = sc.parallelize(user_pair.take(1000))
    //println("user pair")
    //out1.foreach(println)

    val goods_pair = log.filter(line => line.split(",")(6) != "0")
      .map(line => (line.split(",")(0), line.split(",")(3)))

    val out2 = sc.parallelize(goods_pair.take(10))
    //println("goods pair")
    //out2.foreach(println)

    goods_pair.rightOuterJoin(user_pair)
    val join = goods_pair.values
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    val out3 = sc.parallelize(join.take(100))
    //println("joined pair")
    //out3.foreach(println)

    out3.coalesce(1).saveAsTextFile(outputpath2)

    sc.stop()
  }
}
