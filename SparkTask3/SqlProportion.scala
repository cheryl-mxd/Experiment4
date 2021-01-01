package st3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, count, desc, format_number, sum, when}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.sparkproject.dmg.pmml.True
import org.apache.spark.sql.functions

import scala.math.Fractional.Implicits.infixFractionalOps

object SqlProportion {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlProportion").master("local[*]").getOrCreate()
    import spark.implicits._

    val info_schema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("age_range", IntegerType),
        StructField("gender", IntegerType)
      )
    )
    val log_schema = StructType(
      List(
        StructField("user_id", StringType),
        StructField("item_id", StringType),
        StructField("cat_id", StringType),
        StructField("seller_id", StringType),
        StructField("brand_id", StringType),
        StructField("time_stamp", StringType),
        StructField("action_type", IntegerType)
      )
    )

    val logDf = spark.read.format("csv")
      .option("header", "true")
      .schema(log_schema)
      .load("data/user_log_format1.csv")
      .select($"user_id", $"action_type") //提取user_id和action_type字段
      .where(($"action_type" === 2) and ($"time_stamp" === "1111")) //筛选双十一和购买
      .dropDuplicates() //去重
      .na.drop("any")
    //logDf.printSchema()
    val infoDf = spark.read.format("csv")
      .option("header", "true")
      .schema(info_schema)
      .load("data/user_info_format1.csv")
      .select($"user_id", $"age_range", $"gender")
      .where($"gender" < 2 and $"age_range" > 0) //筛选年龄和性别明确的
      .na.drop("any")
    //infoDf.printSchema()

    val genderjoin = logDf.join(infoDf, "user_id").groupBy("gender").count()
    genderjoin.show()
    val agejoin = logDf.join(infoDf, "user_id").groupBy("age_range").count()
    agejoin.show()

    /*
    val genderDf = infoDf.join(logDf, "user_id")
      .groupBy("gender").count()
    genderDf.printSchema()
    genderDf.show()
    val gender = genderDf.collect
    println(gender(0)(1))

    //val userageDf = infoDf.select($"user_id", $"age_range").where($"age_range" === "1" or $"age_range" === "2" or $"age_range" === "3" or $"age_range" === "4" or $"age_range" === "5" or $"age_range" === "6" or $"age_range" === "7" or $"age_range" === "8").na.drop()
    val ageDf = infoDf.join(logDf, "user_id")
      .groupBy("age_range").count()
    ageDf.sort(ageDf("count").desc)
    ageDf.printSchema()
    ageDf.show()
    val age = ageDf.collect
    println(age(0)(1))
  */

    spark.stop()
  }
}
