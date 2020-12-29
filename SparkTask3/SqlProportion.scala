package st3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, desc, format_number}
import org.apache.spark.sql.{SQLContext, SparkSession}

object SqlProportion {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlProportion").master("local[*]").getOrCreate()
    import spark.implicits._

    val logDf = spark.read.format("csv")
      .option("header", "true")
      .load("data/user_log_format1.csv")
    //logDf.printSchema()
    val infoDf = spark.read.format("csv")
      .option("header", "true")
      .load("data/user_info_format1.csv")
    //logDf.printSchema()

    val buyDf = logDf.select($"user_id", $"action_type").where($"action_type" === "2").na.drop()
    /*val usergenderDf = infoDf.select($"user_id", $"gender").where($"gender" === "0" or $"gender" === "1").na.drop()
    val genderDf = usergenderDf.join(buyDf, "user_id")
      .groupBy("gender").count()
    genderDf.printSchema()
    genderDf.show()
    val gender = genderDf.collect
    println(gender(0)(1))*/

    val userageDf = infoDf.select($"user_id", $"age_range")
      .where($"age_range" === "1" or $"age_range" === "2" or $"age_range" === "3" or $"age_range" === "4" or $"age_range" === "5" or $"age_range" === "6" or $"age_range" === "7" or $"age_range" === "8")
      .na.drop()
    val ageDf = userageDf.join(buyDf, "user_id")
      .groupBy("age_range").count()
    ageDf.sort(ageDf("count").desc)
    ageDf.printSchema()
    ageDf.show()
    val age = ageDf.collect
    println(age(0)(1))


    //logDf.createOrReplaceTempView("log")
    //spark.sql("SELECT user_id,action_type FROM log").show()

    spark.stop()
  }
}
