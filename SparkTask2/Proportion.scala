package st2

import au.com.bytecode.opencsv.CSVParser
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.{SQLContext, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object Proportion {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProportionCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val infordd = sc.textFile("data/user_info_format1.csv")
    val header1 = infordd.first
    val info = infordd.filter(line => line != header1)
    //(user_id, gender)
    val user_info = info.map(line => line.split(","))
      .filter(line => line.length == 3)
      .filter(line => line(1) != "0") //筛除年龄未知
      .filter(line => line(2) != "2") //筛除性别未知
      .map(line => (line(0), (line(1), line(2))))

    val logrdd = sc.textFile("data/user_log_format1.csv")
    val header2 = logrdd.first
    val log = logrdd.filter(line => line != header2)
    val user_log = log.map(line => line.split(","))
      .filter(line => line.length == 7)
      .filter(line => line(5) == "1111") //筛选双十一
      .filter(line => line(6) == "2") //筛选购买
      .map(line => (line(0), line(6))) //(user_id,action_type)
      .distinct //去重

    //统计买了商品的买家
    val count = user_info.join(user_log) //(user_id,<(age,gender),action_type>)
    val maleCount = count.filter(line => line._2._1._2 == "1").count()
    println("Male:" + maleCount)
    val femaleCount = count.filter(line => line._2._1._2 == "0").count()
    println("Female:" + femaleCount)
    val gendercount = count.count()
    println("Total users:" + gendercount)

    //统计男女比例
    println("Male proportion:" + maleCount.toDouble / gendercount.toDouble)
    println("Female proportion:" + femaleCount.toDouble / gendercount.toDouble)
    println("-" * 40)

    //统计年龄段比例
    val range1 = count.filter(line => line._2._1._1 == "1").count()
    val range2 = count.filter(line => line._2._1._1 == "2").count()
    val range3 = count.filter(line => line._2._1._1 == "3").count()
    val range4 = count.filter(line => line._2._1._1 == "4").count()
    val range5 = count.filter(line => line._2._1._1 == "5").count()
    val range6 = count.filter(line => line._2._1._1 == "6").count()
    val range7 = count.filter(line => line._2._1._1 == "7" || line._2._1._1 == "8").count()
    val agecount = count.count()
    println("<18:" + range1 + ", proportion=" + range1.toDouble / agecount.toDouble)
    println("[18,24]:" + range2 + ", proportion=" + range2.toDouble / agecount.toDouble)
    println("[25,29]:" + range3 + ", proportion=" + range3.toDouble / agecount.toDouble)
    println("[30,34]:" + range4 + ", proportion=" + range4.toDouble / agecount.toDouble)
    println("[35,39]:" + range5 + ", proportion=" + range5.toDouble / agecount.toDouble)
    println("[40,49]:" + range6 + ", proportion=" + range6.toDouble / agecount.toDouble)
    println(">=50:" + range7 + ", proportion=" + range7.toDouble / agecount.toDouble)
    println("Total age ranges:" + agecount)

    sc.stop()
  }
}
