package st1

import au.com.bytecode.opencsv.CSVParser
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.{SQLContext, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

object Count {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("SparkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputpath1 = "data/user_log_format1.csv"
    val inputpath2 = "data/user_info_format1.csv"
    val outputpath1 = "out/out1"
    val outputpath2 = "out/out2"

    val logrdd = sc.textFile(inputpath1)
    val header = logrdd.first
    val log = logrdd.filter(line => line != header) //去除标题
      .map(line => line.split(",")) //分割行
      .filter(line => line.length == 7) //删除缺失值
      .filter(line=>line(5)=="1111")  //筛选双十一日志
      .map(line=>(line(0),(line(1),line(2),line(3),line(4),line(5),line(6))))

    val infordd = sc.textFile(inputpath2)
    val header2 = infordd.first
    val info = infordd.filter(line => line != header2) //去除标题
      .map(line => line.split(","))
      .filter(line => line.length == 3)
      .map(line=>(line(0),(line(1),line(2))))

    //统计双十一最热门的商品
    val count1=log.map(line=>(line._2._1,line._2._6.toInt)) //转化为<item_id,action_type>形式
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .map(line=>(line._1,line._2))

    val result1=sc.parallelize(count1.take(100))
    result1.coalesce(1).saveAsTextFile(outputpath1)

    val count2=log.join(info)
      .filter(line=>line._2._2._1=="1"||line._2._2._1=="2"||line._2._2._1=="3") //筛选30岁以下的年轻人
      .map(line=>(line._2._1._3,line._2._1._6.toInt))
      .reduceByKey(_+_)
      .sortBy(_._2,false)
      .map(line=>(line._1,line._2))

    val result2=sc.parallelize(count2.take(100))
    result2.coalesce(1).saveAsTextFile(outputpath2)

    sc.stop()
  }
}