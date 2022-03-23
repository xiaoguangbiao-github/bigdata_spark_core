package cn.xiaoguangbiao.core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的聚合-有key
 */
object RDDDemo06_Aggregate_Key {
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 1.source/加载数据/创建RDD
    //RDD[一行行的数据]
    val lines: RDD[String] = sc.textFile("data/input/words.txt")

    //TODO 2.transformation
    //RDD[(单词, 1)]
    val wordAndOneRDD: RDD[(String, Int)] = lines.filter(StringUtils.isNoneBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
    //分组+聚合
    //groupBy/groupByKey + sum/reduce
    //wordAndOneRDD.groupBy(_._1)
    val grouped: RDD[(String, Iterable[Int])] = wordAndOneRDD.groupByKey()
    //grouped.mapValues(_.reduce(_+_))
    val result: RDD[(String, Int)] = grouped.mapValues(_.sum)
    //reduceByKey
    val result2: RDD[(String, Int)] = wordAndOneRDD.reduceByKey(_+_)
    //foldByKey
    val result3: RDD[(String, Int)] = wordAndOneRDD.foldByKey(0)(_+_)
    //aggregateByKeye(初始值)(局部聚合, 全局聚合)
    val result4: RDD[(String, Int)] = wordAndOneRDD.aggregateByKey(0)(_ + _, _ + _)

    //TODO 3.sink/输出
    result.foreach(println)
    result2.foreach(println)
    result3.foreach(println)
    result4.foreach(println)
  }
}
