package cn.xiaoguangbiao.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示Spark入门案例-WordCount
 */
object WordCount_bak {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 2.source/读取数据
    //RDD:A Resilient Distributed Dataset (RDD):弹性分布式数据集,简单理解为分布式集合!使用起来和普通集合一样简单!
    //RDD[就是一行行的数据]
    val lines: RDD[String] = sc.textFile("data/input/words.txt")

    //TODO 3.transformation/数据操作/转换
    //切割:RDD[一个个的单词]
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //记为1:RDD[(单词, 1)]
    val wordAndOnes: RDD[(String, Int)] = words.map((_,1))
    //分组聚合:groupBy + mapValues(_.map(_._2).reduce(_+_)) ===>在Spark里面分组+聚合一步搞定:reduceByKey
    val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_+_)

    //TODO 4.sink/输出
    //直接输出
    result.foreach(println)
    //收集为本地集合再输出
    println(result.collect().toBuffer)
    //输出到指定path(可以是文件/夹)
    result.repartition(1).saveAsTextFile("data/output/result")
    result.repartition(2).saveAsTextFile("data/output/result2")
    result.saveAsTextFile("data/output/result3")

    //为了便于查看Web-UI可以让程序睡一会
    Thread.sleep(1000 * 6000)

    //TODO 5.关闭资源
    sc.stop()
  }
}
