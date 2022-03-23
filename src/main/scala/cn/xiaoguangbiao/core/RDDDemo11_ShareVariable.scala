package cn.xiaoguangbiao.core

import java.lang

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的共享变量
 */
object RDDDemo11_ShareVariable{
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //需求:
    // 以词频统计WordCount程序为例，处理的数据word2.txt所示，包括非单词符号，
    // 做WordCount的同时统计出特殊字符的数量
    //创建一个计数器/累加器
    val mycounter: LongAccumulator = sc.longAccumulator("mycounter")
    //定义一个特殊字符集合
    val ruleList: List[String] = List(",", ".", "!", "#", "$", "%")
    //将集合作为广播变量广播到各个节点的内存中
    val broadcast: Broadcast[List[String]] = sc.broadcast(ruleList)

    //TODO 1.source/加载数据/创建RDD
    val lines: RDD[String] = sc.textFile("data/input/words2.txt")

    //TODO 2.transformation
    val wordcountResult: RDD[(String, Int)] = lines.filter(StringUtils.isNoneBlank(_))
      .flatMap(_.split("\\s+"))
      .filter(ch => {
        //获取广播数据
        val list: List[String] = broadcast.value
        if (list.contains(ch)) { //如果是特殊字符
          mycounter.add(1)
          false
        } else { //是单词
          true
        }
      }).map((_, 1))
      .reduceByKey(_ + _)

    //TODO 3.sink/输出
    wordcountResult.foreach(println)
    val chResult: lang.Long = mycounter.value
    println("特殊字符的数量:"+chResult)

  }
}
