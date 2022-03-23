package cn.xiaoguangbiao.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的聚合-没有key
 */
object RDDDemo05_Aggregate_NoKey{
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 1.source/加载数据/创建RDD
    val rdd1: RDD[Int] = sc.parallelize(1 to 10) //8
    //TODO 2.transformation

    //TODO 3.sink/输出/Action
    //需求求rdd1中各个元素的和
    println(rdd1.sum())
    println(rdd1.reduce(_ + _))
    println(rdd1.fold(0)(_ + _))
    //aggregate(初始值)(局部聚合, 全局聚合)
    println(rdd1.aggregate(0)(_ + _, _ + _))
    //55.0
    //55
    //55
    //55
  }
}
