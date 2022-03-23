package cn.xiaoguangbiao.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的重分区
 */
object RDDDemo04_RePartition{
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 1.source/加载数据/创建RDD
    val rdd1: RDD[Int] = sc.parallelize(1 to 10) //8
    //repartition可以增加或减少分区,注意:原来的不变
    val rdd2: RDD[Int] = rdd1.repartition(9)//底层是coalesce(分区数,shuffle=true)
    val rdd3: RDD[Int] = rdd1.repartition(7)
    println(rdd1.getNumPartitions)//8
    println(rdd2.getNumPartitions)//9
    println(rdd3.getNumPartitions)//7

    //coalesce默认只能减少分区,除非把shuffle指定为true,注意:原来的不变
    val rdd4: RDD[Int] = rdd1.coalesce(9)//底层是coalesce(分区数,shuffle=false)
    val rdd5: RDD[Int] = rdd1.coalesce(7)
    val rdd6: RDD[Int] = rdd1.coalesce(9,true)
    println(rdd4.getNumPartitions)//8
    println(rdd5.getNumPartitions)//7
    println(rdd6.getNumPartitions)//9

    //TODO 2.transformation
    //TODO 3.sink/输出
  }
}
