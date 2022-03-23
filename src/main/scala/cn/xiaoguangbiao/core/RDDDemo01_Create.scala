package cn.xiaoguangbiao.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的创建
 */
object RDDDemo01_Create {
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 1.source/加载数据/创建RDD
    val rdd1: RDD[Int] = sc.parallelize(1 to 10) //8
    val rdd2: RDD[Int] = sc.parallelize(1 to 10,3) //3

    val rdd3: RDD[Int] = sc.makeRDD(1 to 10)//底层是parallelize //8
    val rdd4: RDD[Int] = sc.makeRDD(1 to 10,4) //4

    //RDD[一行行的数据]
    val rdd5: RDD[String] = sc.textFile("data/input/words.txt")//2
    val rdd6: RDD[String] = sc.textFile("data/input/words.txt",3)//3
    //RDD[一行行的数据]
    val rdd7: RDD[String] = sc.textFile("data/input/ratings10")//10
    val rdd8: RDD[String] = sc.textFile("data/input/ratings10",3)//10
    //RDD[(文件名, 一行行的数据),(文件名, 一行行的数据)....]
    val rdd9: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10")//2
    val rdd10: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10",3)//3

    println(rdd1.getNumPartitions)//8 //底层partitions.length
    println(rdd2.partitions.length)//3
    println(rdd3.getNumPartitions)//8
    println(rdd4.getNumPartitions)//4
    println(rdd5.getNumPartitions)//2
    println(rdd6.getNumPartitions)//3
    println(rdd7.getNumPartitions)//10
    println(rdd8.getNumPartitions)//10
    println(rdd9.getNumPartitions)//2
    println(rdd10.getNumPartitions)//3

    //TODO 2.transformation
    //TODO 3.sink/输出
  }
}
