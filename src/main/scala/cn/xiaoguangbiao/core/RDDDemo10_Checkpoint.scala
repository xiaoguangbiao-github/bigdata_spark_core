package cn.xiaoguangbiao.core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author xiaoguangbiao
 * Desc 演示RDD的Checkpoint/检查点设置
 */
object RDDDemo10_Checkpoint{
  def main(args: Array[String]): Unit = {
    //TODO 0.env/创建环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //TODO 1.source/加载数据/创建RDD
    //RDD[一行行的数据]
    val lines: RDD[String] = sc.textFile("data/input/words.txt")

    //TODO 2.transformation
    //RDD[(单词, 数量)]
    val result: RDD[(String, Int)] = lines.filter(StringUtils.isNoneBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    //TODO =====注意:resultRDD在后续会被频繁使用到,且该RDD的计算过程比较复杂,所以为了提高后续访问该RDD的效率,应该将该RDD放到缓存中
    //result.cache()//底层persist()
    //result.persist()//底层persist(StorageLevel.MEMORY_ONLY)
    result.persist(StorageLevel.MEMORY_AND_DISK)//底层persist(StorageLevel.MEMORY_ONLY)
    //TODO =====注意:上面的缓存持久化并不能保证RDD数据的绝对安全,所以应使用Checkpoint把数据发在HDFS上
    sc.setCheckpointDir("./ckp")//实际中写HDFS目录
    result.checkpoint()


    //需求:对WordCount的结果进行排序,取出top3
    val sortResult1: Array[(String, Int)] = result
      .sortBy(_._2, false) //按照数量降序排列
      .take(3)//取出前3个

    //result.map(t=>(t._2,t._1))
    val sortResult2: Array[(Int, String)] = result.map(_.swap)
      .sortByKey(false)//按照数量降序排列
      .take(3)//取出前3个

    val sortResult3: Array[(String, Int)] = result.top(3)(Ordering.by(_._2)) //topN默认就是降序

    result.unpersist()//清空缓存

    //TODO 3.sink/输出
    result.foreach(println)
    //(hello,4)
    //(you,2)
    //(me,1)
    //(her,3)
    sortResult1.foreach(println)
    //(hello,4)
    //(her,3)
    //(you,2)
    sortResult2.foreach(println)
    //(4,hello)
    //(3,her)
    //(2,you)
    sortResult3.foreach(println)
    //(hello,4)
    //(her,3)
    //(you,2)

  }
}
