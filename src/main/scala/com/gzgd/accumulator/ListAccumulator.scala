package com.gzgd.accumulator

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

object ListAccumulator {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ListAccumulator")
		val sc = new SparkContext(conf)

		sc.setLogLevel("ERROR")
		// 定义累加器
		val accumulator = new ListAccumulator
		// 注册累加器
		sc.register(accumulator, "myAccumulator")

		sc.parallelize(List("hadoop", "hbase", "hive", "spark", "scala"), 2).foreach {
			case word => {
				accumulator.add(word)
			}
		}

		val result = accumulator.value

		println(result)

		import scala.collection.JavaConversions._
		for (i <- 0 until result.size()) {
			println(result(i))
		}

		sc.stop()
	}
}

/**
  * 自定义累加器
  * 	1. 继承AccumulatorV2[输入数据类型，输出数据类型]
  * 	2. 重写 6 个方法
  */
class ListAccumulator extends AccumulatorV2[String, util.ArrayList[util.HashMap[String, String]]]{

	/** 定义要返回的对象 */
	private val list = new util.ArrayList[util.HashMap[String, String]]()
	/** 判断累加器是否为空 */
	override def isZero: Boolean = list.isEmpty
	/** 复制一个新的累加器 */
	override def copy(): AccumulatorV2[String, util.ArrayList[util.HashMap[String, String]]] = new ListAccumulator()
	/** 重置累加器 */
	override def reset(): Unit = list.clear()
	/** 往累加器增加元素 */
	override def add(v: String): Unit = {
		// TODO 具体业务逻辑

		val map = new util.HashMap[String, String]()
		map.put(v, v)

		list.add(map)
	}
	/** 合并累加器 */
	override def merge(other: AccumulatorV2[String, util.ArrayList[util.HashMap[String, String]]]): Unit = list.addAll(other.value)
	/** 获取累加器的值 */
	override def value: util.ArrayList[util.HashMap[String, String]] = list
}
