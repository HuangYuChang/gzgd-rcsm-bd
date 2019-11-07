package com.gzgd.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器
  */
class MapAccumulator extends AccumulatorV2[(String,String), mutable.HashMap[String, String]] {

	private  val mapAccumulator = mutable.HashMap[String,String]()

	/** 向累加器中添加值 */
	def add(keyAndValue:((String,String))): Unit ={
		this.synchronized{
			val key = keyAndValue._1
			val value = keyAndValue._2
			if (!mapAccumulator.contains(key))
				mapAccumulator += key->value
			else if(mapAccumulator.get(key).get!=value) {
				mapAccumulator += key->(mapAccumulator.get(key).get+"||"+value)
			}
		}
	}
	/** 判断当前累加器为初始化对象 */
	def isZero: Boolean = {
		mapAccumulator.isEmpty
	}
	/** 复制累加器对象 */
	def copy(): MapAccumulator ={
		val newMapAccumulator = new  MapAccumulator()
		mapAccumulator.foreach(x=>newMapAccumulator.add(x))
		newMapAccumulator
	}
	/** 获取累加器的结果 */
	def value: mutable.HashMap[String, String] = {
		mapAccumulator
	}
	/** 合并累加器 */
	def merge(other:AccumulatorV2[((String,String)),mutable.HashMap[String, String]]) = other match
	{
		case map: MapAccumulator => {
			other.value.foreach(x =>
				if (!this.value.contains(x._1))
					this.add(x)
				else
					x._2.split("\\|\\|").foreach(
						y => {
							if (!this.value.get(x._1).get.split("\\|\\|").contains(y))
								this.add(x._1, y)
						}
					)
			)
		}
		case _  =>
			throw new UnsupportedOperationException(
				s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
	}
	/** 重置累加器对象 */
	def reset(): Unit ={
		mapAccumulator.clear()
	}
}
