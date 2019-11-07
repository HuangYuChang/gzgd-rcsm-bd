package com.gzgd.bi

import org.apache.spark.sql.SparkSession

/**
  * 将贵网每一天黔新闻的uid分成AB组并写入hive动态分区表
  * 模式：SQL模式
  */
object QianNewsUid2Hive {
	def main(args: Array[String]): Unit = {
		// 接收参数
		val Array(startDate, endDate, pathPrefix, qianHiveTable, logLevel, version) = args
		// 创建SparkSession
		val session = SparkSession.builder()
			.appName(s"${this.getClass.getSimpleName}")
			.enableHiveSupport()
			.getOrCreate()
		// 设置SparkContext
		val sc = session.sparkContext
		// 设置日志等级
		sc.setLogLevel(logLevel)
		// 处理每一天的数据
		for (date <- startDate.toInt to endDate.toInt) {
			val dateStr = date.toString
			// 拼接路径
			val inputPath = s"$pathPrefix/$dateStr/*"
			// 读取数据并缓存
			val baseData = session.read.textFile(inputPath).rdd.cache()
			// 获取黔新闻数据并缓存
			val qianNewsBaseData = baseData
				.filter(x => x.split("\t", -1)(5).contains(version))
				.filter(x => (x.split("\t")(5).contains("SC823") || x.split("\t")(5).contains("SC818")))
				.filter(x => ("QXW_1".equals((x.split("\t")(2))))).cache()
			// 导入隐式转换
			import session.implicits._
			// 过滤出黔新闻A组数据，并存储
			qianNewsBaseData.map(x => x.split("\t", -1)(0)).distinct
				.filter(x => "01234".contains((x.charAt(x.length - 1).toInt % 10).toString))
				.toDF("uid").createOrReplaceTempView("temp_qian_a_table")
			val qianPartitionFieldA = s"qianA-$date"
			session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
			session.sql(s"insert into table $qianHiveTable partition(time='$qianPartitionFieldA') select uid from temp_qian_a_table")
			// 过滤出黔新闻B组数据，并存储
			qianNewsBaseData.map(x => x.split("\t", -1)(0)).distinct
				.filter(x => "56789".contains((x.charAt(x.length - 1).toInt % 10).toString))
				.toDF("uid").createOrReplaceTempView("temp_qian_b_table")
			val qianPartitionFieldB = s"qianB-$date"
			session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
			session.sql(s"insert into table $qianHiveTable partition(time='$qianPartitionFieldB') select uid from temp_qian_b_table")
			// 释放缓存
			qianNewsBaseData.unpersist()
			baseData.unpersist()
		}
		// 释放资源
		session.stop()
		println("over")
	}
}
