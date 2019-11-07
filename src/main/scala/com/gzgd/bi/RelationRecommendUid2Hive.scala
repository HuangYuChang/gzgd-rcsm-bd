package com.gzgd.bi

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession

/**
  * 将贵网每一天关联推荐的uid分成AB组并写入hive动态分区表
  * 模式：SQL模式
  */
object RelationRecommendUid2Hive {
	def main(args: Array[String]): Unit = {
		// 接收参数
		val Array(startDate, endDate, pathPrefix, rlHiveTable, logLevel) = args
		// 创建SparkSession
		val session = SparkSession.builder()
			.appName(s"${this.getClass.getSimpleName}")
			.enableHiveSupport()
			.getOrCreate()

		val sc = session.sparkContext
		sc.setLogLevel(logLevel)

		for (date <- startDate.toInt to endDate.toInt) {
			
			val start = System.currentTimeMillis()

			val dateStr = date.toString

			val inputPath = s"$pathPrefix/$dateStr/*"
			// 过滤出关联数据
			val baseData = session.read.textFile(inputPath).rdd.cache()
			// 获取关联推荐数据
			val relationRecommendUid = baseData.filter(x => { // 过滤出场景id为 171587_4 的数据(关联推荐的数据)
				var flag = false
				val sceneId = x.split("\t", -1)(2)
				if (!StringUtils.isEmpty(sceneId)) {
					if ("171587_4".equals(sceneId)) flag = true
				}
				flag
			}).map(x => x.split("\t", -1)(0)).distinct.cache()

			import session.implicits._
			// 存储关联推荐数据
			// 过滤出A组数据，并存储
			relationRecommendUid.filter(x => "01234".contains((x.charAt(x.length - 1).toInt % 10).toString))
				.toDF("uid").createOrReplaceTempView("temp_a_table")
			val partitionFieldA = s"A-$date"
			session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
			session.sql(s"insert into table $rlHiveTable partition(time='$partitionFieldA') select uid from temp_a_table")
			// 过滤出B组数据，并存储
			relationRecommendUid.filter(x => "56789".contains((x.charAt(x.length - 1).toInt % 10).toString))
				.toDF("uid").createOrReplaceTempView("temp_b_table")
			val partitionFieldB = s"B-$date"
			session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
			session.sql(s"insert into table $rlHiveTable partition(time='$partitionFieldB') select uid from temp_b_table")
			relationRecommendUid.unpersist()
			baseData.unpersist()
			println(System.currentTimeMillis() - start)
		}
		session.stop()
		println("over")
	}
}
