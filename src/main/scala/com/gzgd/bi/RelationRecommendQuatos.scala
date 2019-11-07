package com.gzgd.bi

import java.text.SimpleDateFormat
import java.util.Calendar

import com.gzgd.utils.{SQLUtils, TimeUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
/**
  * 计算:
  * 	1. 贵州关联推荐每天指标：曝光量，点击量，用户量和购买量
  * 	2. 贵州关联推荐每天每个推荐位，A组和B组中AI，人工置顶推荐的曝光，点击，购买量
  *
  * 注意：
  * 	执行该程序前应该先将关联推荐的数据分成AB组，并且按照日期分区写入hive
  */
object RelationRecommendQuatos {

	def main(args: Array[String]): Unit = {

		val Array(inputPath, startDate, endDate, rlrcHiveTable, logLevel) = args

		val session = SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
			.enableHiveSupport()
			.getOrCreate()

		val sc = session.sparkContext
		sc.setLogLevel(logLevel)

		for (date <- startDate.toInt to endDate.toInt) {
			val currentDate = date.toString
			val start = System.currentTimeMillis()
			// 读取每天数据
			val baseData = session.read.textFile(s"$inputPath/$currentDate/*").rdd
				.map(line => (line.split("\t", -1)(0), line)).persist(StorageLevel.MEMORY_ONLY_SER)
			// 过滤得到关联推荐数据
			val relationRecommendBaseData = baseData.filter(x => "171587_4".equals(x._2.split("\t", -1)(2)))
			// 过滤关联推荐A组数据
			val relationRecommendBaseDataGroupA = relationRecommendBaseData
				.filter(x => "01234".contains((x._1.charAt(x._1.length - 1).toInt % 10).toString))

			val baseRecommendGroupA = relationRecommendBaseDataGroupA.map(x => {
				val fields = x._2.split("\t", -1)
				(fields(0), fields(3), TimeUtils.timeStamp2Date(fields(4))) // uid, actionType, actionTtime
			})
			// 过滤关联推荐B组数据
			val relationRecommendBaseDataGroupB = relationRecommendBaseData.filter(x => "56789".contains((x._1.charAt(x._1.length - 1).toInt % 10).toString))

			val baseRecommendGroupB = relationRecommendBaseDataGroupB.map(x => {
				val fields = x._2.split("\t", -1)
				(fields(0), fields(3), TimeUtils.timeStamp2Date(fields(4))) // uid, actionType, actionTtime
			})
			import session.implicits._
			// 2. 计算贵州关联推荐每天指标：曝光量，点击量，用户量和购买量
			// A组数据
			baseRecommendGroupA.toDF("uid", "action_type", "action_time").createOrReplaceTempView("v_tableA")
			// 统计A组每天的曝光量、点击量、用户量和购买量
			val sql = "select action_time, count(*) exposureA from v_tableA where action_type=1 group by action_time"
			session.sql(sql).createOrReplaceTempView("v_exposure_a")
			val sql2 = "select action_time, count(*) clickNumA from v_tableA where action_type=2 group by action_time"
			session.sql(sql2).createOrReplaceTempView("v_click_a")
			val sql3 = "select action_time, count(distinct(uid)) userNumA from v_tableA where group by action_time"
			session.sql(sql3).createOrReplaceTempView("v_user_a")
			val sql4 = "select action_time, count(*) orderNumA from v_tableA where action_type=6 group by action_time"
			session.sql(sql4).createOrReplaceTempView("v_order_a")

			session.sql(SQLUtils.groupAQueryCondition).show()

			calNDayRemainUser(session, "20190413", currentDate, "A", 7, rlrcHiveTable)
			// B组数据
			baseRecommendGroupB.toDF("uid", "action_type", "action_time").createOrReplaceTempView("v_tableB")
			// 统计B组每天的曝光量、点击量、用户量和购买量
			val sqlB = "select action_time, count(*) exposureB from v_tableB where action_type=1 group by action_time"
			session.sql(sqlB).createOrReplaceTempView("v_exposure_b")
			val sql2B = "select action_time, count(*) clickNumB from v_tableB where action_type=2 group by action_time"
			session.sql(sql2B).createOrReplaceTempView("v_click_b")
			val sql3B = "select action_time, count(distinct(uid)) userNumB from v_tableB where group by action_time"
			session.sql(sql3B).createOrReplaceTempView("v_user_b")
			val sql4B = "select action_time, count(*) orderNumB from v_tableB where action_type=6 group by action_time"
			session.sql(sql4B).createOrReplaceTempView("v_order_b")
			session.sql(SQLUtils.groupBQueryCondition).show()

			calNDayRemainUser(session, "20190413", currentDate, "B", 7, rlrcHiveTable)

			val baseDataGroupA = relationRecommendBaseData.filter(x => "01234".contains((x._1.charAt(x._1.length - 1).toInt % 10).toString))
				.filter(_._2.split("\t", -1)(3).equals("3"))
			// 求A组指标：播放次数，平均观影时长
			val playCountGroupA = baseDataGroupA.map(_._2.split("\t", -1)(0)).distinct.count

			println("playCountGroupA:" + playCountGroupA)

			println("-----------------------------------------------------------------------------------------------------")

			val baseDataGroupB = relationRecommendBaseData.filter(x => "56789".contains((x._1.charAt(x._1.length - 1).toInt % 10).toString))
				.filter(_._2.split("\t", -1)(3).equals("3"))
			// 求B组指标：播放次数，平均观影时长
			val playCountGroupB = baseDataGroupB.map(_._2.split("\t", -1)(0)).distinct.count

			println("playCountGroupB:" + playCountGroupB)

			println("-----------------------------------------------------------------------------------------------------")
			// 释放缓存
			baseData.unpersist()
			println(System.currentTimeMillis() - start)
		}
		// 释放资源
		session.stop()
	}

	def calculateUserQuatos(session: SparkSession, startDate: String, endDate: String, group: String, table: String): Array[Long] = {
		val preEndDate = getDate(endDate, -1)
		val groupPreEndDate = s"$group-$preEndDate"
		val groupStartDate = s"$group-$startDate"
		val groupEndDate = s"$group-$endDate"

		val rdd1 = session.sql(s"select uid from $table where time >= '$groupStartDate' and time <= '$groupPreEndDate'").rdd.distinct
		val rdd2 = session.sql(s"select uid from $table where time = '$groupEndDate'").rdd

		val newUserNum = rdd2.subtract(rdd1).count
		val remainUserNum = rdd2.intersection(rdd1).count
		val currentDateActiveUser = rdd2.count()

		Array(newUserNum, remainUserNum, currentDateActiveUser)
	}
	/**
	  * 计算n日留存
	  */
	def calNDayRemainUser(session: SparkSession,
						  startDate: String,
						  endDate: String,
						  group: String,
						  n: Int,
						  hiveTable: String): Unit = {

		val dateArr = getLastNDateStartAndEndDate(endDate, n)
		// 求出前一个n天新增用户
		// 获取前一个n天的起始时间
		val last2WeekStartDate = dateArr(1)
		val last2WeekEndDate = dateArr(2)
		// 求出前一个n天活跃用户
		val rdd3 = session.sql(s"select uid from $hiveTable where time >= '$group-$last2WeekStartDate' and time <= '$group-$last2WeekEndDate'").rdd.distinct
		// 求出前一个n天前所有用户
		val rdd4 = session.sql(s"select uid from $hiveTable where time >= '$group-$startDate' and time < '$group-$last2WeekStartDate'").rdd.distinct
		// 取上面两个RDD的差集即上上周的新增用户
		val newUserInLast2Week = rdd3.subtract(rdd4)

		// 求出本个n天的活跃用户
		// 获取本个n天的起始时间
		val lastWeekStartDate = dateArr(0)
		val lastWeekEndDate = endDate
		val rdd5 = session.sql(s"select uid from $hiveTable where time >= '$group-$lastWeekStartDate' and time <= '$group-$lastWeekEndDate'").rdd.distinct

		// 计算用户n天的留存率（前一个n天的新增用户在本个n天的活跃用户数/前一个n天的新增用户数）
		// 前一个n天的新增用户在本个n天的活跃用户数
		val activeUser = rdd5.intersection(newUserInLast2Week).count

		// 保存每一组的前一个n天的新增用户数，以及其在本个n天还活跃的用户数
		println(group + s"_new_user_$n" + ": " + newUserInLast2Week.count.toString)
		println(group + s"_remain_user_x_$n" + ": " + activeUser.toString)

		// 求出前一个n天在本个n天的留存用户
		val lastWeekRemainUser = rdd5.intersection(rdd3).count

		println(group + s"_active_user_$n" + ": " + rdd3.count.toString)
		println(group + s"_remain_user_y_$n" + ": " + lastWeekRemainUser.toString)
	}
	/**
	  * 获取指定日期的某些指定时间周期的起止日期
	  */
	def getLastNDateStartAndEndDate(dateStr: String, n: Int): Array[String] = {
		val calendar = Calendar.getInstance()
		val sdf = new SimpleDateFormat("yyyyMMdd")
		// 将字符串转换成日期
		val date = sdf.parse(dateStr)

		calendar.setTime(date)

		calendar.add(Calendar.DAY_OF_YEAR, -(n - 1))
		val startDate1 = sdf.format(calendar.getTime)

		calendar.add(Calendar.DAY_OF_YEAR, -1)
		val endDate2 = sdf.format(calendar.getTime)

		calendar.add(Calendar.DAY_OF_YEAR, -(n - 1))
		val startDate2 = sdf.format(calendar.getTime)

		return Array(startDate1, startDate2, endDate2)
	}
	/**
	  * 获取指定日期上周以及上上周的起止日期
	  */
	def getLast2WeekStartAndEndDate(dateStr: String): Array[String] = {
		val calendar = Calendar.getInstance()
		val sdf = new SimpleDateFormat("yyyyMMdd")
		// 将字符串转换成日期
		val date = sdf.parse(dateStr)

		calendar.setTime(date)

		calendar.add(Calendar.DAY_OF_YEAR, -6)
		val endDate1 = sdf.format(calendar.getTime)

		calendar.add(Calendar.DAY_OF_YEAR, -1)
		val endDate2 = sdf.format(calendar.getTime)

		calendar.add(Calendar.DAY_OF_YEAR, -6)
		val startDate2 = sdf.format(calendar.getTime)

		return Array(endDate1, startDate2, endDate2)
	}
	/**
	  * 获取离指定日期n天的日期(字符串)
	  */
	def getDate(dateStr: String, n: Int): String = {
		val calendar = Calendar.getInstance()
		val sdf = new SimpleDateFormat("yyyyMMdd")
		// 将字符串转换成日期
		val date = sdf.parse(dateStr.toString)
		calendar.setTime(date)
		calendar.add(Calendar.DAY_OF_YEAR, n)

		return sdf.format(calendar.getTime)
	}
}
