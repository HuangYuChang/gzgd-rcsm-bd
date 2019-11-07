package com.gzgd.bi

import java.text.SimpleDateFormat
import java.util.Calendar

import com.gzgd.accumulator.MapAccumulator
import com.gzgd.utils.TimeUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 计算贵州关联推荐每天指标：曝光量，点击量，用户量和购买量，将指标存入MongoDB
  * 自定义累加器
  * 提取相关参数
  */
object RelationRecommendQuatos2MongoDB {

	def main(args: Array[String]): Unit = {

		val start = System.currentTimeMillis()

		val Array(startDate, endDate, path, mongodbRelationUri, mongodbStayUri, hiveTable, logLevel) = args

		val sceneId = "171587_4"

		val session = SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
//			.master("local[2]")
			.enableHiveSupport()
			.getOrCreate()
		val sc = session.sparkContext
		sc.setLogLevel(logLevel)

		val map: MapAccumulator = new MapAccumulator()

		sc.register(map, "MapAccumulator")

		for (date <- startDate.toInt to endDate.toInt) {
			// 初始化map集合
			map.reset()
			val dateStr = date.toString
			val inputPath = s"$path/$dateStr/*"
			// 过滤出关联数据
			val baseData = session.read.textFile(inputPath).rdd.map(line => (line.split("\t", -1)(0), line))
				.filter(x => { // 过滤出场景id为 171587_4 的数据(关联推荐的数据)
				var flag = false
				val sceneId = x._2.split("\t", -1)(2)
				if ("171587_4".equals(sceneId)) flag = true
				flag
			}).cache()

			// 过滤出相应的数据
			val baseGroupA = baseData.filter(x => "01234".contains((x._1.charAt(x._1.length - 1).toInt % 10).toString))
			val baseRecommendGroupA = baseGroupA.map(x => {
				val fields = x._2.split("\t", -1)
				(fields(0), fields(3), TimeUtils.timeStamp2Date(fields(4))) // uid, actionType, actionTtime
			})

			val baseGroupB = baseData.filter(x => "56789".contains((x._1.charAt(x._1.length - 1).toInt % 10).toString))
			val baseRecommendGroupB = baseGroupB.map(x => {
				val fields = x._2.split("\t", -1)
				(fields(0), fields(3), TimeUtils.timeStamp2Date(fields(4))) // uid, actionType, actionTtime
			})

			import session.implicits._
			// 计算贵州关联推荐每天指标：曝光量，点击量，用户量和购买量，并存入map集合
			// 过滤出A组数据
			baseRecommendGroupA.toDF("uid", "action_type", "action_time").createOrReplaceTempView("v_tableA")
			// 统计A组每天的曝光量、点击量、用户量和购买量
			val sql = "select action_time, count(*) a_expose from v_tableA where action_type=1 group by action_time"
			session.sql(sql).rdd.foreach(x => map.add("a_expose", x.get(1).toString))
			val sql2 = "select action_time, count(*) a_click from v_tableA where action_type=2 group by action_time"
			session.sql(sql2).rdd.foreach(x => map.add("a_click", x.get(1).toString))
			val sql3 = "select action_time, count(distinct(uid)) a_user_active from v_tableA where group by action_time"
			session.sql(sql3).rdd.foreach(x => map.add("a_user_active", x.get(1).toString))
			val sql4 = "select action_time, count(*) a_order from v_tableA where action_type=6 group by action_time"
			session.sql(sql4).rdd.foreach(x => map.add("a_order", x.get(1).toString))
			// A组播放量
			val playTimesA = baseRecommendGroupA.filter(_._2.contains("3")).count()
			map.add("a_play", playTimesA.toString)

			getNewUser(session, "20190413", dateStr, "A", map, hiveTable)

			calNDayRemainUser(session, "20190413", dateStr, "A", 7, map, hiveTable)
			calNDayRemainUser(session, "20190413", dateStr, "A", 30, map, hiveTable)

			// 过滤出B组数据
			baseRecommendGroupB.toDF("uid", "action_type", "action_time").createOrReplaceTempView("v_tableB")
			// 统计B组每天的曝光量、点击量、用户量和购买量并存入map集合
			val sqlB = "select action_time, count(*) b_expose from v_tableB where action_type=1 group by action_time"
			session.sql(sqlB).rdd.foreach(x => map.add("b_expose", x.get(1).toString))
			val sql2B = "select action_time, count(*) b_click from v_tableB where action_type=2 group by action_time"
			session.sql(sql2B).rdd.foreach(x => map.add("b_click", x.get(1).toString))
			val sql3B = "select action_time, count(distinct(uid)) b_user_active from v_tableB group by action_time"
			session.sql(sql3B).rdd.foreach(x => map.add("b_user_active", x.get(1).toString))
			val sql4B = "select action_time, count(*) b_order from v_tableB where action_type=6 group by action_time"
			session.sql(sql4B).rdd.foreach(x => map.add("b_order", x.get(1).toString))
			// B组播放量
			val playTimesB = baseRecommendGroupB.filter(_._2.contains("3")).count()
			map.add("b_play", playTimesB.toString)

			getNewUser(session, "20190413", dateStr, "B", map, hiveTable)

			calNDayRemainUser(session, "20190413", dateStr, "B", 7, map, hiveTable)
			calNDayRemainUser(session, "20190413", dateStr, "B", 30, map, hiveTable)

			val mapValue = map.value

			println(mapValue)

			// 存入MongoDB每一天的数据
			val value = sc.parallelize(List(
				Row(dateStr,
					sceneId,
					mapValue.get("a_expose").get,
					mapValue.get("a_click").get,
					if (mapValue.get("a_user_active").get.contains("||"))
						mapValue.get("a_user_active").get.split("\\|\\|")(1)
					else mapValue.get("a_user_active").get,
					mapValue.get("a_order").get,
					mapValue.get("a_user_new").get,
					mapValue.get("a_play").get,
					mapValue.get("b_expose").get,
					mapValue.get("b_click").get,
					if (mapValue.get("b_user_active").get.contains("||"))
						mapValue.get("b_user_active").get.split("\\|\\|")(0)
					else mapValue.get("b_user_active").get,
					mapValue.get("b_order").get,
					mapValue.get("b_user_new").get,
					mapValue.get("b_play").get)))

			val schema = StructType(Array(
				StructField("date", StringType, true),
				StructField("scene_id", StringType, true),
				StructField("a_expose", StringType, true),
				StructField("a_click", StringType, true),
				StructField("a_user_active", StringType, true),
				StructField("a_order", StringType, true),
				StructField("a_user_new", StringType, true),
				StructField("a_play", StringType, true),
				StructField("b_expose", StringType, true),
				StructField("b_click", StringType, true),
				StructField("b_user_active", StringType, true),
				StructField("b_order", StringType, true),
				StructField("b_user_new", StringType, true),
				StructField("b_play", StringType, true)))

			session.createDataFrame(value, schema).write
				.options(Map("spark.mongodb.output.uri" -> mongodbRelationUri))
				.mode("append")
				.format("com.mongodb.spark.sql")
				.save()

			val value_stay = sc.parallelize(List(
				Row(dateStr,
					sceneId,
					mapValue.get("a_user_new_7").get,
					mapValue.get("a_user_new_7_stay").get,
					mapValue.get("a_user_active_7").get,
					mapValue.get("a_user_active_7_stay").get,
					mapValue.get("a_user_new_30").get,
					mapValue.get("a_user_new_30_stay").get,
					mapValue.get("a_user_active_30").get,
					mapValue.get("a_user_active_30_stay").get,
					mapValue.get("b_user_new_7").get,
					mapValue.get("b_user_active_7_stay").get,
					mapValue.get("b_user_active_7").get,
					mapValue.get("b_user_new_7_stay").get,
					mapValue.get("b_user_new_30").get,
					mapValue.get("b_user_new_30_stay").get,
					mapValue.get("b_user_active_30").get,
					mapValue.get("b_user_active_30_stay").get)))

			val schema_stay = StructType(Array(
				StructField("date", StringType, true),
				StructField("scene_id", StringType, true),
				StructField("a_user_new_7", StringType, true),
				StructField("a_user_new_7_stay", StringType, true),
				StructField("a_user_active_7", StringType, true),
				StructField("a_user_active_7_stay", StringType, true),
				StructField("a_user_new_30", StringType, true),
				StructField("a_user_new_30_stay", StringType, true),
				StructField("a_user_active_30", StringType, true),
				StructField("a_user_active_30_stay", StringType, true),
				StructField("b_user_new_7", StringType, true),
				StructField("b_user_active_7_stay", StringType, true),
				StructField("b_user_active_7", StringType, true),
				StructField("b_user_new_7_stay", StringType, true),
				StructField("b_user_new_30", StringType, true),
				StructField("b_user_new_30_stay", StringType, true),
				StructField("b_user_active_30", StringType, true),
				StructField("b_user_active_30_stay", StringType, true)))

			session.createDataFrame(value_stay, schema_stay).write
				.options(Map("spark.mongodb.output.uri" -> mongodbStayUri))
				.mode("append")
				.format("com.mongodb.spark.sql")
				.save()

			println(System.currentTimeMillis() - start)

			println(s"$dateStr is over")

			// 释放缓存
			baseData.unpersist()
		}
		// 释放资源
		sc.stop()
		session.stop()
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
	def getNewUser(session: SparkSession, startDate: String, endDate: String, group: String, map: MapAccumulator, hiveTable: String): Unit = {
		val rddBefore = session.sql(s"select uid from $hiveTable where time >= '$group-$startDate' and time < '$group-$endDate'").rdd.distinct()
		val rddCurrentDate = session.sql(s"select uid from $hiveTable where time = '$group-$endDate'").rdd
		val RDDNewUser = rddCurrentDate.subtract(rddBefore)
		map.add(s"${group.toLowerCase}_user_new", RDDNewUser.count.toString)
	}
	/**
	  * 计算n日留存
	  */
	def calNDayRemainUser(session: SparkSession, startDate: String, endDate: String, group: String, n: Int, map: MapAccumulator, hiveTable: String): Unit = {

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
		map.add(group.toLowerCase + s"_user_new_$n", newUserInLast2Week.count.toString)
		map.add(group.toLowerCase + s"_user_new_${n}_stay", activeUser.toString)
		// 求出前一个n天在本个n天的留存用户
		val lastWeekRemainUser = rdd5.intersection(rdd3).count

		map.add(group.toLowerCase + s"_user_active_$n", rdd3.count.toString)
		map.add(group.toLowerCase + s"_user_active_${n}_stay", lastWeekRemainUser.toString)
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
}
