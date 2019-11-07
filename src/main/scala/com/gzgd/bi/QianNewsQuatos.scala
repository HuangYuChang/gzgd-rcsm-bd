package com.gzgd.bi

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, HashMap, HashSet, Properties}

import com.gzgd.accumulator.MapAccumulator
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 黔新闻AB组每天曝光数，播放总时长，播放平均时长，日留存用户，累计用户（过滤相关推荐位）
  *
  * 注意：
  * 	执行该程序前应该先将黔新闻推荐的数据分成AB组，并且按照日期分区写入hive
  */
object QianNewsQuatos {
	val list = new util.LinkedList[String]
	val list2 = new util.LinkedList[String]
	val set = new HashSet[String]
	val map = new HashMap[String, Int]

	def main(args: Array[String]): Unit = {

		val Array(inputPath, startDate, endDate, user, password, qnHiveTable, mysqlUrl, mysqlTable, logLevel,
		qianNewsStartDate, version) = args

		val session = SparkSession.builder().appName(s"${this.getClass.getSimpleName}")
    		.master("local[4]")
			.enableHiveSupport()
			.getOrCreate()
		
		val sc = session.sparkContext
		sc.setLogLevel(logLevel)
		// 定义一个倍数：i，若观影时长大于i倍的视频时长则该数据不参与观影时长相关指标计算
		val i: Int = 2

		for (date <- startDate.toInt to endDate.toInt) {
			val qianMap = new MapAccumulator
			sc.register(qianMap, "qianMap")

			val Asum = sc.longAccumulator("Asum")
			val Bsum = sc.longAccumulator("Bsum")

			val currentDate = date.toString
			//得到数据
			val data = sc.textFile(s"$inputPath/$currentDate/*").filter(x => x.split("\t").length > 0)
			//过滤数据
			val filterdata = data.filter { line =>
				(line.split("\t")(5).contains(version) &&
					(line.split("\t")(5).contains("SC823") ||
						line.split("\t")(5).contains("SC818")))
			}
			//A组黔新闻数据
			val filterdata2 = filterdata.filter {
				line =>
					val uid = line.split("\t")(0)
					val value = uid.charAt(uid.length() - 1).toInt
					"01234".contains((value % 10).toString())
			}.filter { line => line.split("\t")(2).equals("QXW_1") }
			//A组曝光
			val Adata = filterdata2.filter { line => line.split("\t")(3).equals("1") }
			//求出推荐位和推荐类型
			val tjwandtjtype = Adata.map { line =>
				val tjw = line.split("\t")(6)
				val tjtype = line.split("\t")(7)
				tjw + "," + tjtype
			}

			tjwandtjtype.foreach {
				case line => {
					val tjtype = line.split(",")(1).split(";")
					val tjw = line.split(",")(0).split(";")
					val tjtypecount = tjtype.length
					for (i <- 0 until (tjtypecount)) {
						if (tjtype(i).equals("2")) {
							val gettjw = tjw(i)
							list.add(gettjw)
							set.add(gettjw)
						}
					}
				}
			}

			val posiArr = set.toArray
			for (i <- 0 until posiArr.length - 1) {
				for (j <- 0 until posiArr.length - i - 1) {
					if (posiArr(j).toString.toInt > posiArr(j + 1).toString.toInt) {
						val temp = posiArr(j)
						posiArr(j) = posiArr(j + 1)
						posiArr(j + 1) = temp
					}
				}
			}
			// 顺序输出AI推荐位
			print("AI推荐位：")
			for (i <- posiArr) {
				print(i)
				if (!i.equals(posiArr(posiArr.length - 1))) print(",")
			}
			println()

			//B组黔新闻数据
			val Bfilterdata = filterdata.filter {
				line =>
					val uid = line.split("\t")(0)
					val value = uid.charAt(uid.length() - 1).toInt
					"56789".contains((value % 10).toString())
			}.filter { line => line.split("\t")(2).equals("QXW_1") }
			//B组曝光
			val Bdata = Bfilterdata.filter { line => line.split("\t")(3).equals("1") }
			//求出对应A组相同的推荐位曝光次数
			Bdata.foreach {
				case line => {
					val tjw = line.split("\t")(6).split(";")
					for (i <- 0 until (tjw.length)) {
						if (set.contains(tjw(i))) {
							list2.add(tjw(i))
						}

					}
				}
			}

			//A组播放
			val Ashow = filterdata2.filter(line => line.split("\t")(3).equals("3"))
			//A组推荐位播放
			Ashow.filter { line => set.contains(line.split("\t")(6)) }.filter(line => {
				var flag = true
				val curDuration = line.split("\t")(5).split(",")(5).split(":")(1).toLong
				val curWatch = line.split("\t")(5).split(",")(2).split(":")(1).toLong
				if ((curDuration * i) < curWatch) {
					flag = false
//									println("a:" + line)
				}
				flag
			}).foreach { line =>
				val showtime = line.split("\t")(5).split(",")(2).split(":")(1)
				Asum.add(showtime.toLong)
			}
			//B组播放
			val Bshow = Bfilterdata.filter { line => line.split("\t")(3).equals("3") }
			//B组推荐位播放
			Bshow.filter { line => set.contains(line.split("\t")(6)) }.filter(line => {
				var flag = true
				val curDuration = line.split("\t")(5).split(",")(5).split(":")(1).toLong
				val curWatch = line.split("\t")(5).split(",")(2).split(":")(1).toLong
				if ((curDuration * i) < curWatch) {
					flag = false
//									println("b:" + line)
				}
				flag
			}).foreach { line =>
				val showtime = line.split("\t")(5).split(",")(2).split(":")(1)
				Bsum.add(showtime.toLong)
			}

			val Atjuser = Ashow.filter { line => set.contains(line.split("\t")(6)) }.map {
				line => line.split("\t")(0)
			}.distinct().count()
			val Btjuser = Bshow.filter { line => set.contains(line.split("\t")(6)) }.map {
				line => line.split("\t")(0)
			}.distinct().count()

			// 计算/打印黔新闻AB组相关指标
//			calculateGroupABQuatos(session, currentDate, "A", qianMap, qnHiveTable, mysqlUrl, mysqlTable, user, password, qianNewsStartDate)
			println("A组曝光次数:" + list.size())
			println("A组总播放时长：" + Asum.value)
			println("A组平均播放时长：" + Asum.value.toString.toLong / Atjuser)
			println("A组播放次数:" + Ashow.filter { line => set.contains(line.split("\t")(6)) }.count())

//			calculateGroupABQuatos(session, currentDate, "B", qianMap, qnHiveTable, mysqlUrl, mysqlTable, user, password, qianNewsStartDate)
			println("B组推荐位曝光次数:" + list2.size())
			println("B组总播放时长：" + Bsum.value)
			println("B组平均播放时长：" + (Bsum.value.toString.toLong / Btjuser))
			println("B组播放次数:" + Bshow.filter { line => set.contains(line.split("\t")(6)) }.count())
		}
		// 释放资源
		sc.stop()
		session.stop()
	}
	/**
	  * 计算黔新闻AB组指标：
	  */
	def calculateGroupABQuatos(session: SparkSession,
							   currentDate: String,
							   group: String,
							   qianMap: MapAccumulator,
							   hiveTable: String,
							   mysqlUrl: String,
							   mysqlTable: String,
							   user: String,
							   password: String,
							   qianNewsStartDate: String): Unit = {

		val props = new Properties()
		props.put("user", user)
		props.put("password", password)

		val preDate = getDate(currentDate, -1)

		// 计算日留存用户
		val array = calculateUserQuatos(session, qianNewsStartDate, currentDate, s"qian$group", hiveTable)
		// 新增用户
		val newUser = array(0)
		// 日留存用户
		val remainUser = array(1)
		// 日活用户
		val activeUser = array(2)
		// 计算累积用户
		val prefix = if ("A".equals(group)) "a" else "b"

		session.read.format("jdbc").options(
			Map(
				"url" -> mysqlUrl,
				"driver" -> "com.mysql.jdbc.Driver",
				"dbtable" -> mysqlTable,
				"user" -> user,
				"password" -> password
			)
		).load().createOrReplaceTempView("temp_user")
		// 获取前一天的累积用户数，并写入累加器
		session.sql(s"select user_num from temp_user where date = '$prefix" + s"$preDate'").rdd
			.foreach(x => qianMap.add(prefix + preDate, x.get(0).toString))
		// 计算今天的累积用户数：今天的新增用户 + 昨天的累积用户
		val totalNum = newUser.toLong + qianMap.value.get(prefix + preDate).get.toLong

		import session.implicits._

		try {
			session.sparkContext.parallelize(List((prefix + currentDate) + "\t" + totalNum))
				.map(x => (x.split("\t")(0), x.split("\t")(1)))
				.toDF("date", "user_num")
				.write.mode(SaveMode.Append).jdbc(mysqlUrl, mysqlTable, props)
		} catch {
			case e: Exception => {
				println("有异常------------------------------")
				println(e.getMessage, e.getStackTrace)
				println("------------------------------------")
			}
		}

		println(s"$group 组日活用户：" + activeUser)
		println(s"$group 组留存用户：" + remainUser)
		println(s"$group 组累积用户：" + totalNum)
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