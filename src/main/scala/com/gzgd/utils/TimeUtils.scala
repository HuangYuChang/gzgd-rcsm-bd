package com.gzgd.utils

import java.text.SimpleDateFormat
import java.util.Date

object TimeUtils {
	def main(args: Array[String]): Unit = {
		val timeStamp = "1554395536"
		val dateStr = timeStamp2Hour(timeStamp)
		println(dateStr)

//		val timeStr = "20190319172347227"
//		val timeStamp = timeStampForamt(timeStr)
//		println(timeStamp)

	}

	/**
	  * 将时间戳转成日期
	  */
	def timeStamp2Date(timeStamp: String): String = {
		if (timeStamp == null || timeStamp.isEmpty() || timeStamp.equals("null")) {
			return "未知";
		}
		val sdf = new SimpleDateFormat("yyyyMMdd")

		val dateStr = sdf.format(new Date((timeStamp + "000").toLong))

		return dateStr;
	}
	/**
	  * 将时间戳转成小时
	  */
	def timeStamp2Hour(timeStamp: String): String = {
		if (timeStamp == null || timeStamp.isEmpty() || timeStamp.equals("null")) {
			return "未知";
		}
		val sdf = new SimpleDateFormat("yyyyMMddHH")

		val hourStr = sdf.format(new Date((timeStamp + "000").toLong))

		return hourStr;
	}
	/**
	  * 转换时间格式
	  */
	def timeStampForamt(timeStamp: String): String = {
		if(timeStamp == null || timeStamp.isEmpty() || timeStamp.equals("null")){
			return "未知";
		}
		// 20190319172347227 --> 1552987427227
		val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
		val date = sdf.parse(timeStamp)
		val time = date.getTime
		return time.toString
	}
	/**
	  * 时间转成时间戳
	  */
	def time2TimeStamp(time: String): String = {
		if(time == null || time.isEmpty() || time.equals("null")){
			return "未知";
		}
		// 20190319172347227 --> 1552987427(227)
		val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")

		val date = sdf.parse(time)

		val time1 = date.getTime
//		var res = String.valueOf(time1)

//		res = res.substring(0, 10)
		return time1.toString
	}

}
