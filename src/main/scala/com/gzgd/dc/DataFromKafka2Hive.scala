package com.gzgd.dc

import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.{Date, HashMap}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * SparkStreaming消费kafka数据，将数据解析之后存入hive动态分区表
  */
object DataFromKafka2Hive {

//	System.setProperty("HADOOP_USER_NAME", "root")

	val simpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
	private val jsontomap = new ConcurrentHashMap[String, String]()

	def main(args: Array[String]): Unit = {

		val Array(zkhosts_hbase, tableName, bootstrap_servers_readfrom, consumer_group, topics_input, hiveTable, logLevel,
		millisecondsStr, autoOffsetReset) = args

		// spark streaming 配置
		val conf = new SparkConf()
			.setAppName(s"${this.getClass.getSimpleName}")
//			.setMaster("local[*]")

		val ssc = new StreamingContext(conf, Milliseconds(millisecondsStr.toLong))

		ssc.sparkContext.setLogLevel(logLevel)

		//创建sparksession
		val session = SparkSession.builder()
			.config(conf)
			.enableHiveSupport()
			.getOrCreate()

		val topics = topics_input.split(",")

		/** 设置kafka消费者参数 */
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> bootstrap_servers_readfrom,
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> consumer_group,
			"auto.offset.reset" -> autoOffsetReset, //latest
			"enable.auto.commit" -> (false: java.lang.Boolean))

		/** 获取hbase表 */
		val table = getConnection(zkhosts_hbase).getTable(TableName.valueOf(tableName))

		val lines = createStreamingContextHbase(ssc, topics, kafkaParams, table)

		lines.foreachRDD(rdd => {
			if (!rdd.isEmpty()) {
				val dt = rdd.map { line => {
					initialMap
					parseJson(line.value.toString())
					jsontomap
				}
				}
				val value = dt.map { map =>
					Row(map.get("requestId"),
						map.get("mac"),
						map.get("terminalType"),
						map.get("deviceId"),
						map.get("areaCode"),
						map.get("userId"),
						map.get("ip"),
						map.get("hardVer"),
						map.get("softVer"),
						map.get("os"),
						map.get("appVer"),
						map.get("appId"),
						map.get("appProduct"),
						map.get("appChannel"),
						map.get("bizType"),
						map.get("eventId"),
						map.get("time"),
						map.get("sp"),
						map.get("upgradeType"),
						map.get("optState"),
						map.get("id"),
						map.get("name"),
						map.get("fromId"),
						map.get("fromName"),
						map.get("ids"),
						map.get("resType"),
						map.get("recomType"),
						map.get("recomPos"),
						map.get("sceneId"),
						map.get("traceId"),
						map.get("fromPlat"),
						map.get("duration"),
						map.get("process"),
						map.get("errCode"),
						map.get("errInfo"),
						map.get("epgName"),
						map.get("epgStartTime"),
						map.get("epgEndTime"),
						map.get("optType"),
						map.get("searchWords"),
						map.get("type"),
						map.get("optStatus"),
						map.get("appName"),
						map.get("dataInfo"),
						map.get("curr_time"))
				}
				val schema = StructType(Array(
					StructField("requestId", StringType, true),
					StructField("mac", StringType, true),
					StructField("terminalType", StringType, true),
					StructField("deviceId", StringType, true),
					StructField("areaCode", StringType, true),
					StructField("userId", StringType, true),
					StructField("ip", StringType, true),
					StructField("hardVer", StringType, true),
					StructField("softVer", StringType, true),
					StructField("os", StringType, true),
					StructField("appVer", StringType, true),
					StructField("appId", StringType, true),
					StructField("appProduct", StringType, true),
					StructField("appChannel", StringType, true),
					StructField("bizType", StringType, true),
					StructField("eventId", StringType, true),
					StructField("time", StringType, true),
					StructField("sp", StringType, true),
					StructField("upgradeType", StringType, true),
					StructField("optState", StringType, true),
					StructField("id", StringType, true),
					StructField("name", StringType, true),
					StructField("fromId", StringType, true),
					StructField("fromName", StringType, true),
					StructField("ids", StringType, true),
					StructField("resType", StringType, true),
					StructField("recomType", StringType, true),
					StructField("recomPos", StringType, true),
					StructField("sceneId", StringType, true),
					StructField("traceId", StringType, true),
					StructField("fromPlat", StringType, true),
					StructField("duration", StringType, true),
					StructField("process", StringType, true),
					StructField("errCode", StringType, true),
					StructField("errInfo", StringType, true),
					StructField("epgName", StringType, true),
					StructField("epgStartTime", StringType, true),
					StructField("epgEndTime", StringType, true),
					StructField("optType", StringType, true),
					StructField("searchWords", StringType, true),
					StructField("type", StringType, true),
					StructField("optStatus", StringType, true),
					StructField("appName", StringType, true),
					StructField("dataInfo", StringType, true),
					StructField("curr_time", StringType, true)
				))
				val df: DataFrame = session.createDataFrame(value, schema)
				df.createOrReplaceTempView("datatable")

				session.sql("set hive.exec.dynamic.partition.mode=nonstrict")
				session.sql(s"insert into $hiveTable partition(curr_time) select * from datatable")
			}
			//  保存新的 Offset
			storeOffSet(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, topics, table)
		})
		// 启动
		ssc.start()
		ssc.awaitTermination()
	}
	/** 获取hbase连接 */
	def getConnection(zkhosts_hbase: String) = {
		val hbaseConf = HBaseConfiguration.create()
		hbaseConf.set("hbase.zookeeper.quorum", zkhosts_hbase)
		val connection = ConnectionFactory.createConnection(hbaseConf)
		connection
	}
	/**
	  * 解析json格式数据，将所有的key-value存入一个map
	  */
	def parseJson(jsonStr: String): Unit = {
		val jsonObject: JSONObject = JSON.parseObject(jsonStr)

		val keySet = jsonObject.keySet

		import scala.collection.JavaConversions._
		for (k <- keySet) {
			val o: Any = jsonObject.get(k)
			if (o.isInstanceOf[JSONObject])
				try {
					val jsonObject1: JSONObject = o.asInstanceOf[JSONObject].getJSONObject(k)
					parseJson(jsonObject1.toString)
				} catch {
					case e: Exception =>
						val value: String = jsonObject.getString(k)
						parseJson(value)
				}
			else if (o.isInstanceOf[JSONArray]) try {
				val jsonArray: JSONArray = o.asInstanceOf[JSONArray]
				val length: Int = jsonArray.size
				var i: Int = 0
				while ( {
					i < length
				}) {
					val o1: Any = o.asInstanceOf[JSONArray].get(i)
					parseJson(o1.toString)

					{
						i += 1;
						i - 1
					}
				}
			} catch {
				case e: Exception =>
					jsontomap.put(k, o.toString)
			}
			else jsontomap.put(k, o.toString)
		}
	}
	/**
	  * 保存新的 OffSet
	  */
	def storeOffSet(ranges: Array[OffsetRange], topic: Array[String], table: Table) = {
		var map = new HashMap[String, String]
		ranges.map { x => (x.topic, x.partition.+("|" + x.untilOffset)) }.map {
			case (x) => {
				if (map.get(x._1) != null) map.put(x._1, map.get(x._1) + "," + x._2)
				else map.put(x._1, x._2)
			}
		}
		val arr = map.keySet().toArray()
		for (index <- 0 until (arr.length)) {
//			val put = new Put((s"${topic}_offset").getBytes)
			val topic = arr(index)
			val put = new Put((s"${topic}_offset").getBytes)
			put.addColumn("topicinfo".getBytes, "topic".getBytes, arr(index).toString() getBytes)
			put.addColumn("topicinfo".getBytes, "partition".getBytes, map.get(arr(index)).toString().getBytes)
			table.put(put)
		}
	}
	/**
	  * 得到历史 OffSet
	  */
	def getOffset(topics: Array[String], table: Table) = {
		var fromOffSets = scala.collection.mutable.LinkedHashMap[TopicPartition, Long]()
		for (i <- 0 until (topics.length)) {
			val topic = topics(i).toString
			val get = new Get((s"${topic}_offset").getBytes)
			val listget = table.get(get)
			if (listget.getRow != null) {
				var top: String = null
				var partitions: String = null
				if (listget.getValue("topicinfo".getBytes, "topic".getBytes) != null && listget.getValue("topicinfo".getBytes, "partition".getBytes) != null)
					top = new String(listget.getValue("topicinfo".getBytes, "topic".getBytes))
				partitions = new String(listget.getValue("topicinfo".getBytes, "partition".getBytes))
				val o = partitions.split(",")
				for (x <- 0 until (o.length)) {
					val pt = o(x).split("\\|")
					fromOffSets.put(new TopicPartition(top, Integer.parseInt(pt(0))), String.valueOf(pt(1)).toLong)
				}
			}
		}
		fromOffSets
	}
	/**
	  * 创建 DirectStream
	  */
	def createStreamingContextHbase(ssc: StreamingContext,
									topics: Array[String],
									kafkaParams: Map[String, Object],
									table: Table): InputDStream[ConsumerRecord[String, String]] = {
		var kafkaStreams: InputDStream[ConsumerRecord[String, String]] = null
		val offSets = getOffset(topics, table)
		if (!offSets.isEmpty) {
			kafkaStreams = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
				ConsumerStrategies.Subscribe(topics, kafkaParams, offSets))
		} else {
			kafkaStreams = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
				ConsumerStrategies.Subscribe(topics, kafkaParams))
		}
		kafkaStreams
	}
	/**
	  * 初始化map集合
	  */
	def initialMap() {
		jsontomap.clear
		jsontomap.put("requestId", "")
		jsontomap.put("mac", "")
		jsontomap.put("terminalType", "")
		jsontomap.put("deviceId", "")
		jsontomap.put("areaCode", "")
		jsontomap.put("userId", "")
		jsontomap.put("ip", "")
		jsontomap.put("hardVer", "")
		jsontomap.put("softVer", "")
		jsontomap.put("os", "")
		jsontomap.put("appVer", "")
		jsontomap.put("appId", "")
		jsontomap.put("appProduct", "")
		jsontomap.put("appChannel", "")
		jsontomap.put("bizType", "")
		jsontomap.put("eventId", "")
		jsontomap.put("time", "")
		jsontomap.put("sp", "")
		jsontomap.put("upgradeType", "")
		jsontomap.put("optState", "")
		jsontomap.put("id", "")
		jsontomap.put("name", "")
		jsontomap.put("fromId", "")
		jsontomap.put("fromName", "")
		jsontomap.put("ids", "")
		jsontomap.put("resType", "")
		jsontomap.put("recomType", "")
		jsontomap.put("recomPos", "")
		jsontomap.put("recomType", "")
		jsontomap.put("sceneId", "")
		jsontomap.put("traceId", "")
		jsontomap.put("fromPlat", "")
		jsontomap.put("duration", "")
		jsontomap.put("process", "")
		jsontomap.put("errCode", "")
		jsontomap.put("errInfo", "")
		jsontomap.put("epgName", "")
		jsontomap.put("epgStartTime", "")
		jsontomap.put("epgEndTime", "")
		jsontomap.put("optType", "")
		jsontomap.put("searchWords", "")
		jsontomap.put("type", "")
		jsontomap.put("optStatus", "")
		jsontomap.put("appName", "")
		jsontomap.put("dataInfo", "")
		jsontomap.put("curr_time", simpleDateFormat.format(new Date))
	}
	/**
	  * 转换时间格式:20190319172347227 --> 1552987427227
	  */
	def timeStampForamt(timeStamp: String): String = {
		if (timeStamp == null || timeStamp.isEmpty() || timeStamp.equals("null")) {
			return "未知"
		}
		val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
		val date = sdf.parse(timeStamp)
		val time = date.getTime
		return time.toString
	}
}
