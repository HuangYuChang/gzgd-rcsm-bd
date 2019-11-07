package com.gzgd.item

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * 将媒资数据从ftp导入hbase,再从hbase导入hdfs
  */
object HotItemFTP2HDFS {
	def main(args: Array[String]): Unit = {
		val Array(hbTableName, familyName, zookeeperQuorum, inputPath, fieldsStr, index, logLevel, regexA, outputPathPrefix) = args
		val session = SparkSession.builder().appName(s"${this.getClass.getSimpleName}").getOrCreate()
		val sc = session.sparkContext
		sc.setLogLevel(logLevel)
		// 判断hbase中的表是否已经存在，如果不存在则创建
		val configuration = sc.hadoopConfiguration
		configuration.set("hbase.zookeeper.quorum", zookeeperQuorum)
		configuration.set(TableInputFormat.INPUT_TABLE, hbTableName)

		val jobConf = new JobConf(configuration)
		// 指定key的纯输出类型
		jobConf.setOutputFormat(classOf[TableOutputFormat])
		// 设置输出表格的名称
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbTableName)
		val fields = fieldsStr.split(regexA)

		session.sparkContext.wholeTextFiles(inputPath).flatMap(x => x._2.split("\n")).map(x => x.split("\t", -1))
			.filter(x => x.length == fields.length).map(log => {
				val put = new Put(Bytes.toBytes(log(index.toInt)))
				for (i <- 0 until log.length) {
					put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(fields(i)), Bytes.toBytes(log(i)))
				}
				(new ImmutableBytesWritable(), put)
			}).saveAsHadoopDataset(jobConf)
		val calendar = Calendar.getInstance()
		calendar.setTime(new Date())
		calendar.add(Calendar.DAY_OF_YEAR, -1)
		val yesterday = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

		val hbaseRDD = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
		val arr = fieldsStr.split(regexA)
		hbaseRDD.map {
			case (cr) =>
				val strBu = new StringBuffer
				for (i <- 0 until (arr.length)) {
					strBu.append(Bytes.toString(cr._2.getValue(Bytes.toBytes(familyName), Bytes.toBytes(arr(i)))))
						.append("\t")
				}
				strBu
		}.repartition(1).saveAsTextFile(s"$outputPathPrefix/$yesterday")
		sc.stop()
		session.stop()
	}
}
