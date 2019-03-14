package com.lkq.flink.word

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
object test {
	def main(args: Array[String]): Unit = {
		var hostname: String = "47.100.177.21"
		var port: Int = 9091
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

		val windowCounts = text.flatMap{ w => refArrayOps(w.split("\\s"))}
			.map{ w => WordWithCount(w, 1)}
  		.keyBy("word")
  		.timeWindow(Time.seconds(5))
  		.sum("count")

		windowCounts.print().setParallelism(1)
		env.execute("Flink Window WordCount")
	}

	case class  WordWithCount(word: String, count: Long)

}
