package com.lkq.flink.word;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * @Author lou
 */
public class wikipediaAnalysis {
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy((KeySelector<WikipediaEditEvent, String>) WikipediaEditEvent::getUser);

		DataStream<Tuple2<String, Integer>> result = keyedEdits.timeWindow(Time.seconds(5))
				.aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> createAccumulator() {
						return new Tuple2<>("", 0);
					}
					@Override
					public Tuple2<String, Integer> add(WikipediaEditEvent wikipediaEditEvent, Tuple2<String, Integer> accumulator) {
						return new Tuple2<>(wikipediaEditEvent.getUser(),
								wikipediaEditEvent.getByteDiff() + accumulator.f1);
					}
					@Override
					public Tuple2<String, Integer> getResult(Tuple2<String, Integer> o) {
						return o;
					}
					@Override
					public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
						return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
					}
				});
		result.map((MapFunction<Tuple2<String, Integer>, String>) Tuple2::toString).addSink(new FlinkKafkaProducer<>("47.100.177.21:9092", "wikiresult", new SimpleStringSchema()));

		env.execute();
	}
}
