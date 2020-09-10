package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {

	public static void main(String[] args) throws Exception {
		// 1.获取环境信息
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

		// 2.为环境信息添加WikipediaEditsSource源
		DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

		// 3.根据事件中的用户名为key来区分数据流
		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
			
			private static final long serialVersionUID = 585602866859299476L;

			

			@Override
			public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
				return wikipediaEditEvent.getUser();
			}
		});

		DataStream<Tuple2<String, Integer>> result = keyedEdits
				// 4.设置窗口时间为5s
				.timeWindow(Time.seconds(5))
				// 5.聚合当前窗口中相同用户名的事件，最终返回一个tuple2<user，累加的ByteDiff>
				.aggregate(
						new AggregateFunction<WikipediaEditEvent, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
							
							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<String, Integer> createAccumulator() {
								return new Tuple2<>("", 0);
							}

							@Override
							public Tuple2<String, Integer> add(WikipediaEditEvent value,
									Tuple2<String, Integer> accumulator) {
								return new Tuple2<>(value.getUser(), value.getByteDiff() + accumulator.f1);
							}

							@Override
							public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
								return accumulator;
							}

							@Override
							public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
								return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
							}
						});

		// 6.把tuple2映射为string
		result.map(new MapFunction<Tuple2<String, Integer>, String>() {

		
			private static final long serialVersionUID = -8366220656295092304L;

			@Override
			public String map(Tuple2<String, Integer> stringLongTuple2) throws Exception {
				return stringLongTuple2.toString();
			}
			// 7.sink数据到kafka，topic为wiki-result
		}).addSink(new FlinkKafkaProducer010<>("192.168.1.49:9092", "wiki-result", new SimpleStringSchema()));

		 result.print();
		// 8.执行操作
		see.execute();

	}
}
