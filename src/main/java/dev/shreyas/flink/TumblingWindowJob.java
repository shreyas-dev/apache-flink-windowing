/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.shreyas.flink;
import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Date;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

@Slf4j
public class TumblingWindowJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Read from 9090
		DataStream<String> data = env.socketTextStream("localhost", 9090);
		data.map(new MapFunction<String, Tuple2<Long,Integer>>() {
			@Override
			public Tuple2<Long, Integer> map(String s) throws Exception {
				String[] strings = s.split(",");
				return new Tuple2<Long, Integer>(Long.parseLong(strings[0]),Integer.parseInt(strings[1]));
			}
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Integer>>() {
			@Override
			public long extractAscendingTimestamp(Tuple2<Long, Integer> longStringTuple2) {
				return longStringTuple2.f0;
			}
		})
		.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<Tuple2<Long, Integer>>() {
					@Override
					public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> t1) throws Exception {
						return new Tuple2<>(longIntegerTuple2.f0 > t1.f0 ? longIntegerTuple2.f0 : t1.f0  , longIntegerTuple2.f1+t1.f1);
					}
				})
				.print();
		// execute program
		env.execute("TumblingWindow");
	}
}
