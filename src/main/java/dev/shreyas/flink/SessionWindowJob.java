package dev.shreyas.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

/**
 * @author shreyas b
 * @created 17/04/2020 - 12:48 AM
 * @project flink-windows
 **/


public class SessionWindowJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Read from 9090
        DataStream<String> data = env.socketTextStream("localhost", 9094);

        data.map(new MapFunction<String, Tuple2<Long,Long>>() {
            @Override
            public Tuple2<Long, Long> map(String s) throws Exception {
                String[] s1 = s.split(",");
                return new Tuple2<Long, Long>(Long.parseLong(s1[0]),Long.parseLong(s1[1]));
            }
        })
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
                .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long,Long> reduce(Tuple2<Long,Long> t1, Tuple2<Long, Long> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                }).print();
        env.execute("Session Window");
    }
}
