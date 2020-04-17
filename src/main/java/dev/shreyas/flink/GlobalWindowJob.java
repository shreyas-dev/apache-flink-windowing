package dev.shreyas.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.util.Date;

/**
 * @author shreyas b
 * @created 17/04/2020 - 2:04 AM
 * @project flink-windows
 **/


public class GlobalWindowJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Read from 9090
        DataStream<String> data = env.socketTextStream("localhost", 9096);

        data.map(new MapFunction<String, Tuple3<String,Long,Long>>() {
            @Override
            public Tuple3<String,Long, Long> map(String s) throws Exception {
                String[] s1 = s.split(",");
                return new Tuple3<String,Long, Long>(s1[0],Long.parseLong(s1[1]),Long.parseLong(s1[2]));
            }
        })
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(10))
                .reduce(new ReduceFunction<Tuple3<String,Long, Long>>() {
                    @Override
                    public Tuple3<String,Long,Long> reduce(Tuple3<String,Long,Long> t1, Tuple3<String,Long, Long> t2) throws Exception {
                        return new Tuple3<>(t1.f0, t1.f1 > t2.f1 ? t2.f1:t1.f1, t1.f2 + t2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(Tuple3<String, Long, Long> t) throws Exception {
                        return new Tuple3<>(t.f0,new Date(t.f1).toString(),t.f2);
                    }
                }).print();
        env.execute("Session Window");
    }
}
