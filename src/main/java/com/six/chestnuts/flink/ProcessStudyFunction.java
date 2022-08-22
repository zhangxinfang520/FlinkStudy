package com.six.chestnuts.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @package:com.six.chestnuts.flink
 * @author: zxf_要努力
 * @file: ProcessStudyFunction.py
 * @time: 2022/8/21 21:23
 */
public class ProcessStudyFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //获取数据源
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
                for (int i = 0; i < 10; i++) {
                    String key_name = "zxf" + i;
                    Integer value = i;
                    //获取时间戳
                    Long time = System.currentTimeMillis();

                    System.out.printf("source,%s,%d,%d\n%n", key_name, value, time);

                    //发送元素 附上事件戳
                    sourceContext.collectWithTimestamp(new Tuple2<>(key_name, value), time);

                    Thread.sleep(10);

                }
            }

            @Override
            public void cancel() {

            }
        });

        // 过滤 value 大于2的 数据
        SingleOutputStreamOperator<String> outdata = dataStreamSource.process(new ProcessFunction<Tuple2<String, Integer>, String>() {
            @Override
            public void processElement(Tuple2<String, Integer> inputvalue, Context context, Collector<String> out) throws Exception {
                if (inputvalue.f1 > 2) {
                    out.collect(String.format("processElement，%s, %d, %d\n", inputvalue.f0, inputvalue.f1, context.timestamp()));
                }
            }
        });
        outdata.print("1:");
        env.execute();
    }
}
