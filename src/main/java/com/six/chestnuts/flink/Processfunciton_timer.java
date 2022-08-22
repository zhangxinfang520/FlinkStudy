package com.six.chestnuts.flink;

import com.six.chestnuts.pojo.KeyAndValue;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.omg.CORBA.MARSHAL;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @package:com.six.chestnuts.flink
 * @author: zxf_要努力
 * @file: Processfunciton_timer.py
 * @time: 2022/8/21 22:47
 */
public class Processfunciton_timer {
    /**
     * KeyedProcessFunction的子类，作用是将每个key最新出现时间记录到backend，并创建定时器，
     * 定时器触发的时候，检查这个单词距离上次出现是否已经达到10秒，如果是，就发射给下游算子
     */
    static class CountKeyWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Long>> {

        private  ValueState<KeyAndValue> state;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态，name是myState
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", KeyAndValue.class));
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
            // 取得当前是哪个key
            Tuple currentKey = context.getCurrentKey();

            // 从backend取得当前单词的myState状态
            KeyAndValue current = state.value();
            // 如果myState还从未没有赋值过，就在此初始化

            if (current == null) {
                current = new KeyAndValue();
                current.key = value.f0;
            }
            // key数量加一
            current.value++;

            // 取当前元素的时间戳，作为该单词最后一次出现的时间
            current.lastmodified = context.timestamp();

            // 重新保存到backend，包括该单词出现的次数，以及最后一次出现的时间
            state.update(current);

            // 为当前key创建定时器，十秒后后触发
            long timer = current.lastmodified + 10000;
            context.timerService().registerProcessingTimeTimer(timer);
            // 打印所有信息，用于核对数据正确性
            System.out.println(String.format("process, %s, %d, lastModified : %d (%s), timer : %d (%s)\n\n",
                    currentKey.getField(0),
                    current.value,
                    current.lastmodified,
                    time(current.lastmodified),
                    timer,
                    time(timer)));
        }
        /**
         * 定时器触发后执行的方法
         * @param timestamp 这个时间戳代表的是该定时器的触发时间
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // 取得当前单词
            Tuple currentKey = ctx.getCurrentKey();

            // 取得该单词的myState状态
            KeyAndValue result = state.value();

            // 当前元素是否已经连续10秒未出现的标志
            boolean isTimeout = false;

            // timestamp是定时器触发时间，如果等于最后一次更新时间+10秒，就表示这十秒内已经收到过该单词了，
            // 这种连续十秒没有出现的元素，被发送到下游算子
            if (timestamp == result.lastmodified + 10000) {
                // 发送
                out.collect(new Tuple2<String, Long>(result.key, result.value));
                isTimeout = true;
            }

            // 打印数据，用于核对是否符合预期
            System.out.println(String.format("ontimer, %s, %d, lastModified : %d (%s), stamp : %d (%s), isTimeout : %s",
                    currentKey.getField(0),
                    result.value,
                    result.lastmodified,
                    time(result.lastmodified),
                    timestamp,
                    time(timestamp),
                    String.valueOf(isTimeout)));
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> socketDataStream = env.socketTextStream("localhost", 7777);
        // 所有输入的key，如果超过10秒没有再次出现，都可以通过CountKeyWithTimeoutFunction得到

        SingleOutputStreamOperator<Tuple2<String, Long>> result = socketDataStream
                //先进行切分
                .flatMap(new Splited())
                // 设置时间戳分配器，用当前时间作为时间戳
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Integer>>() {
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return null;
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2, long l) {
                        // 使用当前系统时间作为时间戳
                        return System.currentTimeMillis();
                    }
                }) // 将单词作为key分区
                .keyBy(0)
                // 按单词分区后的数据，交给自定义KeyedProcessFunction处理
                .process(new CountKeyWithTimeoutFunction());

        result.print("keyedStream");
        env.execute();
    }


        public static String time(long timeStamp) {
            return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(timeStamp));
        }



    }

