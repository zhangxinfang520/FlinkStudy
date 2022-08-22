package com.six.chestnuts.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * @package:com.six.chestnuts.flink
 * @author: zxf_要努力
 * @file: Splited.py
 * @time: 2022/8/21 22:43
 */

//作用是将字符串分割后生成多个Tuple2实例，f0是分隔后的单词，f1等于1
public class Splited implements FlatMapFunction<String, Tuple2<String,Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        if(StringUtils.isNullOrWhitespaceOnly(s)){
            System.out.println("输入无效");
            return;
        }
        for (String key: s.split("\t")){
            collector.collect(new Tuple2<>(key,1));
        }
    }
}
