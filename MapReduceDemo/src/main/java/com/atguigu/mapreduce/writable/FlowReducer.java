package com.atguigu.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : Zhaoheng Shi
 * @date : 2021/4/8 16:47
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1 遍历集合累加值
        long totalUpFlow = 0;
        long totalDownFlow = 0;

        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }

        // 2 封装outk, outv
        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow();

        // 3 写出
        context.write(key,outV);
    }
}
