package shengdong.hadoop.MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ShengdongSHI on 2017/3/1.
 */
public class MaxTemperatureReducer extends Reducer<LongWritable,Text,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        int val = 0;
        for(IntWritable value : values){
            val = value.get();
            if(maxValue <= val){
                maxValue = val;
            }
        }
        result.set(maxValue);

        context.write(key,result);
    }
}
