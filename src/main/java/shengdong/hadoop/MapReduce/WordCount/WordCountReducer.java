package shengdong.hadoop.MapReduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ShengdongSHI on 2017/3/1.
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();


    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value : values){
            sum +=value.get();
        }
        result.set(sum);
        context.write(key,result);
    }

}
