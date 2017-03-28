package shengdong.hadoop.MapReduce.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ShengdongSHI on 2017/3/1.
 */
public class WordCountMapper extends Mapper<Object,Text,Text,IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        while (st.hasMoreTokens()){
            word.set(st.nextToken());
            context.write(word,ONE);
        }
    }
}
