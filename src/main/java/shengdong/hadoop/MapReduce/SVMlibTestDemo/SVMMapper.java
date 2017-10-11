package shengdong.hadoop.MapReduce.SVMlibTestDemo;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by ShengdongSHI on 2017/4/7.
 */
public class SVMMapper extends Mapper<NullWritable,DoubleWritable,NullWritable,DoubleWritable> {

    private DoubleWritable dValue = new DoubleWritable();

    public void map(Object key ,DoubleWritable value, Context context){

    }

}
