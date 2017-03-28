package shengdong.hadoop.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ShengdongSHI on 2017/3/1.
 */
public class MaxTemperature {
    public static void main(String[] args){
        if(args.length != 2){
            System.err.println("Usage : MaxTemperature <input path> <output path> ");
            System.exit(-1);
        }

        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf,"Max Temperature");
            job.setJarByClass(MaxTemperature.class);

            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            job.setMapperClass(MaxTemperatureMapper.class);
            job.setCombinerClass(MaxTemperatureReducer.class);
            job.setReducerClass(MaxTemperatureReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
