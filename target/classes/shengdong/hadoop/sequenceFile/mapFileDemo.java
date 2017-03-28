package shengdong.hadoop.sequenceFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import shengdong.hadoop.HDFS.HDFSDao;

import java.io.IOException;
import java.net.URI;

/**
 * Created by ShengdongSHI on 2017/3/1.
 */
public class mapFileDemo {

    public static final String[] DATA = {
            "One , two , buckle my shoe",
            "Three , four , shut the door",
            "Five , six , pick up sticks",
            "Seven , eight , lay them straight",
            "Nine , ten, a big fat men"
    };

    public static final JobConf CONF = HDFSDao.getConf();


    /**
     * 向mapfile中写入数据
     * @param uri
     * @throws IOException
     */
    public void mapFileWriter(String uri) throws IOException {
        FileSystem fs = FileSystem.get(URI.create("uri"),CONF);
        IntWritable key = new IntWritable();
        Text value = new Text();

        MapFile.Writer mapWriter = null;
        try {
            mapWriter = new MapFile.Writer(CONF,fs,uri,key.getClass(),value.getClass());
            for(int i = 0;i < 60;i++){
                key.set(i + 1);
                value.set(DATA[i % DATA.length]);
                mapWriter.append(key,value);
            }
        } finally {
            IOUtils.closeStream(mapWriter);
            fs.close();
        }
    }


    /**
     * 读取mapfile
     * @param uri
     * @throws IOException
     */
    public void mapFileReader(String uri) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(uri),CONF);
        MapFile.Reader reader = null;

        try {
            reader = new MapFile.Reader(fs,uri,CONF);
            WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(),CONF);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),CONF);
            while (reader.next(key,value)){
                System.out.println(key + "\t" + value + "\n");
            }

        } finally {
            IOUtils.closeStream(reader);
            fs.close();
        }
    }

    public static void main(String[] args){
        String uri = args[0];
        mapFileDemo demo = new mapFileDemo();
        try {
            demo.mapFileWriter(uri);
            Thread.sleep(5000);
            demo.mapFileReader(uri);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
