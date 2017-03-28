package shengdong.hadoop.sequenceFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import shengdong.hadoop.HDFS.HDFSDao;

import java.io.IOException;
import java.net.URI;

/**
 * Created by ShengdongSHI on 2017/2/28.
 */
public class seqWriterDemo {

    public static final String[] DATA = {
            "One , two , buckle my shoe",
            "Three , four , shut the door",
            "Five , six , pick up sticks",
            "Seven , eight , lay them straight",
            "Nine , ten, a big fat men"
    };

    public static final JobConf CONF = HDFSDao.getConf();


    /**
     * 向seq文件中写入数据
     * @param desUri seq文件路径
     * @throws IOException
     */
    public void seqWriter(String desUri) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(desUri),CONF);
        Path desFile = new Path(desUri);
        IntWritable key = new IntWritable();
        Text value = new Text();

        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs,CONF,desFile,key.getClass(),value.getClass());
            for(int i = 0;i < 60 ;i++){
                key.set(60 - i);
                value.set(DATA[i % DATA.length]);
                writer.append(key,value);
            }
        } finally {
          IOUtils.closeStream(writer);
          fs.close();
        }

    }


    public void seqReader(String srcPath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(srcPath),CONF);
        Path srcFile = new Path(srcPath);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs,srcFile,CONF);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),CONF);
            Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(),CONF);
            long position = reader.getPosition();
            while (reader.next(key,value)){
                /**
                 * syncseen : 同步点，值数据读取的实例出错后能够再一次与记录边界同步的数据流中的一个位置
                 * 改点作为下一次读取的起始点
                 */
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.println("["+position+syncSeen+"]\t"+key+"\t"+value+"\n");
                //下一条记录的起点
                position = reader.getPosition();

            }
        } finally {
            IOUtils.closeStream(reader);
            fs.close();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String uri = args[0];
        seqWriterDemo demo = new seqWriterDemo();
        demo.seqWriter(uri);
        Thread.sleep(5000);
        demo.seqReader(uri);
    }
}
