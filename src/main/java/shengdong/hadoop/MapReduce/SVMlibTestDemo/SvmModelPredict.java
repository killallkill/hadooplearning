package shengdong.hadoop.MapReduce.SVMlibTestDemo;

import libsvm.svm;
import libsvm.svm_model;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * Created by ShengdongSHI on 2017/4/6.
 */
public class SvmModelPredict {

    public static Configuration getConf(){
        Configuration conf = new Configuration();
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/marped-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");
        return conf;
    }

    private String testFilePath;
    private String modelFilePath;
    private String resultPath;
    private Configuration conf;
    //定义hdfs路径
    public static final String HDFSPATH = "hdfs://192.168.118.100:9000";

    public SvmModelPredict(String tPath, String modelPath, String resultpath) {
        this.testFilePath = tPath;
        this.modelFilePath = modelPath;
        this.resultPath = resultpath;
        this.conf = getConf();
    }

    /**
     * 从hdfs读取model
     * @return 读取的model
     * @throws IOException
     */
    public svm_model getModel() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(this.HDFSPATH),this.conf);
        InputStream in = fs.open(new Path(this.modelFilePath));
        BufferedReader modelReader = new BufferedReader(new InputStreamReader(in));
        svm_model model = svm.svm_load_model(modelReader);
        return model;
    }


    public void predict() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(this.HDFSPATH),conf);
        InputStream dataIn = fs.open(new Path(this.testFilePath));
        OutputStream dataOut = fs.create(new Path(this.resultPath), new Progressable() {
            @Override
            public void progress() {
                System.out.println("#");
            }
        });
        BufferedReader dataPreditcReader = new BufferedReader(new InputStreamReader(dataIn));
        DataOutputStream resultOut = new DataOutputStream(dataOut);




    }

}
