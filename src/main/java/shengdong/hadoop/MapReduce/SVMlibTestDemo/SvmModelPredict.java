package shengdong.hadoop.MapReduce.SVMlibTestDemo;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.StringTokenizer;

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
    private svm_model model;
    private svm_node[] testDataNodes;
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
     * @throws IOException
     */
    public void getModel() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(this.HDFSPATH),this.conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(this.modelFilePath));
            BufferedReader modelReader = new BufferedReader(new InputStreamReader(in));
            this.model = svm.svm_load_model(modelReader);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 用于获取计算的结果标签
     * @return 返回计算的结果标签
     * @throws IOException
     */
    public void predict() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(this.HDFSPATH),conf);
        InputStream dataIn = fs.open(new Path(this.testFilePath));
        OutputStream dataOut = fs.create(new Path(this.resultPath), new Progressable() {
            @Override
            public void progress() {
                System.out.println("#");
            }
        });
        BufferedReader dataPreditcReader;
        DataOutputStream writer;
        dataPreditcReader = new BufferedReader(new InputStreamReader(dataIn));
        writer = new DataOutputStream(dataOut);

        String line;
//        Vector<Double> vResultLabel = new Vector<Double>();
        while(true){
            line = dataPreditcReader.readLine();
            System.out.println("----------------");
            System.out.println(line);
            if(line == null){
                break;
            }
            StringTokenizer lineSt = new StringTokenizer(line," \t\n\r\f:");
            double label = Double.valueOf(lineSt.nextToken()).doubleValue();
            int count = lineSt.countTokens();
            this.testDataNodes = new svm_node[count];
            for(int i = 0 ; i < count && lineSt.hasMoreTokens(); i++){
                this.testDataNodes[i] = new svm_node();
                this.testDataNodes[i].index = Integer.valueOf(lineSt.nextToken());
                this.testDataNodes[i].value = Double.valueOf(lineSt.nextToken()).doubleValue();
                System.out.println("-----");
                System.out.println(this.testDataNodes[i].index + "\t" + this.testDataNodes[i].value);
            }
            if(this.model != null && this.testDataNodes != null && this.testDataNodes.length == count) {
                Double result = svm.svm_predict(this.model, this.testDataNodes);
                writer.writeBytes(result + "\t" + line.toString() + "\n");
                writer.flush();
            }
        }
        dataPreditcReader.close();
        dataIn.close();
        writer.flush();
        writer.close();
        dataOut.close();

    }

}
