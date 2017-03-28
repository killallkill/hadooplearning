package shengdong.hadoop.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * Created by ShengdongSHI on 2017/2/23.
 */
public class HDFSDao {

    //定义hdfs路径
    public static final String HDFSPATH = "hdfs://192.168.118.100:9000";

    //定义配置conf和文件path
    private String hdfsPath;
    private Configuration conf;

    public HDFSDao(String path, Configuration conf) {
        this.hdfsPath = path;
        this.conf = conf;
    }

    public HDFSDao(Configuration conf) {
        this.conf = conf;
        this.hdfsPath = HDFSPATH;
    }

    /**
     * 装载hadoop配置文件
     * @return conf配置
     */
    public static JobConf getConf(){
        JobConf conf = new JobConf();
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/marped-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");
        return conf;
    }

    /**
     * 从本地拷贝文件到hdfs
     * @param localPath 本地文件路径
     * @param remotePath hdfs文件路径
     * @throws IOException 文件读取异常
     */
    public void upload(String localPath, String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyFromLocalFile(new Path(localPath),new Path(remotePath));
        System.out.println("Copy from loacl :" + localPath + " to remote :"+ remotePath);
        fs.close();
    }

    /**
     * 从hdfs拷贝文件到本地
     * @param localPath 本地路径
     * @param remotePath
     * @throws IOException
     */
    public void download(String localPath, String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyToLocalFile(new Path(remotePath),new Path(localPath));
        System.out.println("Copy from remote : " + remotePath + " to local : "+ localPath);
        //文件读取完毕需要关闭读取流
        fs.close();
    }


    /**
     * 向hdfs文件中写入数据
     * @param uri hdfs文件路径
     * @param content 写入内容
     * @throws IOException
     */
    public void writeContentToHDFSFile(String uri , String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        //定义执行进度显示，以“.”为显示标识
        OutputStream out = fs.create(new Path(uri), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        out.write(content.getBytes("UTF-8"),0,content.length());
        out.flush();
        out.close();
    }

    /**
     * 从本地文件写入数据到hdfs
     * @param local 本地文件路径
     * @param remote hdfs路径
     * @throws IOException
     */
    public void writeLocalFileToHDFSFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(remote),conf);

        InputStream localIn = new BufferedInputStream(new FileInputStream(local));
        OutputStream out = fs.create(new Path(remote), new Progressable() {
            @Override
            public void progress() {
                System.out.println("#");
            }
        });

        IOUtils.copyBytes(localIn,out,4096,true);
    }

    /**
     * 在hdfs中创建目录
     * @param uri 文件创建路径
     * @throws IOException
     */
    public void mkdir(String uri) throws IOException {
        Path file = new Path(uri);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        if(!fs.exists(file)){
            fs.mkdirs(file);
            System.out.println(uri + " is created.");
        }else {
            System.out.println(uri + " is exists.");
        }

        fs.close();
    }

    /**
     * 删除文件或目录
     * @param fileUri 删除文件路径
     * @throws IOException
     */
    public void deleteFile(String fileUri) throws IOException {
        Path dFile = new Path(fileUri);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        boolean delResult = fs.deleteOnExit(dFile);
        if(delResult){
            System.out.println(fileUri + " is deleted.");
        }else {
            System.out.println(fileUri + " can't be deleted. Or it's not exist");
        }
        fs.close();
    }

    /**
     * cat 命令执行
     * @param fileUri
     * @return
     * @throws IOException
     */
    public String hdfsCat(String fileUri) throws IOException {
        Path remoteFile = new Path(fileUri);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);

        System.out.println("Cat " + fileUri);
        FSDataInputStream fsin = null;
        OutputStream baos = new ByteArrayOutputStream();
        String str = null;

        try {
            fsin = fs.open(remoteFile);
            IOUtils.copyBytes(fsin,baos,4096,true);
            str = baos.toString();
        } finally {
          IOUtils.closeStream(fsin);
          fs.close();
        }
        System.out.println(str);
        return str;
    }

    public void ls(String fileUri) throws IOException {
        Path filePath = new Path(fileUri);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        FileStatus[] fileStatuses = fs.listStatus(filePath);

        String filename = null ,filepermission = null;
        long filesize ;
        System.out.println("ls " + fileUri);
        System.out.println("-----------------------------------");
        for(FileStatus f : fileStatuses){
            filename = f.getPath().toString();
            filepermission = f.getPermission().toString();
            filesize = f.getLen();
            System.out.println("Name : "+filename+" Size : "+filesize+" Permission : "+filepermission);
        }
        System.out.println("-----------------------------------");
        fs.close();
    }

    public static void main(String[] args){
        JobConf conf = getConf();
        HDFSDao hdfsDao = new HDFSDao(conf);
        String srcPath = null;
        String desPath = null;

        if(args.length == 2){
            srcPath = args[0];
            desPath = args[1];
        }else if(args.length == 1){
            srcPath = args[0];
            desPath = hdfsDao.hdfsPath+"/";
        }

        try {
            hdfsDao.deleteFile("test");
            hdfsDao.mkdir("test");
            if(srcPath != null && desPath != null) {
                hdfsDao.upload(srcPath, desPath);
            }
            hdfsDao.ls("/test");
            hdfsDao.hdfsCat(hdfsDao.hdfsPath+"/test/test.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
