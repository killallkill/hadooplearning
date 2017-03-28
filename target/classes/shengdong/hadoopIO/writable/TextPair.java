package shengdong.hadoopIO.writable;

/**
 * Created by ShengdongSHI on 2017/2/15.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 该类用于表示一对字符串的实现
 *
 */
public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    public TextPair(){
        set(new Text(),new Text());
    }

    public TextPair(Text t1 ,Text t2){
        set(t1,t2);
    }

    public TextPair(String s1,String s2){
        set(new Text(s1),new Text(s2));
    }

    public void set(Text t1, Text t2){
        this.first = t1;
        this.second = t2;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if(cmp != 0){
            return cmp;
        }
        return second.compareTo(tp.second);
    }

    /**
     * 该函数将TextPair中的数据，依次序列化到输出流中
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    /**
     * 用于填充或查看各个字段的值
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    /**
     * MR中HashPartitioner通常用HashCode（）来选择分区，一个较好的hash函数
     * 能保证分区大小相似，故选择此函数
     * @return first.hashCode()*163 + second.hashCode()
     */
    @Override
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }

    /**
     * 比较两个text对象是否一致
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o){
        if(o instanceof TextPair){
            TextPair tp = (TextPair)o;
            return first.equals(tp.first) && second.equals(tp.second);
        }

        return false;
    }

    @Override
    public String toString(){
        return first+"\t"+second;
    }
}
