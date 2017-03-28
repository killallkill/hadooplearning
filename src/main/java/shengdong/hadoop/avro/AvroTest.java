package shengdong.hadoop.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by ShengdongSHI on 2017/2/22.
 */
public class AvroTest
{

    private String baseUrl = System.getProperty("user.dir");
    private final String schemaPath = baseUrl + "/src/main/resources/";


    public void test() throws IOException {
        /**
         * 加载预定义的数据模式
         */
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("F:\\Hadooplearning\\src\\main\\resources\\StringPair.json"));

        /**
         * 创建一个avro对象实例
         */
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left",new Utf8("L"));
        datum.put("right",new Utf8("R"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        /**
         * datumwriter对象将数据翻译成encoder可以理解得对象类型，再由encoder写入到输出流
         * 向GenericDatumWriter对象传递模式，因为它需要根据模式来确定将数据对象中那些数值写入到输出流
         */
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        writer.write(datum,encoder);
        encoder.flush();
        out.close();

        /**
         * 反向读取对象
         */
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        Decoder decoder = DecoderFactory.get().createBinaryDecoder(out.toByteArray(),null);
        GenericRecord result = reader.read(null,decoder);

        assertThat(result.get("left").toString(),is("L"));
        assertThat(result.get("right").toString(),is("R"));

    }

    public static void main(String[] args){
        AvroTest test = new AvroTest();
        try {
            test.test();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
