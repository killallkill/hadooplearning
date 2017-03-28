import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import shengdong.hadoop.avro.StringPair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by ShengdongSHI on 2017/2/22.
 */
public class avrotest {
    public avrotest() {
    }

    public void testavro() throws IOException {
        StringPair pair = new StringPair();
        pair.setLeft("L");
        pair.setRight("R");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out,null);
        writer.write(pair,encoder);
        encoder.flush();
        out.close();

        DatumReader<StringPair> reader = new SpecificDatumReader<StringPair>(StringPair.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),null);
        StringPair result = reader.read(null,decoder);
        assertThat("test left",result.left.toString(),is("L"));
        assertThat("test right",result.right.toString(),is("R"));
    }

    public static void main(String[] args){
        avrotest test = new avrotest();
        try {
            test.testavro();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
