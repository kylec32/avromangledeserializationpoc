package com.example;

import com.example.int$.ExampleRecord;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class MangledDeserializationIssue {
    public static void main(String[] args) {
        ExampleRecord example = new ExampleRecord().newBuilder()
                                                    .setId("example_id")
                                                    .build();
        var seralized = serialize(example);

        System.out.println(deserialize(ExampleRecord.class, seralized));
    }

    public static <T extends SpecificRecordBase> byte[] serialize(T data) {
        try {
            byte[] result = null;

            if (data != null) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

                DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
                datumWriter.write(data, encoder);

                encoder.flush();
                byteArrayOutputStream.close();

                result = byteArrayOutputStream.toByteArray();
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize data='" + data + "'", e);
        }
    }

    public static <T extends SpecificRecordBase> T deserialize(Class<? extends T> clazz, byte[] data) {
        try {
            T result = null;
            if (data != null) {
                Schema schema = clazz.newInstance().getSchema();
                DatumReader<T> datumReader =
                        new SpecificDatumReader<>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                Object blah = datumReader.read(null, decoder);
                result = (T)blah;
            }
            return result;
        } catch (InstantiationException | IllegalAccessException | IOException e) {
            throw new RuntimeException("Can't deserialize data '" + Arrays.toString(data) + "'", e);
        }
    }
}
