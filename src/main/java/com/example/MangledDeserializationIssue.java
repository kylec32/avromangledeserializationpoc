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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class MangledDeserializationIssue {
    public static void main(String[] args) {
        ExampleRecord example = ExampleRecord.newBuilder()
                                            .setId("example_id")
                                            .build();
        var seralized = serialize(example);

        System.out.println(deserialize(seralized));
    }

    public static byte[] serialize(ExampleRecord data) {
        try {
            byte[] result = null;

            if (data != null) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

                DatumWriter<ExampleRecord> datumWriter = new SpecificDatumWriter<>(data.getSchema());
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

    public static ExampleRecord deserialize(byte[] data) {
        try {
            ExampleRecord result = null;
            if (data != null) {
                Schema schema = ExampleRecord.SCHEMA$;
                DatumReader<ExampleRecord> datumReader =
                        new SpecificDatumReader<>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                return datumReader.read(null, decoder);
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Can't deserialize data '" + Arrays.toString(data) + "'", e);
        }
    }
}
