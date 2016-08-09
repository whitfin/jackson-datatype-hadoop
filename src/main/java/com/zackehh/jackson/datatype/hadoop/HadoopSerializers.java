package com.zackehh.jackson.datatype.hadoop;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.Map;

class HadoopSerializers {

    static class ArraySerializer extends JsonSerializer<ArrayWritable> {

        @Override
        public void serialize(ArrayWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeStartArray();
            for (Writable w : value.get()) {
                jgen.writeObject(w);
            }
            jgen.writeEndArray();
        }

    }

    static class BooleanSerializer extends JsonSerializer<BooleanWritable> {

        @Override
        public void serialize(BooleanWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeBoolean(value.get());
        }

    }

    static class ByteSerializer extends JsonSerializer<ByteWritable> {

        @Override
        synchronized public void serialize(ByteWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

    static class BytesSerializer extends JsonSerializer<BytesWritable> {

        @Override
        public void serialize(BytesWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeBinary(value.getBytes(), 0, value.getCapacity());
        }

    }

    static class DoubleSerializer extends JsonSerializer<DoubleWritable> {

        @Override
        public void serialize(DoubleWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

    static class FloatSerializer extends JsonSerializer<FloatWritable> {

        @Override
        public void serialize(FloatWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

    static class IntSerializer extends JsonSerializer<IntWritable> {

        @Override
        public void serialize(IntWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

    static class LongSerializer extends JsonSerializer<LongWritable> {

        @Override
        public void serialize(LongWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

    static class MapSerializer extends JsonSerializer<MapWritable> {

        @Override
        public void serialize(MapWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeStartObject();
            for (Map.Entry e : value.entrySet()) {
                jgen.writeObjectField(e.getKey().toString(), e.getValue());
            }
            jgen.writeEndObject();
        }

    }

    static class MD5HashSerializer extends JsonSerializer<MD5Hash> {

        @Override
        public void serialize(MD5Hash value, JsonGenerator jgen, SerializerProvider serializers)
                throws IOException {
            jgen.writeObject(value.getDigest());
        }

    }

    static class NullSerializer extends JsonSerializer<NullWritable> {

        @Override
        public void serialize(NullWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNull();
        }

    }

    static class ShortSerializer extends JsonSerializer<ShortWritable> {

        @Override
        public void serialize(ShortWritable value, JsonGenerator jgen, SerializerProvider serializers)
                throws IOException {
            jgen.writeNumber(value.get());
        }
    }

    static class TextSerializer extends JsonSerializer<Text> {

        @Override
        public void serialize(Text value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeObject(value.toString());
        }

    }

    static class VIntSerializer extends JsonSerializer<VIntWritable> {

        @Override
        public void serialize(VIntWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

    static class VLongSerializer extends JsonSerializer<VLongWritable> {

        @Override
        public void serialize(VLongWritable value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeNumber(value.get());
        }

    }

}
