package com.zackehh.jackson.datatype.hadoop;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.io.*;

public class HadoopModule extends SimpleModule {

    public HadoopModule() {
        addSerializer(ArrayWritable.class, new HadoopSerializers.ArraySerializer());
        addSerializer(BooleanWritable.class, new HadoopSerializers.BooleanSerializer());
        addSerializer(ByteWritable.class, new HadoopSerializers.ByteSerializer());
        addSerializer(BytesWritable.class, new HadoopSerializers.BytesSerializer());
        addSerializer(DoubleWritable.class, new HadoopSerializers.DoubleSerializer());
        addSerializer(FloatWritable.class, new HadoopSerializers.FloatSerializer());
        addSerializer(IntWritable.class, new HadoopSerializers.IntSerializer());
        addSerializer(LongWritable.class, new HadoopSerializers.LongSerializer());
        addSerializer(MapWritable.class, new HadoopSerializers.MapSerializer());
        addSerializer(MD5Hash.class, new HadoopSerializers.MD5HashSerializer());
        addSerializer(NullWritable.class, new HadoopSerializers.NullSerializer());
        addSerializer(ShortWritable.class, new HadoopSerializers.ShortSerializer());
        addSerializer(Text.class, new HadoopSerializers.TextSerializer());
        addSerializer(VIntWritable.class, new HadoopSerializers.VIntSerializer());
        addSerializer(VLongWritable.class, new HadoopSerializers.VLongSerializer());
    }

}
