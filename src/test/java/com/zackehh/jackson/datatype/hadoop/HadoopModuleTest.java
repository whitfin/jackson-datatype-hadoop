package com.zackehh.jackson.datatype.hadoop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.hadoop.io.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class HadoopModuleTest {

    private class HadoopMapper extends ObjectMapper {

        private HadoopMapper() {
            registerModule(new HadoopModule());
        }

    }

    private final HadoopMapper MAPPER = new HadoopMapper();

    @Test
    public void testArrayWritable() {
        Text one = new Text("one");
        Text two = new Text("two");
        Text three = new Text("three");

        ArrayWritable arrayWritable = new ArrayWritable(Text.class, new Writable[]{ one, two, three });

        JsonNode convertedNode = MAPPER.convertValue(arrayWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isArray());

        Assert.assertEquals(convertedNode.size(), 3);

        Assert.assertEquals(convertedNode.get(0).asText(), "one");
        Assert.assertEquals(convertedNode.get(1).asText(), "two");
        Assert.assertEquals(convertedNode.get(2).asText(), "three");
    }

    @Test
    public void testBooleanWritable() {
        BooleanWritable booleanWritable = new BooleanWritable(true);

        JsonNode convertedNode = MAPPER.convertValue(booleanWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isBoolean());
        Assert.assertTrue(convertedNode.asBoolean());
    }

    @Test
    public void testByteWritable() {
        byte b = 97;

        ByteWritable byteWritable = new ByteWritable(b);

        JsonNode convertedNode = MAPPER.convertValue(byteWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isNumber());
        Assert.assertEquals(convertedNode.asInt(), b);
    }

    @Test
    public void testBytesWritable() throws IOException {
        byte[] bytes = "this is input".getBytes();

        BytesWritable bytesWritable = new BytesWritable(bytes);

        JsonNode convertedNode = MAPPER.convertValue(bytesWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isBinary());
        Assert.assertEquals(convertedNode.binaryValue(), bytes);
    }

    @Test
    public void testDoubleWritable() {
        DoubleWritable doubleWritable = new DoubleWritable(15);

        JsonNode convertedNode = MAPPER.convertValue(doubleWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isDouble());
        Assert.assertEquals(convertedNode.asDouble(), 15.0);
    }

    @Test
    public void testFloatWritable() {
        FloatWritable floatWritable = new FloatWritable(15);

        JsonNode convertedNode = MAPPER.convertValue(floatWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isFloatingPointNumber());
        Assert.assertEquals(convertedNode.asDouble(), 15.0);
    }

    @Test
    public void testIntWritable() {
        IntWritable intWritable = new IntWritable(15);

        JsonNode convertedNode = MAPPER.convertValue(intWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isInt());
        Assert.assertEquals(convertedNode.asInt(), 15);
    }

    @Test
    public void testLongWritable() {
        LongWritable longWritable = new LongWritable(15);

        JsonNode convertedNode = MAPPER.convertValue(longWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isLong());
        Assert.assertEquals(convertedNode.asLong(), 15);
    }

    @Test
    public void testMapWritable() {
        MapWritable mapWritable = new MapWritable();

        mapWritable.put(new Text("array"), new ArrayWritable(Text.class, new Writable[]{ }));
        mapWritable.put(new Text("boolean"), new BooleanWritable(true));
        mapWritable.put(new Text("byte"), new ByteWritable((byte) 97));
        mapWritable.put(new Text("bytes"), new BytesWritable("this is input".getBytes()));
        mapWritable.put(new Text("double"), new DoubleWritable(1));
        mapWritable.put(new Text("float"), new FloatWritable(1));
        mapWritable.put(new Text("int"), new IntWritable(1));
        mapWritable.put(new Text("map"), new MapWritable());
        mapWritable.put(new Text("md5"), new MD5Hash(new byte[16]));
        mapWritable.put(new Text("null"), NullWritable.get());
        mapWritable.put(new Text("short"), new ShortWritable((short) 1));
        mapWritable.put(new Text("text"), new Text("this is input"));
        mapWritable.put(new Text("vint"), new VIntWritable(1));
        mapWritable.put(new Text("vlong"), new VLongWritable(1));

        JsonNode convertedNode = MAPPER.convertValue(mapWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isObject());
        Assert.assertEquals(convertedNode.size(), 14);

        JsonNodeFactory factory = JsonNodeFactory.instance;

        Assert.assertEquals(convertedNode.path("array"), factory.arrayNode());
        Assert.assertEquals(convertedNode.path("boolean"), factory.booleanNode(true));
        Assert.assertEquals(convertedNode.path("byte"), factory.binaryNode(new byte[]{ 97 }));
        Assert.assertEquals(convertedNode.path("bytes"), factory.binaryNode("this is input".getBytes()));

        Assert.assertEquals(convertedNode.path("double"), factory.numberNode(1));
        Assert.assertEquals(convertedNode.path("float"), factory.numberNode(1f));
        Assert.assertEquals(convertedNode.path("int"), factory.numberNode(1));
        Assert.assertEquals(convertedNode.path("map"), factory.objectNode());
        Assert.assertEquals(convertedNode.path("md5"), factory.binaryNode(new byte[16]));

        Assert.assertEquals(convertedNode.path("null"), factory.nullNode());
        Assert.assertEquals(convertedNode.path("short"), factory.numberNode((short) 1));
        Assert.assertEquals(convertedNode.path("text"), factory.textNode("this is input"));
        Assert.assertEquals(convertedNode.path("vint"), factory.numberNode(1));
        Assert.assertEquals(convertedNode.path("vlong"), factory.numberNode(1));
    }

    @Test
    public void testMD5Hash() throws IOException {
        MD5Hash md5Hash = new MD5Hash(new byte[16]);

        JsonNode convertedNode = MAPPER.convertValue(md5Hash, JsonNode.class);

        Assert.assertTrue(convertedNode.isBinary());
        Assert.assertEquals(convertedNode.binaryValue(), new byte[16]);
    }

    @Test
    public void testNullWritable() throws IOException {
        JsonNode convertedNode = MAPPER.convertValue(NullWritable.get(), JsonNode.class);

        Assert.assertTrue(convertedNode.isNull());
    }

    @Test
    public void testShortWritable() {
        ShortWritable shortWritable = new ShortWritable((short) 15);

        JsonNode convertedNode = MAPPER.convertValue(shortWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isIntegralNumber());
        Assert.assertEquals(convertedNode.asInt(), 15);
    }

    @Test
    public void testText() {
        Text text = new Text("this is input");

        JsonNode convertedNode = MAPPER.convertValue(text, JsonNode.class);

        Assert.assertTrue(convertedNode.isTextual());
        Assert.assertEquals(convertedNode.asText(), "this is input");
    }

    @Test
    public void testVIntWritable() {
        VIntWritable vIntWritable = new VIntWritable(15);

        JsonNode convertedNode = MAPPER.convertValue(vIntWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isInt());
        Assert.assertEquals(convertedNode.asInt(), 15);
    }

    @Test
    public void testVLongWritable() {
        VLongWritable vLongWritable = new VLongWritable(15);

        JsonNode convertedNode = MAPPER.convertValue(vLongWritable, JsonNode.class);

        Assert.assertTrue(convertedNode.isLong());
        Assert.assertEquals(convertedNode.asLong(), 15);
    }

}
