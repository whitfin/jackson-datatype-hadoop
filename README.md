# jackson-datatype-hadoop
[![Build Status](https://img.shields.io/travis/zackehh/jackson-datatype-hadoop.svg)](https://travis-ci.org/zackehh/jackson-datatype-hadoop)

This library provides bonus functionality to Jackson's serialization features by adding support for Hadoop datatypes such as `Text` and `IntWritable`. Everything in the public interface should be supported, so anything which isn't working is a bug (please file an issue!).

### Setup

`json-output-format` available on Maven central, via Sonatype OSS:

```
<dependency>
    <groupId>com.zackehh</groupId>
    <artifactId>jackson-datatype-hadoop</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Registering Module

To use Hadoop datatypes (Writable types), you need to register the module with your `ObjectMapper`:

```java
ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new HadoopModule());
```

### Contributions

If you find any issues or have any improvements, feel free to file issues and Pull Requests. As it stands, this lib has 100% coverage so it should be fairly stable. 