/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package example;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * @author Sergey Avseyev
 */
public class ClickStreamConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickStreamConsumer.class);
    private static String kafkaTopic;
    private static String kafkaZookeeper;
    private static String defaultFS;

    public static void main(String[] args) {
        initialize(args);
        run();
    }

    private static void run() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOGGER.warn("IOException during opening filesystem", e);
            System.exit(1);
        }

        Properties props = new Properties();
        props.put("zookeeper.connect", kafkaZookeeper);
        props.put("group.id", "clickstream");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(kafkaTopic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(kafkaTopic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> event = it.next();
            Path filename = new Path("couchbase/" + new String(event.key()));
            try {
                if (fs.exists(filename)) {
                    fs.delete(filename, true);
                }
                FSDataOutputStream out = fs.create(filename);
                out.write(event.message());
                out.close();
                LOGGER.info("Written file: {}", filename.toUri().toString());
            } catch (IOException e) {
                LOGGER.warn("IOException during operation", e);
                System.exit(1);
            }
        }

    }

    private static void initialize(String[] args) {
        try {
            InputStream config;
            if (args.length == 0) {
                config = ClickStreamProducer.class.getResourceAsStream("/click_stream_example.properties");
            } else {
                config = new FileInputStream(new File(args[0]));
            }
            if (config != null) {
                System.getProperties().load(config);
                kafkaTopic = Util.stringPropertyOr("kafkaTopic", "default");
                kafkaZookeeper = Util.stringPropertyOr("kafkaZookeeper", "localhost:2181");
                defaultFS = Util.stringPropertyOr("defaultFS", "localhost:8020");

            }
        } catch (IOException ex) {
            LOGGER.debug("Cannot load configuration", ex);
        }
    }
}