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

import com.couchbase.kafka.CouchbaseProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * @author Sergey Avseyev
 */
public class ClickStreamExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickStreamExample.class);

    private static String couchbaseBucket;
    private static List<String> couchbaseNodes;
    private static String kafkaTopic;
    private static String kafkaNodes;
    private static String kafkaZookeeper;

    public static void main(String[] args) {
        initialize(args);
        CouchbaseProducer producer = new CouchbaseProducer(
                couchbaseNodes, couchbaseBucket, kafkaTopic, kafkaZookeeper);
        producer.run();
    }

    private static void initialize(String[] args) {
        try {
            InputStream config;
            if (args.length == 0) {
                config = ClickStreamExample.class.getResourceAsStream("/click_stream_example.properties");
            } else {
                config = new FileInputStream(new File(args[0]));
            }
            if (config != null) {
                System.getProperties().load(config);
                couchbaseBucket = stringPropertyOr("couchbaseBucket", "default");
                couchbaseNodes = splitNodes(stringPropertyOr("couchbaseNodes", "localhost"));
                kafkaTopic = stringPropertyOr("kafkaTopic", "default");
                kafkaZookeeper = stringPropertyOr("kafkaZookeeper", "localhost:2181");
            }
        } catch (IOException ex) {
            LOGGER.debug("Cannot load configuration", ex);
        }
    }

    private static String stringPropertyOr(String path, String def) {
        String found = System.getProperty("example." + path);
        return found == null ? def : found;
    }

    private static List<String> splitNodes(final String nodes) {
        if (nodes == null) {
            return null;
        } else {
            return Arrays.asList(nodes.split(","));
        }
    }
}
