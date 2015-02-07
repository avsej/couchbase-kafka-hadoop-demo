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

package com.couchbase.kafka;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.*;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
public class CouchbaseProducer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseProducer.class);
    private final KafkaEnvironment env;
    private final ClusterFacade core;
    private final Producer<String, String> producer;
    private final String kafkaTopic;
    private final String couchbaseBucket;
    private final List<String> couchbaseNodes;

    public CouchbaseProducer(final List<String> couchbaseNodes, final String couchbaseBucket,
                             final String kafkaTopic, final String kafkaZookeeper) {
        this(couchbaseNodes, couchbaseBucket, kafkaTopic, kafkaZookeeper,
                DefaultKafkaEnvironment.create());
    }

    public CouchbaseProducer(final List<String> couchbaseNodes, final String couchbaseBucket, final String kafkaTopic,
                             final String kafkaZookeeper, KafkaEnvironment env) {
        this.env = env;
        this.kafkaTopic = kafkaTopic;
        this.couchbaseBucket = couchbaseBucket;
        this.couchbaseNodes = couchbaseNodes;

        final Properties props = new Properties();
        ZkClient zkClient = new ZkClient(kafkaZookeeper, 4000, 6000, ZKStringSerializer$.MODULE$);
        List<String> brokerList = new ArrayList<>();
        Iterator<Broker> brokerIterator = ZkUtils.getAllBrokersInCluster(zkClient).iterator();
        while (brokerIterator.hasNext()) {
            Broker broker = brokerIterator.next();
            brokerList.add(broker.host() + ":" + broker.port());
        }

        props.put("metadata.broker.list", String.join(";", brokerList));
        props.put("serializer.class", env.kafkaValueSerializerClass());
        props.put("key.serializer.class", env.kafkaKeySerializerClass());
        final ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<String, String>(producerConfig);

        core = new CouchbaseCore(this.env);
    }

    @Override
    public String toString() {
        return "CouchbaseProducer{" +
                "couchbaseBucket=" + couchbaseBucket +
                ", kafkaTopic=" + kafkaTopic +
                '}';
    }

    @Override
    public void run() {
        core.send(new SeedNodesRequest(couchbaseNodes))
                .timeout(2, TimeUnit.SECONDS)
                .toBlocking()
                .single();
        core.send(new OpenBucketRequest(couchbaseBucket, ""))
                .timeout(2, TimeUnit.SECONDS)
                .toBlocking()
                .single();

        String streamName = couchbaseBucket + "->" + kafkaTopic;
        core.send(new OpenConnectionRequest(streamName, couchbaseBucket))
                .toList()
                .flatMap(couchbaseResponses -> partitionSize())
                .flatMap(this::requestStreams)
                .toBlocking()
                .forEach(this::postMessage);
    }

    protected void postMessage(DCPRequest request) {
        if (request instanceof MutationMessage) {
            MutationMessage mutation = (MutationMessage) request;
            KeyedMessage<String, String> message =
                    new KeyedMessage<>(kafkaTopic, mutation.key(),
                            mutation.content().toString(CharsetUtil.UTF_8));
            LOGGER.info("post document '{}'", mutation.key());
            producer.send(message);
        }
    }

    private Observable<Integer> partitionSize() {
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response
                                .config().bucketConfig(couchbaseBucket);
                        return config.numberOfPartitions();
                    }
                });
    }

    private Observable<DCPRequest> requestStreams(int numberOfPartitions) {
        return Observable.merge(
                Observable.range(0, numberOfPartitions)
                        .flatMap(new Func1<Integer, Observable<StreamRequestResponse>>() {
                            @Override
                            public Observable<StreamRequestResponse> call(Integer partition) {
                                return core.send(new StreamRequestRequest(partition.shortValue(), couchbaseBucket));
                            }
                        })
                        .map(new Func1<StreamRequestResponse, Observable<DCPRequest>>() {
                            @Override
                            public Observable<DCPRequest> call(StreamRequestResponse response) {
                                return response.stream();
                            }
                        })
        );
    }
}
