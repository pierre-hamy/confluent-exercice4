/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.developer;

import io.confluent.developer.interactivequery.MessageService;
import io.confluent.developer.streams.DetectMessageReceivedTransformer;
import io.confluent.developer.streams.DetectMissingResponseTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class Main {

    /**
     * This simple main function execute the topology and create a Kafka Streams
     * instance based on a configuration file provided as argument.
     * @param args First argumeent is mandatory - it is the path of the configuration file to load
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new RuntimeException("No Kafka client configuration file provided");
        }

        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));

        final StreamsBuilder builder = new StreamsBuilder();
        sendAlertIfMissingMessage(builder);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        MessageService messageService = new MessageService(streams);
        messageService.start();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            latch.await();
            messageService.stop();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static void sendAlertIfMissingMessage(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, Long>> inflightMessageStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("inflightMessageStore"),
                Serdes.String(),
                Serdes.Long())
                .withLoggingDisabled();

        builder.addStateStore(inflightMessageStore);

        builder
                .stream("message-sent", Consumed.with(Serdes.String(), Serdes.String()))
                .transform(() -> new DetectMissingResponseTransformer(), "inflightMessageStore")
                .to("missing-response-alert", Produced.with(Serdes.String(), Serdes.String()));

        builder
                .stream("message-received", Consumed.with(Serdes.String(), Serdes.String()))
                .transform(() -> new DetectMessageReceivedTransformer(), "inflightMessageStore");
    }


}
