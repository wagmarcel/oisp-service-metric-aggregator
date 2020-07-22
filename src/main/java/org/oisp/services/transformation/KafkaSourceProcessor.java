/*
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.oisp.services.transformation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.oisp.services.conf.Config;

import java.util.HashMap;
import java.util.Map;


public class KafkaSourceProcessor {

    private KafkaIO.Read<String, byte[]> transform = null;

    public KafkaIO.Read<String, byte[]> getTransform() {
        return transform;
    }

    public KafkaSourceProcessor(Map<String, Object> userConfig, String topic) {


        String serverUri = userConfig.get(Config.KAFKA_BOOTSTRAP_SERVERS).toString();

        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put("group.id", "aggregator");
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        transform = KafkaIO.<String, byte[]>read()
                .withBootstrapServers(serverUri)
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withConsumerConfigUpdates(consumerProperties)
                .withReadCommitted()
                .commitOffsetsInFinalize();

    }
}
