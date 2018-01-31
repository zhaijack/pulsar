/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class PatternTopicsConsumerImplTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private static final Logger log = LoggerFactory.getLogger(PatternTopicsConsumerImplTest.class);
    private final long ackTimeOutMillis = TimeUnit.SECONDS.toMillis(2);

    // TODO: mock zookeeper, mock httpclient, verify both way works well.
    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        //doReturn(new MockedZooKeeperClientFactoryImpl()).when(pulsar).getZooKeeperClientFactory();
    }

    @Override
    @AfterMethod
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = testTimeout)
    public void testGetConsumersAndGetTopics() throws Exception {

        String key = "PatternTopicsConsumerGet";
        final String subscriptionName = "my-ex-subscription-" + key;

        final String topicName1 = "persistent://prop/use/ns-abc/pattern-topic-1-" + key;
        final String topicName2 = "persistent://prop/use/ns-abc/pattern-topic-2-" + key;
        final String topicName3 = "persistent://prop/use/ns-abc/pattern-topic-3-" + key;
        Pattern pattern = Pattern.compile("pattern-topic*");
        String namespace = "prop/use/ns-abc";
        List<String> topicNames = Lists.newArrayList(topicName1, topicName2, topicName3);

        admin.properties().createProperty("prop", new PropertyAdmin());
        admin.persistentTopics().createPartitionedTopic(topicName2, 2);
        admin.persistentTopics().createPartitionedTopic(topicName3, 3);

        // 2. Create consumer
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setReceiverQueueSize(4);
        conf.setAckTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS);
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer = pulsarClient.subscribeAsync(namespace, pattern, subscriptionName, conf).get();
        assertTrue(consumer instanceof PatternTopicsConsumerImpl);

        List<String> topics = ((PatternTopicsConsumerImpl) consumer).getPartitionedTopics();
        List<ConsumerImpl> consumers = ((PatternTopicsConsumerImpl) consumer).getConsumers();

        topics.forEach(topic -> log.info("topic: {}", topic));
        consumers.forEach(c -> log.info("consumer: {}", c.getTopic()));

        IntStream.range(0, 6).forEach(index ->
            assertTrue(topics.get(index).equals(consumers.get(index).getTopic())));

        assertTrue(((PatternTopicsConsumerImpl) consumer).getTopics().size() == 3);

        consumer.unsubscribe();
        consumer.close();
    }
}
