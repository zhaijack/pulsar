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
package org.apache.pulsar.io.db.debezium;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.EmbeddedEngine.ConnectorCallback;
import io.debezium.embedded.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A db source connector implementation based on <tt>Debezium</tt>.
 */
@Slf4j
public class DebeziumSource implements Source<Object>, ConnectorCallback {

    static class DebeziumRecord implements Record<Object> {

        private final SourceRecord record;
        private final AvroData avroData;

        DebeziumRecord(SourceRecord record, AvroData avroData) {
            this.record = record;
            this.avroData = avroData;
        }

        @Override
        public Optional<String> getKey() {
            Object key = avroData.fromConnectData(record.keySchema(), record.key());
            AvroSchema avroSchema = AvroSchema.of(key.getClass());
            byte[] keyData = avroSchema.encode(key);
            return Optional.of(new String(keyData, UTF_8));
        }

        @Override
        public Object getValue() {
            return avroData.fromConnectData(
                record.valueSchema(), record.value());
        }

        @Override
        public Optional<String> getPartitionId() {
            return Optional.empty();
        }

        @Override
        public Optional<Long> getRecordSequence() {
            return Optional.empty();
        }

        @Override
        public Map<String, String> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public void ack() {
            // no-op
        }

        @Override
        public void fail() {
            // no-op
        }
    }

    private LinkedBlockingQueue<Record<Object>> recordQueue;
    private ExecutorService engineExecutor;
    private EmbeddedEngine engine;
    private DebeziumSourceConfig srcConfig;
    private AvroData avroData;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext)
        throws Exception {
        this.srcConfig = DebeziumSourceConfig.load(config);
        Configuration debeziumConf = DebeziumUtils.createConfiguration(srcConfig);

        this.avroData = new AvroData(new AvroDataConfig(config));
        this.recordQueue = new LinkedBlockingQueue<>(srcConfig.getRecordQueueSize());
        this.engine = EmbeddedEngine.create()
            .using(debeziumConf)
            .using(Clock.system())
            .using(this)
            .using((success, message, throwable) -> {
                if (success) {
                    log.info("Successfully stop debezium source : {}", message);
                } else {
                    log.error("Encountered error stop debezium source - {} : ",
                        message, throwable);
                }
            })
            .using(OffsetCommitPolicy.periodic(debeziumConf))
            .notifying(this::handleDbRecord)
            .build();
        this.engineExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("debezium-connector-engine-%d")
                .build());
        this.engineExecutor.submit(engine);
    }

    public void handleDbRecord(SourceRecord sourceRecord) {
        try {
            recordQueue.put(new DebeziumRecord(sourceRecord, avroData));
        } catch (InterruptedException e) {
            log.warn("Interrupted at inserting source record : ", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted at inserting source record");
        }
    }

    @Override
    public Record<Object> read() throws Exception {
        return recordQueue.take();
    }

    @Override
    public void close() {
        if (null != engine) {
            engine.stop();
        }
        if (null != engineExecutor) {
            engineExecutor.shutdown();
        }
    }

    @Override
    public void connectorStarted() {
        log.info("Debezium connector started with configuration : {}", srcConfig);
    }

    @Override
    public void connectorStopped() {
        log.info("Debezium connector stopped.");
    }

    @Override
    public void taskStarted() {
        log.info("Debezium connector task started.");
    }

    @Override
    public void taskStopped() {
        log.info("Debezium connector task stopped.");
    }
}
