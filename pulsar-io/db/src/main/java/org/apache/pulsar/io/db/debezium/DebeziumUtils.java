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

import io.debezium.config.Configuration;
import org.apache.pulsar.io.db.debezium.DebeziumSourceConfig.SourceType;

public final class DebeziumUtils {

    private DebeziumUtils() {}

    public static String getDebeziumConnectorClass(SourceType type) {
        switch (type) {
            case MYSQL:
                return "io.debezium.connector.mysql.MySqlConnector";
            default:
                throw new IllegalArgumentException("Unknown debezium source type : " + type);
        }
    }

    public static Configuration createConfiguration(DebeziumSourceConfig config) {
        return Configuration.create()
            .with("connector.class",
                getDebeziumConnectorClass(config.getSourceType()))
            // TODO: change to pulsar topic based or bookkeeper based offset store
            .with("offset.storage",
                "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            .with("offset.storage.file.filename",
                "/tmp/pulsar-debezium-" + config.getSourceType() + "-offset.dat")
            .with("offset.flush.interval.ms", config.getOffsetFlushIntervalMs())
            .with("name", "pulsar-debezium-connector-" + config.getSourceType())
            .with("database.hostname", config.getDbHostname())
            .with("database.port", config.getDbPort())
            .with("database.user", config.getDbUser())
            .with("database.password", config.getDbPassword())
            .with("server.id", config.getServerId())
            .with("database.server.name", config.getDbServerName())
            // TODO: change to pulsar topic based or bookkeeper based database history store
            .with("database.history",
                "io.debezium.relational.history.FileDatabaseHistory")
            .with("database.history.file.filename",
                "/tmp/pulsar-debezimum-dbhistory-" + config.getSourceType() + ".dat")
            .build();
    }

}
