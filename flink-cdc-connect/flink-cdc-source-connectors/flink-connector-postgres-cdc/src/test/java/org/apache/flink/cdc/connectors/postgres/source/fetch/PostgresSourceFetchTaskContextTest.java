/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source.fetch;

import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.TestHelper;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.postgresql.Utils.lastKnownLsn;

/** Unit test for {@link PostgresSourceFetchTaskContext}. */
class PostgresSourceFetchTaskContextTest {

    private static final Schema LOGICAL_MESSAGE_KEY_SCHEMA =
            SchemaBuilder.struct().field("prefix", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema VALUE_SCHEMA_WITH_OP =
            SchemaBuilder.struct()
                    .field(Envelope.FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    private PostgresConnectorConfig connectorConfig;
    private OffsetContext.Loader<PostgresOffsetContext> offsetLoader;

    @BeforeEach
    public void beforeEach() {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    @Test
    void shouldNotResetLsnWhenLastCommitLsnIsNull() {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, null);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);
        Assertions.assertThat(lastKnownLsn(offsetContext)).isEqualTo(Lsn.valueOf(12345L));
    }

    @Test
    void shouldEmitNonLogicalRecord() {
        PostgresSourceFetchTaskContext context = createContext(null);

        Assertions.assertThat(context.shouldEmit(recordWithOperation("c"))).isTrue();
    }

    @Test
    void shouldNotEmitLogicalMessageWhenPrefixesAreAbsent() {
        PostgresSourceFetchTaskContext context = createContext(null);

        Assertions.assertThat(context.shouldEmit(logicalMessageRecord("test_prefix"))).isFalse();
    }

    @Test
    void shouldNotEmitLogicalMessageWhenPrefixesAreEmpty() {
        PostgresSourceFetchTaskContext context = createContext(Collections.emptyList());

        Assertions.assertThat(context.shouldEmit(logicalMessageRecord("test_prefix"))).isFalse();
    }

    @Test
    void shouldEmitLogicalMessageWhenPrefixMatches() {
        PostgresSourceFetchTaskContext context = createContext(Collections.singletonList("test_"));

        Assertions.assertThat(context.shouldEmit(logicalMessageRecord("test_prefix"))).isTrue();
    }

    @Test
    void shouldNotEmitLogicalMessageWhenPrefixDoesNotMatch() {
        PostgresSourceFetchTaskContext context = createContext(Collections.singletonList("test_"));

        Assertions.assertThat(context.shouldEmit(logicalMessageRecord("other_prefix"))).isFalse();
    }

    @Test
    void shouldNotEmitLogicalMessageWhenRecordPrefixIsNull() {
        PostgresSourceFetchTaskContext context = createContext(Collections.singletonList("test_"));

        Assertions.assertThat(context.shouldEmit(logicalMessageRecord(null))).isFalse();
    }

    private static PostgresSourceFetchTaskContext createContext(
            List<String> logicalMessagePrefixes) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.database("postgres");
        configFactory.username("postgres");
        configFactory.password("postgres");
        configFactory.tableList("public.test_table");
        if (logicalMessagePrefixes != null) {
            configFactory.includeLogicalMessages(logicalMessagePrefixes);
        }

        PostgresSourceConfig sourceConfig = configFactory.create(0);
        return new PostgresSourceFetchTaskContext(sourceConfig, new PostgresDialect(sourceConfig));
    }

    private static SourceRecord logicalMessageRecord(String prefix) {
        Struct key = new Struct(LOGICAL_MESSAGE_KEY_SCHEMA).put("prefix", prefix);
        Struct value = new Struct(VALUE_SCHEMA_WITH_OP).put(Envelope.FieldName.OPERATION, "m");
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "topic",
                LOGICAL_MESSAGE_KEY_SCHEMA,
                key,
                VALUE_SCHEMA_WITH_OP,
                value);
    }

    private static SourceRecord recordWithOperation(String op) {
        Struct value = new Struct(VALUE_SCHEMA_WITH_OP).put(Envelope.FieldName.OPERATION, op);
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "topic",
                LOGICAL_MESSAGE_KEY_SCHEMA,
                null,
                VALUE_SCHEMA_WITH_OP,
                value);
    }
}
