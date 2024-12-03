/*
 * Copyright 2024 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.source.input;

import static io.aiven.kafka.connect.common.config.SchemaRegistryFragment.SCHEMA_REGISTRY_URL;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;

import io.aiven.kafka.connect.common.config.SchemaRegistryFragment;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final public class TransformationUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformationUtils.class);

    private TransformationUtils() {
        // hidden
    }

    public static byte[] serializeAvroRecordToBytes(final List<GenericRecord> avroRecords, final String topic,
            final AbstractConfig sourceConfig) {
        final SchemaRegistryFragment registryFragment = new SchemaRegistryFragment(sourceConfig);
        final Map<String, String> config = Collections.singletonMap(SCHEMA_REGISTRY_URL,
                registryFragment.getSchemaRegistryUrl());

        try (KafkaAvroSerializer avroSerializer = (KafkaAvroSerializer) registryFragment.getAvroValueSerializer()
                .getDeclaredConstructor()
                .newInstance(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            avroSerializer.configure(config, false);
            for (final GenericRecord avroRecord : avroRecords) {
                out.write(avroSerializer.serialize(topic, avroRecord));
            }
            return out.toByteArray();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException
                | IOException e) {
            LOGGER.error("Error in reading s3 object stream for topic {} with error : {}", topic, e.getMessage(), e);
        }
        return new byte[0];
    }
}
