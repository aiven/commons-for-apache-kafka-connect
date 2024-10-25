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

package io.aiven.kafka.connect.s3.source.output;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutputWriter {

    Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);

    void configureValueConverter(Map<String, String> config, S3SourceConfig s3SourceConfig);

    List<Object> getRecords(InputStream inputStream, String topic, int topicPartition);

    byte[] getValueBytes(Object record, String topic, S3SourceConfig s3SourceConfig);
}