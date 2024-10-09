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

package io.aiven.kafka.connect.s3.source.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;
import io.aiven.kafka.connect.s3.source.output.OutputWriter;

public final class RecordProcessor {

    private RecordProcessor() {

    }
    public static List<SourceRecord> processRecords(final Iterator<List<AivenS3SourceRecord>> sourceRecordIterator,
            final List<SourceRecord> results, final S3SourceConfig s3SourceConfig,
            final Optional<Converter> keyConverter, final Converter valueConverter,
            final AtomicBoolean connectorStopped, final OutputWriter outputWriter) {

        final Map<String, String> conversionConfig = new HashMap<>();
        final int maxPollRecords = s3SourceConfig.getInt(S3SourceConfig.MAX_POLL_RECORDS);

        for (int i = 0; sourceRecordIterator.hasNext() && i < maxPollRecords && !connectorStopped.get(); i++) {
            final List<AivenS3SourceRecord> recordList = sourceRecordIterator.next();
            final List<SourceRecord> sourceRecords = createSourceRecords(recordList, s3SourceConfig, keyConverter,
                    valueConverter, conversionConfig, outputWriter);
            results.addAll(sourceRecords);
        }

        return results;
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    static List<SourceRecord> createSourceRecords(final List<AivenS3SourceRecord> aivenS3SourceRecordList,
            final S3SourceConfig s3SourceConfig, final Optional<Converter> keyConverter, final Converter valueConverter,
            final Map<String, String> conversionConfig, final OutputWriter outputWriter) {

        final List<SourceRecord> sourceRecordList = new ArrayList<>();
        for (final AivenS3SourceRecord aivenS3SourceRecord : aivenS3SourceRecordList) {
            final String topic = aivenS3SourceRecord.getToTopic();
            final Optional<SchemaAndValue> keyData = keyConverter
                    .map(c -> c.toConnectData(topic, aivenS3SourceRecord.key()));

            outputWriter.configureValueConverter(conversionConfig, s3SourceConfig);
            valueConverter.configure(conversionConfig, false);
            final SchemaAndValue schemaAndValue = valueConverter.toConnectData(topic, aivenS3SourceRecord.value());

            sourceRecordList.add(aivenS3SourceRecord.getSourceRecord(topic, keyData, schemaAndValue));
        }

        return sourceRecordList;
    }
}