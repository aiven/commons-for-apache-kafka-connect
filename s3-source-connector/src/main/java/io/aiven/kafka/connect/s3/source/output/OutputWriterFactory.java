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

import static io.aiven.kafka.connect.s3.source.config.S3SourceConfig.OUTPUT_FORMAT_KEY;

import io.aiven.kafka.connect.s3.source.config.S3SourceConfig;

public final class OutputWriterFactory {

    private OutputWriterFactory() {
        // hidden
    }
    public static OutputWriter getWriter(final S3SourceConfig s3SourceConfig) {
        final OutputFormat outputFormatEnum = s3SourceConfig.getOutputFormat();
        switch (outputFormatEnum) {
            case AVRO :
                return new AvroWriter();
            case PARQUET :
                return new ParquetWriter();
            case JSON :
                return new JsonWriter();
            case BYTES :
                return new ByteArrayWriter();
            default :
                throw new IllegalArgumentException(
                        "Unknown output format " + s3SourceConfig.getString(OUTPUT_FORMAT_KEY));
        }
    }
}