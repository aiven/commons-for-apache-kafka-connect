/*
 * Copyright 2020 Aiven Oy
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

package io.aiven.kafka.connect.common.grouper;

import io.aiven.kafka.connect.common.templating.Template;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class RecordGrouperFactoryTest {


    @Test
    void keyOnly() {
        final Template filenameTemplate = Template.of("{{key}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertEquals(RecordGrouperFactory.KEY_RECORD, grType);
    }

    @Test
    void topicPartitionAndKey() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{key}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        assertEquals(RecordGrouperFactory.KEY_TOPIC_PARTITION_RECORD, grType);
    }

    @Test
    void topicPartition() {
        final Template filenameTemplate = Template.of("{{topic}}/{{partition}}/{{start_offset}}");
        final String grType = RecordGrouperFactory.resolveRecordGrouperType(filenameTemplate);
        System.out.println(grType);
        assertEquals(RecordGrouperFactory.TOPIC_PARTITION_RECORD, grType);
    }
}
