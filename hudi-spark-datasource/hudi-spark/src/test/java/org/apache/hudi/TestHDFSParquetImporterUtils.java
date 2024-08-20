/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hudi.cli.HDFSParquetImporterUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHDFSParquetImporterUtils {

  @Test
  void testReplaceEnumsInRecord() {
    String schemaStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"ComplexUserProfile\","
        + "\"namespace\": \"com.example\","
        + "\"fields\": ["
        + "    {\"name\": \"username\", \"type\": \"string\"},"
        + "    {\"name\": \"status\", \"type\": {"
        + "        \"type\": \"enum\","
        + "        \"name\": \"Status\","
        + "        \"symbols\": [\"ACTIVE\", \"INACTIVE\", \"BANNED\"]"
        + "    }},"
        + "    {\"name\": \"preferences\", \"type\": {"
        + "        \"type\": \"map\","
        + "        \"values\": {"
        + "            \"type\": \"enum\","
        + "            \"name\": \"PreferenceType\","
        + "            \"symbols\": [\"LOW\", \"MEDIUM\", \"HIGH\"]"
        + "        }"
        + "    }},"
        + "    {\"name\": \"notifications\", \"type\": {"
        + "        \"type\": \"array\","
        + "        \"items\": {"
        + "            \"type\": \"enum\","
        + "            \"name\": \"NotificationType\","
        + "            \"symbols\": [\"EMAIL\", \"SMS\", \"PUSH\"]"
        + "        }"
        + "    }}"
        + "]"
        + "}";
    Schema schema = new Schema.Parser().parse(schemaStr);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    recordBuilder.set("username", "John Smith");
    recordBuilder.set("status", "ACTIVE");

    Map<String, String> preferences = new HashMap<>();
    preferences.put("volume", "LOW");
    recordBuilder.set("preferences", preferences);

    GenericData.Array<String> notifications = new GenericData.Array<>(3, schema.getField("notifications").schema());
    notifications.add("EMAIL");
    notifications.add("SMS");
    notifications.add("PUSH");
    recordBuilder.set("notifications", notifications);

    GenericRecord transformedRecord = HDFSParquetImporterUtils.replaceEnumsInRecord(recordBuilder.build());
    assertTrue(transformedRecord.get("status") instanceof GenericEnumSymbol);

    Object preferencesVal = transformedRecord.get("preferences");
    assertTrue(((Map) preferencesVal).get("volume") instanceof GenericEnumSymbol);

    Object notificationsVal = transformedRecord.get("notifications");
    assertTrue(((GenericData.Array<?>) notificationsVal).get(0) instanceof GenericEnumSymbol);
    assertTrue(((GenericData.Array<?>) notificationsVal).get(1) instanceof GenericEnumSymbol);
    assertTrue(((GenericData.Array<?>) notificationsVal).get(2) instanceof GenericEnumSymbol);
  }
}
