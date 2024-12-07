package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import com.google.crypto.tink.subtle.Base64;

import java.util.Objects;

import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_VALUE_DESERIALIZER_SCHEMA;

public class KafkaSourceUtil {

  public static final String NATIVE_KAFKA_CONSUMER_GROUP_ID = "group.id";
  public static final int GROUP_ID_MAX_BYTES_LENGTH = 255;

  public static void configureSchemaDeserializer(SchemaProvider schemaProvider, TypedProperties props) {
    if (schemaProvider == null || Objects.isNull(schemaProvider.getSourceSchema())) {
      throw new HoodieReadFromSourceException("SchemaProvider has to be set to use KafkaAvroSchemaDeserializer");
    }
    props.put(KAFKA_VALUE_DESERIALIZER_SCHEMA.key(), schemaProvider.getSourceSchema().toString());
    // assign consumer group id based on the schema, since if there's a change in the schema we ensure KafkaRDDIterator doesn't use cached Kafka Consumer
    String groupId = props.getString(NATIVE_KAFKA_CONSUMER_GROUP_ID, "");
    String schemaHash = Base64.encode(HashID.hash(schemaProvider.getSourceSchema().toString(), HashID.Size.BITS_128));
    String updatedConsumerGroup = groupId.isEmpty() ? schemaHash
        : StringUtils.concatenateWithThreshold(String.format("%s_", groupId), schemaHash, GROUP_ID_MAX_BYTES_LENGTH);
    props.put(NATIVE_KAFKA_CONSUMER_GROUP_ID, updatedConsumerGroup);
  }
}
