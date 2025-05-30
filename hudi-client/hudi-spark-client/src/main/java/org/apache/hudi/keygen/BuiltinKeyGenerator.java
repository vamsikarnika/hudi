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

package org.apache.hudi.keygen;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.client.utils.SparkRowSerDe;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import scala.Function1;

import static org.apache.hudi.common.util.CollectionUtils.tail;

/**
 * Base class for all built-in key generators.
 *
 * NOTE: By default it implements all the methods of {@link SparkKeyGeneratorInterface}, which
 *       by default however fallback to Avro implementation. For maximum performance (to avoid
 *       conversion from Spark's internal data-types to Avro) you should override these methods
 *       in your implementation.
 *
 * TODO rename to AvroFallbackBaseKeyGenerator
 */
@ThreadSafe
public abstract class BuiltinKeyGenerator extends BaseKeyGenerator implements SparkKeyGeneratorInterface {

  private static final Logger LOG = LoggerFactory.getLogger(BuiltinKeyGenerator.class);

  protected static final String FIELDS_SEP = ",";

  protected static final UTF8String NULL_RECORD_KEY_PLACEHOLDER_UTF8 = UTF8String.fromString(NULL_RECORDKEY_PLACEHOLDER);
  protected static final UTF8String EMPTY_RECORD_KEY_PLACEHOLDER_UTF8 = UTF8String.fromString(EMPTY_RECORDKEY_PLACEHOLDER);

  protected transient volatile SparkRowConverter rowConverter;
  protected transient volatile SparkRowAccessor rowAccessor;

  protected transient volatile StringPartitionPathFormatter stringPartitionPathFormatter;
  protected transient volatile UTF8StringPartitionPathFormatter utf8StringPartitionPathFormatter;

  protected BuiltinKeyGenerator(TypedProperties config) {
    super(config);
  }

  @Override
  public String getRecordKey(Row row) {
    tryInitRowConverter(row.schema());
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    return getRecordKey(rowConverter.convertToAvro(row));
  }

  @Override
  public UTF8String getRecordKey(InternalRow internalRow, StructType schema) {
    tryInitRowConverter(schema);
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    return UTF8String.fromString(getRecordKey(rowConverter.convertToAvro(internalRow)));
  }

  @Override
  public String getPartitionPath(Row row) {
    tryInitRowConverter(row.schema());
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    return getPartitionPath(rowConverter.convertToAvro(row));
  }

  @Override
  public UTF8String getPartitionPath(InternalRow internalRow, StructType schema) {
    tryInitRowConverter(schema);
    // NOTE: This implementation has considerable computational overhead and has to be overridden
    //       to provide for optimal performance on Spark. This implementation provided exclusively
    //       for compatibility reasons.
    GenericRecord avroRecord = rowConverter.convertToAvro(internalRow);
    return UTF8String.fromString(getPartitionPath(avroRecord));
  }

  protected void tryInitRowAccessor(StructType schema) {
    if (this.rowAccessor == null) {
      synchronized (this) {
        if (this.rowAccessor == null) {
          this.rowAccessor = new SparkRowAccessor(schema);
        }
      }
    }
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final String combinePartitionPath(Object... partitionPathParts) {
    return getStringPartitionPathFormatter().combine(partitionPathFields, partitionPathParts);
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final UTF8String combinePartitionPathUnsafe(Object... partitionPathParts) {
    return getUTF8StringPartitionPathFormatter().combine(partitionPathFields, partitionPathParts);
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final String combineRecordKey(List<String> fieldNames, List<Object> recordKeyParts) {
    return combineRecordKeyInternal(
        StringPartitionPathFormatter.JavaStringBuilder::new,
        BuiltinKeyGenerator::toString,
        BuiltinKeyGenerator::handleNullRecordKey,
        fieldNames,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final UTF8String combineRecordKeyUnsafe(List<String> fieldNames, List<Object> recordKeyParts) {
    return combineRecordKeyInternal(
        UTF8StringPartitionPathFormatter.UTF8StringBuilder::new,
        BuiltinKeyGenerator::toUTF8String,
        BuiltinKeyGenerator::handleNullRecordKey,
        fieldNames,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final String combineCompositeRecordKey(Object... recordKeyParts) {
    return combineCompositeRecordKeyInternal(
        StringPartitionPathFormatter.JavaStringBuilder::new,
        BuiltinKeyGenerator::toString,
        BuiltinKeyGenerator::handleNullOrEmptyCompositeKeyPart,
        BuiltinKeyGenerator::isNullOrEmptyCompositeKeyPart,
        recordKeyParts
    );
  }

  /**
   * NOTE: This method has to stay final (so that it's easier for JIT compiler to apply certain
   *       optimizations, like inlining)
   */
  protected final UTF8String combineCompositeRecordKeyUnsafe(Object... recordKeyParts) {
    return combineCompositeRecordKeyInternal(
        UTF8StringPartitionPathFormatter.UTF8StringBuilder::new,
        BuiltinKeyGenerator::toUTF8String,
        BuiltinKeyGenerator::handleNullOrEmptyCompositeKeyPartUTF8,
        BuiltinKeyGenerator::isNullOrEmptyCompositeKeyPartUTF8,
        recordKeyParts
    );
  }

  private <S> S combineRecordKeyInternal(
      Supplier<PartitionPathFormatterBase.StringBuilder<S>> builderFactory,
      Function<Object, S> converter,
      Function<S, S> emptyKeyPartHandler,
      List<String> fieldNames,
      List<Object> recordKeyParts
  ) {
    if (recordKeyParts.size() == 1) {
      return emptyKeyPartHandler.apply(converter.apply(recordKeyParts.get(0)));
    }

    PartitionPathFormatterBase.StringBuilder<S> sb = builderFactory.get();
    for (int i = 0; i < recordKeyParts.size(); ++i) {
      sb.appendJava(fieldNames.get(i)).appendJava(DEFAULT_COLUMN_VALUE_SEPARATOR);
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      sb.append(emptyKeyPartHandler.apply(converter.apply(recordKeyParts.get(i))));

      if (i < recordKeyParts.size() - 1) {
        sb.appendJava(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    return sb.build();
  }

  private <S> S combineCompositeRecordKeyInternal(
      Supplier<PartitionPathFormatterBase.StringBuilder<S>> builderFactory,
      Function<Object, S> converter,
      Function<S, S> emptyKeyPartHandler,
      Predicate<S> isNullOrEmptyKeyPartPredicate,
      Object... recordKeyParts
  ) {
    boolean hasNonNullNonEmptyPart = false;

    PartitionPathFormatterBase.StringBuilder<S> sb = builderFactory.get();
    for (int i = 0; i < recordKeyParts.length; ++i) {
      // NOTE: If record-key part has already been a string [[toString]] will be a no-op
      S convertedKeyPart = emptyKeyPartHandler.apply(converter.apply(recordKeyParts[i]));

      if (recordKeyParts.length > 1) {
        sb.appendJava(recordKeyFields.get(i));
        sb.appendJava(DEFAULT_COLUMN_VALUE_SEPARATOR);
      }
      sb.append(convertedKeyPart);
      // This check is to validate that overall composite-key has at least one non-null, non-empty
      // segment
      hasNonNullNonEmptyPart |= !isNullOrEmptyKeyPartPredicate.test(convertedKeyPart);

      if (i < recordKeyParts.length - 1) {
        sb.appendJava(DEFAULT_RECORD_KEY_PARTS_SEPARATOR);
      }
    }

    if (hasNonNullNonEmptyPart) {
      return sb.build();
    } else {
      throw new HoodieKeyException(String.format("All of the values for (%s) were either null or empty", recordKeyFields));
    }
  }

  private void tryInitRowConverter(StructType structType) {
    if (rowConverter == null) {
      synchronized (this) {
        if (rowConverter == null) {
          rowConverter = new SparkRowConverter(structType);
        }
      }
    }
  }

  private StringPartitionPathFormatter getStringPartitionPathFormatter() {
    if (stringPartitionPathFormatter == null) {
      synchronized (this) {
        if (stringPartitionPathFormatter == null) {
          this.stringPartitionPathFormatter = new StringPartitionPathFormatter(
              StringPartitionPathFormatter.JavaStringBuilder::new, hiveStylePartitioning, encodePartitionPath);
        }
      }
    }

    return stringPartitionPathFormatter;
  }

  private UTF8StringPartitionPathFormatter getUTF8StringPartitionPathFormatter() {
    if (utf8StringPartitionPathFormatter == null) {
      synchronized (this) {
        if (utf8StringPartitionPathFormatter == null) {
          this.utf8StringPartitionPathFormatter = new UTF8StringPartitionPathFormatter(
              UTF8StringPartitionPathFormatter.UTF8StringBuilder::new, hiveStylePartitioning, encodePartitionPath);
        }
      }
    }

    return utf8StringPartitionPathFormatter;
  }

  protected static String requireNonNullNonEmptyKey(String key) {
    if (key != null && key.length() > 0) {
      return key;
    } else {
      throw new HoodieKeyException("Record key has to be non-empty string!");
    }
  }

  protected static UTF8String requireNonNullNonEmptyKey(UTF8String key) {
    if (key != null && key.numChars() > 0) {
      return key;
    } else {
      throw new HoodieKeyException("Record key has to be non-empty string!");
    }
  }

  protected static <S> S handleNullRecordKey(S s) {
    if (s == null || s.toString().isEmpty()) {
      throw new HoodieKeyException("Record key has to be non-null!");
    }

    return s;
  }

  static UTF8String toUTF8String(Object o) {
    if (o == null) {
      return null;
    } else if (o instanceof UTF8String) {
      return (UTF8String) o;
    } else {
      // NOTE: If object is a [[String]], [[toString]] would be a no-op
      return UTF8String.fromString(o.toString());
    }
  }

  private static String toString(Object o) {
    return o == null ? null : o.toString();
  }

  private static String handleNullOrEmptyCompositeKeyPart(Object keyPart) {
    if (keyPart == null) {
      return NULL_RECORDKEY_PLACEHOLDER;
    } else {
      // NOTE: [[toString]] is a no-op if key-part was already a [[String]]
      String keyPartStr = keyPart.toString();
      return !keyPartStr.isEmpty() ? keyPartStr : EMPTY_RECORDKEY_PLACEHOLDER;
    }
  }

  private static UTF8String handleNullOrEmptyCompositeKeyPartUTF8(UTF8String keyPart) {
    if (keyPart == null) {
      return NULL_RECORD_KEY_PLACEHOLDER_UTF8;
    } else if (keyPart.numChars() == 0) {
      return EMPTY_RECORD_KEY_PLACEHOLDER_UTF8;
    }

    return keyPart;
  }

  @SuppressWarnings("StringEquality")
  private static boolean isNullOrEmptyCompositeKeyPart(String keyPart) {
    // NOTE: Converted key-part is compared against null/empty stub using ref-equality
    //       for performance reasons (it relies on the fact that we're using internalized
    //       constants)
    return keyPart == NULL_RECORDKEY_PLACEHOLDER || keyPart == EMPTY_RECORDKEY_PLACEHOLDER;
  }

  private static boolean isNullOrEmptyCompositeKeyPartUTF8(UTF8String keyPart) {
    // NOTE: Converted key-part is compared against null/empty stub using ref-equality
    //       for performance reasons (it relies on the fact that we're using internalized
    //       constants)
    return keyPart == NULL_RECORD_KEY_PLACEHOLDER_UTF8 || keyPart == EMPTY_RECORD_KEY_PLACEHOLDER_UTF8;
  }

  /**
   * Converts provided (raw) value extracted from the {@link InternalRow} object into a deserialized,
   * JVM native format (for ex, converting {@code Long} into {@link Instant},
   * {@code Integer} to {@link LocalDate}, etc)
   *
   * This method allows to avoid costly full-row deserialization sequence. Note, that this method
   * should be maintained in sync w/
   *
   * <ol>
   *   <li>{@code RowEncoder#deserializerFor}, as well as</li>
   *   <li>{@code HoodieAvroUtils#convertValueForAvroLogicalTypes}</li>
   * </ol>
   *
   * @param dataType target data-type of the given value
   * @param value target value to be converted
   */
  private static Object convertToLogicalDataType(DataType dataType, Object value) {
    if (value == null) {
      return null;
    } else if (dataType instanceof TimestampType) {
      // Provided value have to be [[Long]] in this case, representing micros since epoch
      return new Timestamp((Long) value / 1000);
    } else if (dataType instanceof DateType) {
      // Provided value have to be [[Int]] in this case
      return LocalDate.ofEpochDay((Integer) value);
    }

    return value;
  }

  protected static class SparkRowConverter {
    private static final String STRUCT_NAME = "hoodieRowTopLevelField";
    private static final String NAMESPACE = "hoodieRow";

    private final Function1<Row, GenericRecord> avroConverter;
    private final SparkRowSerDe rowSerDe;

    SparkRowConverter(StructType schema) {
      this.rowSerDe = HoodieSparkUtils.getCatalystRowSerDe(schema);
      this.avroConverter = AvroConversionUtils.createConverterToAvro(schema, STRUCT_NAME, NAMESPACE);
    }

    GenericRecord convertToAvro(Row row) {
      return avroConverter.apply(row);
    }

    GenericRecord convertToAvro(InternalRow row) {
      return avroConverter.apply(rowSerDe.deserializeRow(row));
    }
  }

  protected class SparkRowAccessor {
    private final HoodieUnsafeRowUtils.NestedFieldPath[] recordKeyFieldsPaths;
    private final HoodieUnsafeRowUtils.NestedFieldPath[] partitionPathFieldsPaths;

    SparkRowAccessor(StructType schema) {
      this.recordKeyFieldsPaths = resolveNestedFieldPaths(getRecordKeyFieldNames(), schema, false);
      // Sometimes, we need to extract the recordKey from the partition-dropped data
      // To be consistent with avro key generator
      // ParquetBootstrapMetadataHandler
      this.partitionPathFieldsPaths = resolveNestedFieldPaths(getPartitionPathFields(), schema, true);
    }

    public Object[] getRecordKeyParts(Row row) {
      return getNestedFieldValues(row, recordKeyFieldsPaths);
    }

    public Object[] getRecordPartitionPathValues(Row row) {
      if (partitionPathFieldsPaths == null) {
        throw new HoodieException("Failed to resolve nested partition field");
      }
      return getNestedFieldValues(row, partitionPathFieldsPaths);
    }

    public Object[] getRecordKeyParts(InternalRow row) {
      return getNestedFieldValues(row, recordKeyFieldsPaths);
    }

    public Object[] getRecordPartitionPathValues(InternalRow row) {
      if (partitionPathFieldsPaths == null) {
        throw new HoodieException("Failed to resolve nested partition field");
      }
      return getNestedFieldValues(row, partitionPathFieldsPaths);
    }

    private Object[] getNestedFieldValues(Row row, HoodieUnsafeRowUtils.NestedFieldPath[] nestedFieldsPaths) {
      Object[] nestedFieldValues = new Object[nestedFieldsPaths.length];
      for (int i = 0; i < nestedFieldsPaths.length; ++i) {
        nestedFieldValues[i] = HoodieUnsafeRowUtils$.MODULE$.getNestedRowValue(row, nestedFieldsPaths[i]);
      }
      return nestedFieldValues;
    }

    private Object[] getNestedFieldValues(InternalRow row, HoodieUnsafeRowUtils.NestedFieldPath[] nestedFieldsPaths) {
      Object[] nestedFieldValues = new Object[nestedFieldsPaths.length];
      for (int i = 0; i < nestedFieldsPaths.length; ++i) {
        Object rawValue = HoodieUnsafeRowUtils$.MODULE$.getNestedInternalRowValue(row, nestedFieldsPaths[i]);
        DataType dataType = tail(nestedFieldsPaths[i].parts())._2.dataType();

        nestedFieldValues[i] = convertToLogicalDataType(dataType, rawValue);
      }

      return nestedFieldValues;
    }

    private HoodieUnsafeRowUtils.NestedFieldPath[] resolveNestedFieldPaths(List<String> fieldPaths, StructType schema, boolean returnNull) {
      try {
        return fieldPaths.stream()
            .map(fieldPath -> HoodieUnsafeRowUtils$.MODULE$.composeNestedFieldPath(schema, fieldPath).get())
            .toArray(HoodieUnsafeRowUtils.NestedFieldPath[]::new);
      } catch (Exception e) {
        if (returnNull) {
          return null;
        }
        LOG.error(String.format("Failed to resolve nested field-paths (%s) in schema (%s)", fieldPaths, schema), e);
        throw new HoodieException("Failed to resolve nested field-paths", e);
      }
    }
  }
}

