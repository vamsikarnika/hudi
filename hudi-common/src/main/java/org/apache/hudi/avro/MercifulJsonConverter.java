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

package org.apache.hudi.avro;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.collection.JavaConverters;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * Converts Json record to Avro Generic Record.
 */
public class MercifulJsonConverter {

  // For each schema (keyed by full name), stores a mapping of schema field name to json field name to account for sanitization of fields
  private static final Map<String, Map<String, String>> SANITIZED_FIELD_MAPPINGS = new ConcurrentHashMap<>();

  private final ObjectMapper mapper;

  private final String invalidCharMask;
  private final boolean shouldSanitize;

  /**
   * Uses a default objectMapper to deserialize a json string.
   */
  public MercifulJsonConverter() {
    this(false, "__");
  }

  /**
   * Allows enabling sanitization and allows choice of invalidCharMask for sanitization
   */
  public MercifulJsonConverter(boolean shouldSanitize, String invalidCharMask) {
    this(new ObjectMapper(), shouldSanitize, invalidCharMask);
  }

  /**
   * Allows a configured ObjectMapper to be passed for converting json records to avro record.
   */
  public MercifulJsonConverter(ObjectMapper mapper, boolean shouldSanitize, String invalidCharMask) {
    this.mapper = mapper;
    this.shouldSanitize = shouldSanitize;
    this.invalidCharMask = invalidCharMask;
  }

  /**
   * Converts json to Avro generic record.
   * NOTE: if sanitization is needed for avro conversion, the schema input to this method is already sanitized.
   *       During the conversion here, we sanitize the fields in the data
   *
   * @param json Json record
   * @param schema Schema
   */
  public GenericRecord convert(String json, Schema schema) {
    try {
      Map<String, Object> jsonObjectMap = mapper.readValue(json, Map.class);
      return convertJsonToAvro(jsonObjectMap, schema, shouldSanitize, invalidCharMask);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public Row convertToRow(String json, Schema schema) {
    try {
      Map<String, Object> jsonObjectMap = mapper.readValue(json, Map.class);
      return convertJsonToRow(jsonObjectMap, schema, shouldSanitize, invalidCharMask);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  /**
   * Clear between fetches. If the schema changes or if two tables have the same schemaFullName then
   * can be issues
   */
  public static void clearCache(String schemaFullName) {
    SANITIZED_FIELD_MAPPINGS.remove(schemaFullName);
  }

  private static GenericRecord convertJsonToAvro(Map<String, Object> inputJson, Schema schema, boolean shouldSanitize, String invalidCharMask) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      Object val = shouldSanitize ? getFieldFromJson(f, inputJson, schema.getFullName(), invalidCharMask) : inputJson.get(f.name());
      if (val != null) {
        avroRecord.put(f.pos(), convertJsonToAvroField(val, f.name(), f.schema(), shouldSanitize, invalidCharMask));
      }
    }
    return avroRecord;
  }

  protected static Row convertJsonToRow(Map<String, Object> inputJson, Schema schema, boolean shouldSanitize, String invalidCharMask) {
    List<Schema.Field> fields = schema.getFields();
    List<Object> values = new ArrayList<>(Collections.nCopies(fields.size(), null));

    for (Schema.Field f : fields) {
      Object val = shouldSanitize ? getFieldFromJson(f, inputJson, schema.getFullName(), invalidCharMask) : inputJson.get(f.name());
      if (val != null) {
        values.set(f.pos(), convertJsonToRowField(val, f.name(), f.schema(), shouldSanitize, invalidCharMask));
      } else {
        Object defaultVal = f.defaultVal();
        if (defaultVal.equals(JsonProperties.NULL_VALUE)) {
          defaultVal = null;
        }
        values.set(f.pos(), defaultVal);
      }
    }
    return RowFactory.create(values.toArray());
  }

  private static Object getFieldFromJson(final Schema.Field fieldSchema, final Map<String, Object> inputJson, final String schemaFullName, final String invalidCharMask) {
    Map<String, String> schemaToJsonFieldNames = SANITIZED_FIELD_MAPPINGS.computeIfAbsent(schemaFullName, unused -> new ConcurrentHashMap<>());
    if (!schemaToJsonFieldNames.containsKey(fieldSchema.name())) {
      // if we don't have field mapping, proactively populate as many as possible based on input json
      for (String inputFieldName : inputJson.keySet()) {
        // we expect many fields won't need sanitization so check if un-sanitized field name is already present
        if (!schemaToJsonFieldNames.containsKey(inputFieldName)) {
          String sanitizedJsonFieldName = HoodieAvroUtils.sanitizeName(inputFieldName, invalidCharMask);
          schemaToJsonFieldNames.putIfAbsent(sanitizedJsonFieldName, inputFieldName);
        }
      }
    }
    Object match = inputJson.get(schemaToJsonFieldNames.getOrDefault(fieldSchema.name(), fieldSchema.name()));
    if (match != null) {
      return match;
    }
    // Check if there is an alias match
    for (String alias : fieldSchema.aliases()) {
      if (inputJson.containsKey(alias)) {
        return inputJson.get(alias);
      }
    }
    return null;
  }

  private static Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    Schema.Type firstType = types.get(0).getType();
    return firstType.equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  private static boolean isOptional(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION) && schema.getTypes().size() == 2
        && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
        || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  @FunctionalInterface
  public interface ConvertInternalApi {
    Object apply(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask);
  }

  private static Object convertJsonToFieldInternal(
      ConvertInternalApi api, Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {

    if (isOptional(schema)) {
      if (value == null) {
        return null;
      } else {
        schema = getNonNull(schema);
      }
    } else if (value == null) {
      // Always fail on null for non-nullable schemas
      throw new HoodieJsonToAvroConversionException(null, name, schema, shouldSanitize, invalidCharMask);
    }

    return api.apply(value, name, schema, shouldSanitize, invalidCharMask);
  }

  public static Object convertJsonToAvroField(
      Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
    return convertJsonToFieldInternal(JsonToAvroFieldProcessorUtil::convertToAvro, value, name, schema, shouldSanitize, invalidCharMask);
  }

  protected static Object convertJsonToRowField(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
    return convertJsonToFieldInternal(JsonToAvroFieldProcessorUtil::convertToRow, value, name, schema, shouldSanitize, invalidCharMask);
  }

  private static class JsonToAvroFieldProcessorUtil {
    @FunctionalInterface
    public interface UtilConvertInternalApi {
      Pair<Boolean, Object> apply(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask);
    }

    /**
     * Base Class for converting json to avro fields.
     */
    private abstract static class JsonToAvroFieldProcessor implements Serializable {
      private Object convertInternal(UtilConvertInternalApi api, Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        Pair<Boolean, Object> res = api.apply(value, name, schema, shouldSanitize, invalidCharMask);
        if (!res.getLeft()) {
          throw new HoodieJsonToAvroConversionException(value, name, schema, shouldSanitize, invalidCharMask);
        }
        return res.getRight();
      }

      public Object convertToAvro(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertInternal(this::convert, value, name, schema, shouldSanitize, invalidCharMask);
      }

      public Object convertToRow(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertInternal(this::convertToRowInternal, value, name, schema, shouldSanitize, invalidCharMask);
      }

      protected abstract Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask);

      protected Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convert(value, name, schema, shouldSanitize, invalidCharMask);
      }
    }

    public static Object convertToAvro(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
      JsonToAvroFieldProcessor processor = getProcessorForSchema(schema);
      return processor.convertToAvro(value, name, schema, shouldSanitize, invalidCharMask);
    }

    public static Object convertToRow(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
      JsonToAvroFieldProcessor processor = getProcessorForSchema(schema);
      return processor.convertToRow(value, name, schema, shouldSanitize, invalidCharMask);
    }

    private static JsonToAvroFieldProcessor getProcessorForSchema(Schema schema) {
      JsonToAvroFieldProcessor processor = null;

      // 3 cases to consider: customized logicalType, logicalType, and type.
      String customizedLogicalType = schema.getProp("logicalType");
      LogicalType logicalType = schema.getLogicalType();
      Type type = schema.getType();
      if (customizedLogicalType != null && !customizedLogicalType.isEmpty()) {
        processor = AVRO_LOGICAL_TYPE_FIELD_PROCESSORS.get(customizedLogicalType);
      } else if (logicalType != null) {
        processor = AVRO_LOGICAL_TYPE_FIELD_PROCESSORS.get(logicalType.getName());
      } else {
        processor = AVRO_TYPE_FIELD_TYPE_PROCESSORS.get(type);
      }

      if (processor == null) {
        throw new IllegalArgumentException(String.format("JsonConverter cannot handle type: %s", type));
      }
      return processor;
    }

    // Avro primitive and complex type processors.
    private static final Map<Schema.Type, JsonToAvroFieldProcessor> AVRO_TYPE_FIELD_TYPE_PROCESSORS = getFieldTypeProcessors();
    // Avro logical type processors.
    private static final Map<String, JsonToAvroFieldProcessor> AVRO_LOGICAL_TYPE_FIELD_PROCESSORS = getLogicalFieldTypeProcessors();

    /**
     * Build type processor map for each avro type.
     */
    private static Map<Schema.Type, JsonToAvroFieldProcessor> getFieldTypeProcessors() {
      Map<Schema.Type, JsonToAvroFieldProcessor> fieldTypeProcessors = new EnumMap<>(Schema.Type.class);
      fieldTypeProcessors.put(Type.STRING, generateStringTypeHandler());
      fieldTypeProcessors.put(Type.BOOLEAN, generateBooleanTypeHandler());
      fieldTypeProcessors.put(Type.DOUBLE, generateDoubleTypeHandler());
      fieldTypeProcessors.put(Type.FLOAT, generateFloatTypeHandler());
      fieldTypeProcessors.put(Type.INT, generateIntTypeHandler());
      fieldTypeProcessors.put(Type.LONG, generateLongTypeHandler());
      fieldTypeProcessors.put(Type.ARRAY, generateArrayTypeHandler());
      fieldTypeProcessors.put(Type.RECORD, generateRecordTypeHandler());
      fieldTypeProcessors.put(Type.ENUM, generateEnumTypeHandler());
      fieldTypeProcessors.put(Type.MAP, generateMapTypeHandler());
      fieldTypeProcessors.put(Type.BYTES, generateBytesTypeHandler());
      fieldTypeProcessors.put(Type.FIXED, generateFixedTypeHandler());
      return Collections.unmodifiableMap(fieldTypeProcessors);
    }

    private static Map<String, JsonToAvroFieldProcessor> getLogicalFieldTypeProcessors() {
      Map<String, JsonToAvroFieldProcessor> logicalFieldTypeProcessors = new HashMap<>();
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.DECIMAL.getValue(), new DecimalLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIME_MICROS.getValue(), new TimeMicroLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIME_MILLIS.getValue(), new TimeMilliLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.DATE.getValue(), new DateLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS.getValue(), new LocalTimestampMicroLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS.getValue(), new LocalTimestampMilliLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIMESTAMP_MICROS.getValue(), new TimestampMicroLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.TIMESTAMP_MILLIS.getValue(), new TimestampMilliLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.DURATION.getValue(), new DurationLogicalTypeProcessor());
      logicalFieldTypeProcessors.put(AvroLogicalTypeEnum.UUID.getValue(), generateStringTypeHandler());
      return Collections.unmodifiableMap(logicalFieldTypeProcessors);
    }

    private static class DecimalLogicalTypeProcessor extends JsonToAvroFieldProcessor {
      @Override
      public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        if (!isValidDecimalTypeConfig(schema)) {
          return Pair.of(false, null);
        }
        Pair<Boolean, BigDecimal> parseResult = parseObjectToBigDecimal(value, schema);
        return Pair.of(parseResult.getLeft(), parseResult.getRight());
      }

      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {

        if (!isValidDecimalTypeConfig(schema)) {
          return Pair.of(false, null);
        }

        if (schema.getType() == Type.FIXED) {
          if (value instanceof List<?>) {
            // Case 1: Input is a list. It is expected to be raw Fixed byte array input, and we only support
            // parsing it to Fixed avro type.
            JsonToAvroFieldProcessor processor = generateFixedTypeHandler();
            return processor.convert(value, name, schema, shouldSanitize, invalidCharMask);
          } else if (value instanceof String) {
            try {
              // It is a kafka encoded string that is here because of the spark avro post processor
              Object fixed = HoodieAvroUtils.convertBytesToFixed(decodeStringToBigDecimalBytes(value), schema);
              return Pair.of(true, fixed);
            } catch (IllegalArgumentException e) {
              // no-op
            }
          }
        }

        // Case 2: Input is a number or String number.
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        Pair<Boolean, BigDecimal> parseResult = parseObjectToBigDecimal(value, schema);
        if (Boolean.FALSE.equals(parseResult.getLeft())) {
          return Pair.of(false, null);
        }
        BigDecimal bigDecimal = parseResult.getRight();

        switch (schema.getType()) {
          case BYTES:
            // Convert to primitive Arvo type that logical type Decimal uses.
            ByteBuffer byteBuffer = new Conversions.DecimalConversion().toBytes(bigDecimal, schema, decimalType);
            return Pair.of(true, byteBuffer);
          case FIXED:
            GenericFixed fixedValue = new Conversions.DecimalConversion().toFixed(bigDecimal, schema, decimalType);
            return Pair.of(true, fixedValue);
          default: {
            return Pair.of(false, null);
          }
        }
      }

      /**
       * Check if the given schema is a valid decimal type configuration.
       */
      private static boolean isValidDecimalTypeConfig(Schema schema) {
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        // At the time when the schema is found not valid when it is parsed, the Avro Schema.parse will just silently
        // set the schema to be null instead of throwing exceptions. Correspondingly, we just check if it is null here.
        if (decimalType == null) {
          return false;
        }
        // Even though schema is validated at schema parsing phase, still validate here to be defensive.
        decimalType.validate(schema);
        return true;
      }

      /**
       * Parse the object to BigDecimal.
       *
       * @param obj Object to be parsed
       * @return Pair object, with left as boolean indicating if the parsing was successful and right as the
       * BigDecimal value.
       */
      private static Pair<Boolean, BigDecimal> parseObjectToBigDecimal(Object obj, Schema schema) {
        BigDecimal bigDecimal = null;
        if (obj instanceof Number) {
          Number number = (Number) obj;
          // Special case integers and 0.0 to avoid conversion errors related to decimals with a scale of 0
          if (obj instanceof Integer || obj instanceof Long || obj instanceof Short || obj instanceof Byte || number.doubleValue() == 0.0) {
            bigDecimal = BigDecimal.valueOf(number.longValue());
          } else {
            bigDecimal = BigDecimal.valueOf(number.doubleValue());
          }
        }

        // Case 2: Object is a number in String format.
        if (obj instanceof String) {
          if (schema.getType() == Type.BYTES) {
            try {
              //encoded big decimal
              bigDecimal = HoodieAvroUtils.convertBytesToBigDecimal(decodeStringToBigDecimalBytes(obj),
                  (LogicalTypes.Decimal) schema.getLogicalType());
            } catch (IllegalArgumentException e) {
              //no-op
            }
          }
          // None fixed byte or fixed byte conversion failure would end up here.
          if (bigDecimal == null) {
            try {
              bigDecimal = new BigDecimal(((String) obj));
            } catch (java.lang.NumberFormatException ignored) {
              /* ignore */
            }
          }
        } else if (schema.getType() == Type.FIXED) {
          List<?> list = (List<?>) obj;
          List<Integer> converval = list.stream()
              .filter(Integer.class::isInstance)
              .map(Integer.class::cast)
              .collect(Collectors.toList());

          byte[] byteArray = new byte[converval.size()];
          for (int i = 0; i < converval.size(); i++) {
            byteArray[i] = (byte) converval.get(i).intValue();
          }
          GenericFixed fixedValue = new GenericData.Fixed(schema, byteArray);
          // Convert the GenericFixed to BigDecimal
          bigDecimal = new Conversions.DecimalConversion().fromFixed(
              fixedValue, schema, schema.getLogicalType());
        }

        if (bigDecimal == null) {
          return Pair.of(false, null);
        }
        // As we don't do rounding, the validation will enforce the scale part and the integer part are all within the
        // limit. As a result, if scale is 2 precision is 5, we only allow 3 digits for the integer.
        // Allowed: 123.45, 123, 0.12
        // Disallowed: 1234 (4 digit integer while the scale has already reserved 2 digit out of the 5 digit precision)
        //             123456, 0.12345
        LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
        if (bigDecimal.scale() > decimalType.getScale()
            || (bigDecimal.precision() - bigDecimal.scale()) > (decimalType.getPrecision() - decimalType.getScale())) {
          // Correspond to case
          // org.apache.avro.AvroTypeException: Cannot encode decimal with scale 5 as scale 2 without rounding.
          // org.apache.avro.AvroTypeException: Cannot encode decimal with scale 3 as scale 2 without rounding
          return Pair.of(false, null);
        }
        return Pair.of(bigDecimal != null, bigDecimal);
      }
    }

    private static byte[] decodeStringToBigDecimalBytes(Object value) {
      return Base64.getDecoder().decode(((String) value).getBytes());
    }

    private static class DurationLogicalTypeProcessor extends JsonToAvroFieldProcessor {
      private static final int NUM_ELEMENTS_FOR_DURATION_TYPE = 3;

      /**
       * We expect the input to be a list of 3 integers representing months, days and milliseconds.
       */
      private boolean isValidDurationInput(Object value) {
        if (!(value instanceof List<?>)) {
          return false;
        }
        List<?> list = (List<?>) value;
        if (list.size() != NUM_ELEMENTS_FOR_DURATION_TYPE) {
          return false;
        }
        for (Object element : list) {
          if (!(element instanceof Integer)) {
            return false;
          }
        }
        return true;
      }

      /**
       * Convert the given object to Avro object with schema whose logical type is duration.
       */
      @Override
      public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {

        if (!isValidDurationTypeConfig(schema)) {
          return Pair.of(false, null);
        }
        if (!isValidDurationInput(value)) {
          return Pair.of(false, null);
        }
        // After the validation the input can be safely cast to List<Integer> with 3 elements.
        List<?> list = (List<?>) value;
        List<Integer> converval = list.stream()
            .filter(Integer.class::isInstance)
            .map(Integer.class::cast)
            .collect(Collectors.toList());

        ByteBuffer buffer = ByteBuffer.allocate(schema.getFixedSize()).order(ByteOrder.LITTLE_ENDIAN);
        for (Integer element : converval) {
          buffer.putInt(element);  // months
        }
        return Pair.of(true, new GenericData.Fixed(schema, buffer.array()));
      }

      @Override
      public Pair<Boolean, Object> convertToRowInternal(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        throw new HoodieException("Duration type is not supported in Row object");
      }

      /**
       * Check if the given schema is a valid decimal type configuration.
       */
      private static boolean isValidDurationTypeConfig(Schema schema) {
        String durationTypeName = AvroLogicalTypeEnum.DURATION.getValue();
        LogicalType durationType = schema.getLogicalType();
        String durationTypeProp = schema.getProp("logicalType");
        // 1. The Avro type should be "Fixed".
        // 2. Fixed size must be of 12 bytes as it hold 3 integers.
        // 3. Logical type name should be "duration". The name might be stored in different places based on Avro version
        //    being used here.
        return schema.getType().equals(Type.FIXED)
            && schema.getFixedSize() == Integer.BYTES * NUM_ELEMENTS_FOR_DURATION_TYPE
            && (durationType != null && durationType.getName().equals(durationTypeName)
            || durationTypeProp != null && durationTypeProp.equals(durationTypeName));
      }
    }

    /**
     * Processor utility handling Number inputs.
     */
    abstract static class Parser {
      abstract Pair<Boolean, Object> handleNumberValue(Number value);

      abstract Pair<Boolean, Object> handleStringNumber(String value);

      abstract Pair<Boolean, Object> handleStringValue(String value);

      static class IntParser extends Parser {
        @Override
        public Pair<Boolean, Object> handleNumberValue(Number value) {
          return Pair.of(true, value.intValue());
        }

        @Override
        public Pair<Boolean, Object> handleStringNumber(String value) {
          return Pair.of(true, Integer.parseInt(value));
        }

        @Override
        public Pair<Boolean, Object> handleStringValue(String value) {
          return Pair.of(true, Integer.valueOf(value));
        }
      }

      static class DateParser extends Parser {

        private static long MILLI_SECONDS_PER_DAY = 86400000;
        @Override
        public Pair<Boolean, Object> handleNumberValue(Number value) {
          return Pair.of(true, new java.sql.Date(value.intValue() * MILLI_SECONDS_PER_DAY));
        }

        @Override
        public Pair<Boolean, Object> handleStringNumber(String value) {
          return Pair.of(true, new java.sql.Date(Integer.parseInt(value) * MILLI_SECONDS_PER_DAY));
        }

        @Override
        public Pair<Boolean, Object> handleStringValue(String value) {
          return Pair.of(true, java.sql.Date.valueOf(value));
        }
      }

      static class LongParser extends Parser {
        @Override
        public Pair<Boolean, Object> handleNumberValue(Number value) {
          return Pair.of(true, value.longValue());
        }

        @Override
        public Pair<Boolean, Object> handleStringNumber(String value) {
          return Pair.of(true, Long.parseLong(value));
        }

        @Override
        public Pair<Boolean, Object> handleStringValue(String value) {
          return Pair.of(true, Long.valueOf(value));
        }
      }
    }

    /**
     * Base Class for converting object to avro logical type TimeMilli/TimeMicro.
     */
    private abstract static class TimeLogicalTypeProcessor extends JsonToAvroFieldProcessor {

      protected static final LocalDateTime LOCAL_UNIX_EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0);

      // Logical type the processor is handling.
      private final AvroLogicalTypeEnum logicalTypeEnum;

      public TimeLogicalTypeProcessor(AvroLogicalTypeEnum logicalTypeEnum) {
        this.logicalTypeEnum = logicalTypeEnum;
      }

      /**
       * Main function that convert input to Object with java data type specified by schema
       */
      public Pair<Boolean, Object> convertCommon(Parser parser, Object value, Schema schema) {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType == null) {
          return Pair.of(false, null);
        }
        logicalType.validate(schema);
        if (value instanceof Number) {
          return parser.handleNumberValue((Number) value);
        }
        if (value instanceof String) {
          String valStr = (String) value;
          if (ALL_DIGITS_WITH_OPTIONAL_SIGN.matcher(valStr).matches()) {
            return parser.handleStringNumber(valStr);
          } else {
            return parser.handleStringValue(valStr);
          }
        }
        return Pair.of(false, null);
      }

      protected DateTimeFormatter getDateTimeFormatter() {
        DateTimeParseContext ctx = DATE_TIME_PARSE_CONTEXT_MAP.get(logicalTypeEnum);
        return ctx == null ? null : ctx.dateTimeFormatter;
      }

      protected Pattern getDateTimePattern() {
        DateTimeParseContext ctx = DATE_TIME_PARSE_CONTEXT_MAP.get(logicalTypeEnum);
        return ctx == null ? null : ctx.dateTimePattern;
      }

      // Depending on the logical type the processor handles, they use different parsing context
      // when they need to parse a timestamp string in handleStringValue.
      private static class DateTimeParseContext {
        public DateTimeParseContext(DateTimeFormatter dateTimeFormatter, Pattern dateTimePattern) {
          this.dateTimeFormatter = dateTimeFormatter;
          this.dateTimePattern = dateTimePattern;
        }

        public final Pattern dateTimePattern;

        public final DateTimeFormatter dateTimeFormatter;
      }

      private static final Map<AvroLogicalTypeEnum, DateTimeParseContext> DATE_TIME_PARSE_CONTEXT_MAP = getParseContext();

      private static Map<AvroLogicalTypeEnum, DateTimeParseContext> getParseContext() {
        // The pattern is derived from ISO_LOCAL_DATE_TIME definition with the relaxation on the separator.
        DateTimeFormatter localDateTimeFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .optionalEnd()
            .optionalStart()
            .appendLiteral(' ')
            .optionalEnd()
            .append(ISO_LOCAL_TIME)
            .toFormatter()
            .withResolverStyle(ResolverStyle.STRICT)
            .withChronology(IsoChronology.INSTANCE);
        // Formatter for parsing timestamp.
        // The pattern is derived from ISO_OFFSET_DATE_TIME definition with the relaxation on the separator.
        // Pattern asserts the string is
        // <optional sign><Year>-<Month>-<Day><separator><Hour>:<Minute> + optional <second> + optional <fractional second> + optional <zone offset>
        // <separator> is 'T' or ' '
        // For example, "2024-07-13T11:36:01.951Z", "2024-07-13T11:36:01.951+01:00",
        // "2024-07-13T11:36:01Z", "2024-07-13T11:36:01+01:00",
        // "2024-07-13 11:36:01.951Z", "2024-07-13 11:36:01.951+01:00".
        // See TestMercifulJsonConverter#timestampLogicalTypeGoodCaseTest
        // and #timestampLogicalTypeBadTest for supported and unsupported cases.
        DateTimeParseContext dateTimestampParseContext = new DateTimeParseContext(
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(localDateTimeFormatter)
                .optionalStart()
                .appendOffsetId()
                .optionalEnd()
                .parseDefaulting(ChronoField.OFFSET_SECONDS, 0L)
                .toFormatter()
                .withResolverStyle(ResolverStyle.STRICT)
                .withChronology(IsoChronology.INSTANCE),
            null /* match everything*/);
        // Formatter for parsing time of day. The pattern is derived from ISO_LOCAL_TIME definition.
        // Pattern asserts the string is
        // <optional sign><Hour>:<Minute> + optional <second> + optional <fractional second>
        // For example, "11:36:01.951".
        // See TestMercifulJsonConverter#timeLogicalTypeTest
        // and #timeLogicalTypeBadCaseTest for supported and unsupported cases.
        DateTimeParseContext dateTimeParseContext = new DateTimeParseContext(
            ISO_LOCAL_TIME,
            Pattern.compile("^[+-]?\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?"));
        // Formatter for parsing local timestamp.
        // The pattern is derived from ISO_LOCAL_DATE_TIME definition with the relaxation on the separator.
        // Pattern asserts the string is
        // <optional sign><Year>-<Month>-<Day><separator><Hour>:<Minute> + optional <second> + optional <fractional second>
        // <separator> is 'T' or ' '
        // For example, "2024-07-13T11:36:01.951", "2024-07-13 11:36:01.951".
        // See TestMercifulJsonConverter#localTimestampLogicalTypeGoodCaseTest
        // and #localTimestampLogicalTypeBadTest for supported and unsupported cases.
        DateTimeParseContext localTimestampParseContext = new DateTimeParseContext(
            localDateTimeFormatter,
            Pattern.compile("^[+-]?\\d{4,10}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?")
        );
        // Formatter for parsing local date. The pattern is derived from ISO_LOCAL_DATE definition.
        // Pattern asserts the string is
        // <optional sign><Year>-<Month>-<Day>
        // For example, "2024-07-13".
        // See TestMercifulJsonConverter#dateLogicalTypeTest for supported and unsupported cases.
        DateTimeParseContext localDateParseContext = new DateTimeParseContext(
            ISO_LOCAL_DATE,
            Pattern.compile("^[+-]?\\d{4,10}-\\d{2}-\\d{2}?")
        );

        EnumMap<AvroLogicalTypeEnum, DateTimeParseContext> ctx = new EnumMap<>(AvroLogicalTypeEnum.class);
        ctx.put(AvroLogicalTypeEnum.TIME_MICROS, dateTimeParseContext);
        ctx.put(AvroLogicalTypeEnum.TIME_MILLIS, dateTimeParseContext);
        ctx.put(AvroLogicalTypeEnum.DATE, localDateParseContext);
        ctx.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS, localTimestampParseContext);
        ctx.put(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS, localTimestampParseContext);
        ctx.put(AvroLogicalTypeEnum.TIMESTAMP_MICROS, dateTimestampParseContext);
        ctx.put(AvroLogicalTypeEnum.TIMESTAMP_MILLIS, dateTimestampParseContext);
        return Collections.unmodifiableMap(ctx);
      }

      // Pattern validating if it is an number in string form.
      // Only check at most 19 digits as this is the max num of digits for LONG.MAX_VALUE to contain the cost of regex matching.
      protected static final Pattern ALL_DIGITS_WITH_OPTIONAL_SIGN = Pattern.compile("^[-+]?\\d{1,19}$");

      /**
       * Check if the given string is a well-formed date time string.
       * If no pattern is defined, it will always return true.
       */
      protected boolean isWellFormedDateTime(String value) {
        Pattern pattern = getDateTimePattern();
        return pattern == null || pattern.matcher(value).matches();
      }

      protected Pair<Boolean, Instant> convertToInstantTime(String input) {
        // Parse the input timestamp
        // The input string is assumed in the format:
        // <optional sign><Year>-<Month>-<Day><separator><Hour>:<Minute> + optional <second> + optional <fractional second> + optional <zone offset>
        // <separator> is 'T' or ' '
        Instant time = null;
        try {
          ZonedDateTime dateTime = ZonedDateTime.parse(input, getDateTimeFormatter());
          time = dateTime.toInstant();
        } catch (DateTimeParseException ignore) {
          /* ignore */
        }
        return Pair.of(time != null, time);
      }

      protected Pair<Boolean, LocalTime> convertToLocalTime(String input) {
        // Parse the input timestamp, DateTimeFormatter.ISO_LOCAL_TIME is implied here
        LocalTime time = null;
        try {
          // Try parsing as an ISO date
          time = LocalTime.parse(input);
        } catch (DateTimeParseException ignore) {
          /* ignore */
        }
        return Pair.of(time != null, time);
      }

      protected Pair<Boolean, LocalDateTime> convertToLocalDateTime(String input) {
        // Parse the input timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME is implied here
        LocalDateTime time = null;
        try {
          // Try parsing as an ISO date
          time = LocalDateTime.parse(input, getDateTimeFormatter());
        } catch (DateTimeParseException ignore) {
          /* ignore */
        }
        return Pair.of(time != null, time);
      }
    }

    private static class DateLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public DateLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.DATE);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.IntParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, LocalDate> result = convertToLocalDate(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                LocalDate date = result.getRight();
                int daysSinceEpoch = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), date);
                return Pair.of(true, daysSinceEpoch);
              }
            },
            value, schema);
      }

      @Override
      public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(new Parser.DateParser(), value, schema);
      }

      private Pair<Boolean, LocalDate> convertToLocalDate(String input) {
        // Parse the input timestamp, DateTimeFormatter.ISO_LOCAL_TIME is implied here
        LocalDate date = null;
        try {
          // Try parsing as an ISO date
          date = LocalDate.parse(input);
        } catch (DateTimeParseException ignore) {
          /* ignore */
        }
        return Pair.of(date != null, date);
      }
    }

    /**
     * Processor for TimeMilli logical type.
     */
    private static class TimeMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public TimeMilliLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIME_MILLIS);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.IntParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, LocalTime> result = convertToLocalTime(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                LocalTime time = result.getRight();
                Integer millisOfDay = time.toSecondOfDay() * 1000 + time.getNano() / 1000000;
                return Pair.of(true, millisOfDay);
              }
            },
            value, schema);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class TimeMicroLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public TimeMicroLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIME_MICROS);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.LongParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, LocalTime> result = convertToLocalTime(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                LocalTime time = result.getRight();
                Long microsOfDay = (long) time.toSecondOfDay() * 1000000 + time.getNano() / 1000;
                return Pair.of(true, microsOfDay);
              }
            },
            value, schema);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class LocalTimestampMicroLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public LocalTimestampMicroLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MICROS);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.LongParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                LocalDateTime time = result.getRight();

                // Calculate the difference in milliseconds
                long diffInMicros = LOCAL_UNIX_EPOCH.until(time, ChronoField.MICRO_OF_SECOND.getBaseUnit());
                return Pair.of(true, diffInMicros);
              }
            },
            value, schema);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class TimestampMicroLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public TimestampMicroLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIMESTAMP_MICROS);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.LongParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, Instant> result = convertToInstantTime(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                Instant time = result.getRight();

                // Calculate the difference in milliseconds
                long diffInMicro = Instant.EPOCH.until(time, ChronoField.MICRO_OF_SECOND.getBaseUnit());
                return Pair.of(true, diffInMicro);
              }
            },
            value, schema);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class LocalTimestampMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public LocalTimestampMilliLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.LOCAL_TIMESTAMP_MILLIS);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.LongParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, LocalDateTime> result = convertToLocalDateTime(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                LocalDateTime time = result.getRight();

                // Calculate the difference in milliseconds
                long diffInMillis = LOCAL_UNIX_EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit());
                return Pair.of(true, diffInMillis);
              }
            },
            value, schema);
      }
    }

    /**
     * Processor for TimeMicro logical type.
     */
    private static class TimestampMilliLogicalTypeProcessor extends TimeLogicalTypeProcessor {
      public TimestampMilliLogicalTypeProcessor() {
        super(AvroLogicalTypeEnum.TIMESTAMP_MILLIS);
      }

      @Override
      public Pair<Boolean, Object> convert(
          Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
        return convertCommon(
            new Parser.LongParser() {
              @Override
              public Pair<Boolean, Object> handleStringValue(String value) {
                if (!isWellFormedDateTime(value)) {
                  return Pair.of(false, null);
                }
                Pair<Boolean, Instant> result = convertToInstantTime(value);
                if (!result.getLeft()) {
                  return Pair.of(false, null);
                }
                Instant time = result.getRight();

                // Calculate the difference in milliseconds
                long diffInMillis = Instant.EPOCH.until(time, ChronoField.MILLI_OF_SECOND.getBaseUnit());
                return Pair.of(true, diffInMillis);
              }
            },
            value, schema);
      }
    }

    private static JsonToAvroFieldProcessor generateBooleanTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Boolean) {
            return Pair.of(true, value);
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateIntTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).intValue());
          } else if (value instanceof String) {
            return Pair.of(true, Integer.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateDoubleTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).doubleValue());
          } else if (value instanceof String) {
            return Pair.of(true, Double.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateFloatTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).floatValue());
          } else if (value instanceof String) {
            return Pair.of(true, Float.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateLongTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (value instanceof Number) {
            return Pair.of(true, ((Number) value).longValue());
          } else if (value instanceof String) {
            return Pair.of(true, Long.valueOf((String) value));
          }
          return Pair.of(false, null);
        }
      };
    }

    private static JsonToAvroFieldProcessor generateStringTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, value.toString());
        }
      };
    }

    private static JsonToAvroFieldProcessor generateBytesTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          // Should return ByteBuffer (see GenericData.isBytes())
          return Pair.of(true, ByteBuffer.wrap(value.toString().getBytes()));
        }

        @Override
        public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, value.toString().getBytes());
        }
      };
    }

    private static JsonToAvroFieldProcessor generateFixedTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        private byte[] convertToJavaObject(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          // The ObjectMapper use List to represent FixedType
          // eg: "decimal_val": [0, 0, 14, -63, -52] will convert to ArrayList<Integer>
          List<Integer> converval = (List<Integer>) value;
          byte[] src = new byte[converval.size()];
          for (int i = 0; i < converval.size(); i++) {
            src[i] = converval.get(i).byteValue();
          }
          byte[] dst = new byte[schema.getFixedSize()];
          System.arraycopy(src, 0, dst, 0, Math.min(schema.getFixedSize(), src.length));
          return dst;
        }

        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, new GenericData.Fixed(
            schema, convertToJavaObject(value, name, schema, shouldSanitize, invalidCharMask)));
        }

        @Override
        public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, convertToJavaObject(value, name, schema, shouldSanitize, invalidCharMask));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateEnumTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        private Object convertToJavaObject(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          if (schema.getEnumSymbols().contains(value.toString())) {
            return value.toString();
          }
          throw new HoodieJsonToAvroConversionException(String.format("Symbol %s not in enum", value.toString()),
              schema.getFullName(), schema, shouldSanitize, invalidCharMask);
        }

        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, new GenericData.EnumSymbol(schema, convertToJavaObject(value, name, schema, shouldSanitize, invalidCharMask)));
        }

        @Override
        public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, convertToJavaObject(value, name, schema, shouldSanitize, invalidCharMask));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateRecordTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, convertJsonToAvro((Map<String, Object>) value, schema, shouldSanitize, invalidCharMask));
        }

        @Override
        public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, convertJsonToRow((Map<String, Object>) value, schema, shouldSanitize, invalidCharMask));
        }
      };
    }

    private static JsonToAvroFieldProcessor generateArrayTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        private List<Object> convertToJavaObject(ConvertInternalApi convertApi, Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          Schema elementSchema = schema.getElementType();
          List<Object> listRes = new ArrayList<>();
          for (Object v : (List) value) {
            listRes.add(convertApi.apply(v, name, elementSchema, shouldSanitize, invalidCharMask));
          }
          return listRes;
        }

        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, new GenericData.Array<>(
              schema,
              convertToJavaObject(
                  MercifulJsonConverter::convertJsonToAvroField,
                  value,
                  name,
                  schema,
                  shouldSanitize,
                  invalidCharMask)));
        }

        @Override
        public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true,
              convertToJavaObject(
                  MercifulJsonConverter::convertJsonToRowField,
                  value,
                  name,
                  schema,
                  shouldSanitize,
                  invalidCharMask).toArray());
        }
      };
    }

    private static JsonToAvroFieldProcessor generateMapTypeHandler() {
      return new JsonToAvroFieldProcessor() {
        public Map<String, Object> convertToJavaObject(
            ConvertInternalApi convertApi,
            Object value,
            String name,
            Schema schema,
            boolean shouldSanitize,
            String invalidCharMask) {
          Schema valueSchema = schema.getValueType();
          Map<String, Object> mapRes = new HashMap<>();
          for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
            mapRes.put(v.getKey(), convertApi.apply(v.getValue(), name, valueSchema, shouldSanitize, invalidCharMask));
          }
          return mapRes;
        }

        @Override
        public Pair<Boolean, Object> convert(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, convertToJavaObject(MercifulJsonConverter::convertJsonToAvroField,
              value,
              name,
              schema,
              shouldSanitize,
              invalidCharMask));
        }

        @Override
        public Pair<Boolean, Object> convertToRowInternal(Object value, String name, Schema schema, boolean shouldSanitize, String invalidCharMask) {
          return Pair.of(true, JavaConverters
              .mapAsScalaMapConverter(
                  convertToJavaObject(
                      MercifulJsonConverter::convertJsonToRowField,
                      value,
                      name,
                      schema,
                      shouldSanitize,
                      invalidCharMask)).asScala());
        }
      };
    }
  }

  /**
   * Exception Class for any schema conversion issue.
   */
  public static class HoodieJsonToAvroConversionException extends HoodieException {

    private final Object value;

    private final String fieldName;
    private final Schema schema;
    private final boolean shouldSanitize;
    private final String invalidCharMask;

    public HoodieJsonToAvroConversionException(Object value, String fieldName, Schema schema, boolean shouldSanitize, String invalidCharMask) {
      this.value = value;
      this.fieldName = fieldName;
      this.schema = schema;
      this.shouldSanitize = shouldSanitize;
      this.invalidCharMask = invalidCharMask;
    }

    @Override
    public String toString() {
      if (shouldSanitize) {
        return String.format("Json to Avro Type conversion error for field %s, %s for %s. Field sanitization is enabled with a mask of %s.", fieldName, value, schema, invalidCharMask);
      }
      return String.format("Json to Avro Type conversion error for field %s, %s for %s", fieldName, value, schema);
    }
  }
}
