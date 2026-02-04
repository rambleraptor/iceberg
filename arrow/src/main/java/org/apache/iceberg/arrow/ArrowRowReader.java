/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.arrow;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

class ArrowRowReader<D> {
  private final List<ArrowValueReader> readers;
  private final int numFields;
  private final DynConstructors.Ctor<? extends StructLike> ctor;
  private final Types.StructType structType;

  ArrowRowReader(
      Schema schema,
      VectorSchemaRoot root,
      Class<? extends StructLike> rootClass,
      Map<Integer, Class<? extends StructLike>> customTypes,
      Map<Integer, Object> constants) {
    this.readers = Lists.newArrayList();
    this.numFields = schema.columns().size();
    this.structType = schema.asStruct();
    for (int i = 0; i < numFields; i++) {
      Types.NestedField field = schema.columns().get(i);
      
      if (constants.containsKey(field.fieldId())) {
        readers.add(new ConstantValueReader(constants.get(field.fieldId())));
        continue;
      }
      
      if (field.fieldId() == MetadataColumns.ROW_POSITION.fieldId()) {
        readers.add(new PosValueReader());
        continue;
      }

      FieldVector vector = root.getVector(field.name());
      
      if (vector != null) {
        readers.add(createValueReader(field.type(), vector, field.fieldId(), customTypes));
      } else {
        readers.add(new NullValueReader());
      }
    }

    if (rootClass != null) {
      this.ctor =
          DynConstructors.builder(rootClass)
              .hiddenImpl(rootClass, Types.StructType.class)
              .hiddenImpl(rootClass, Schema.class)
              .build();
    } else {
      this.ctor = null;
    }
  }

  @SuppressWarnings("unchecked")
  public D read(int rowId, long offset) {
    if (ctor != null) {
      try {
        StructLike struct = ctor.newInstance(structType);
        for (int i = 0; i < readers.size(); i++) {
          struct.set(i, readers.get(i).read(rowId, offset));
        }
        return (D) struct;
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate root class", e);
      }
    }
    
    GenericRecord record = GenericRecord.create(structType);
    for (int i = 0; i < readers.size(); i++) {
      record.set(i, readers.get(i).read(rowId, offset));
    }
    return (D) record;
  }

  private static ArrowValueReader createValueReader(
      Type type,
      FieldVector vector,
      Integer fieldId,
      Map<Integer, Class<? extends StructLike>> customTypes) {
    if (type.isStructType()) {
      return new StructValueReader(
          (StructVector) vector, type.asStructType(), fieldId, customTypes);
    }

    switch (type.typeId()) {
      case INTEGER:
        return new IntValueReader((IntVector) vector);
      case LONG:
        return new LongValueReader((BigIntVector) vector);
      case FLOAT:
        return new FloatValueReader((Float4Vector) vector);
      case DOUBLE:
        return new DoubleValueReader((Float8Vector) vector);
      case STRING:
        return new StringValueReader((VarCharVector) vector);
      case BOOLEAN:
        return new BooleanValueReader((BitVector) vector);
      case DECIMAL:
        return new DecimalValueReader((DecimalVector) vector);
      case DATE:
        return new DateValueReader((DateDayVector) vector);
      case TIME:
        return new TimeValueReader((TimeMicroVector) vector);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return new TimestampTzValueReader((TimeStampMicroTZVector) vector);
        } else {
          return new TimestampValueReader((TimeStampMicroVector) vector);
        }
      case TIMESTAMP_NANO:
        if (((Types.TimestampNanoType) type).shouldAdjustToUTC()) {
          return new TimestampNanoTzValueReader((TimeStampNanoTZVector) vector);
        } else {
          return new TimestampNanoValueReader((TimeStampNanoVector) vector);
        }
      case BINARY:
        return new BinaryValueReader((VarBinaryVector) vector);
      case FIXED:
        return new FixedValueReader((FixedSizeBinaryVector) vector);
      case UUID:
        return new UUIDValueReader((FixedSizeBinaryVector) vector);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private interface ArrowValueReader {
    Object read(int rowId, long offset);
  }

  private static class NullValueReader implements ArrowValueReader {
    @Override
    public Object read(int rowId, long offset) {
      return null;
    }
  }

  private static class ConstantValueReader implements ArrowValueReader {
    private final Object value;

    ConstantValueReader(Object value) {
      this.value = value;
    }

    @Override
    public Object read(int rowId, long offset) {
      return value;
    }
  }

  private static class PosValueReader implements ArrowValueReader {
    @Override
    public Object read(int rowId, long offset) {
      return offset + rowId;
    }
  }

  private static class StructValueReader implements ArrowValueReader {
    private final StructVector vector;
    private final List<ArrowValueReader> readers;
    private final DynConstructors.Ctor<? extends StructLike> ctor;
    private final Types.StructType structType;

    StructValueReader(
        StructVector vector,
        Types.StructType structType,
        Integer fieldId,
        Map<Integer, Class<? extends StructLike>> customTypes) {
      this.vector = vector;
      this.structType = structType;
      this.readers = Lists.newArrayList();
      List<Types.NestedField> fields = structType.fields();
      for (Types.NestedField field : fields) {
        FieldVector child = vector.getChild(field.name());
        if (child != null) {
          readers.add(
              createValueReader(field.type(), child, field.fieldId(), customTypes));
        } else {
          readers.add(new NullValueReader());
        }
      }

      Class<? extends StructLike> customClass = customTypes.get(fieldId);
      if (customClass != null) {
        this.ctor =
            DynConstructors.builder(customClass)
                .hiddenImpl(customClass, Types.StructType.class)
                .hiddenImpl(customClass, Schema.class)
                .hiddenImpl(customClass)
                .build();
      } else {
        this.ctor = null;
      }
    }

    @Override
    public Object read(int rowId, long offset) {
      if (vector.isNull(rowId)) {
        return null;
      }

      if (ctor != null) {
        StructLike struct = ctor.newInstance(structType);
        for (int i = 0; i < readers.size(); i++) {
          struct.set(i, readers.get(i).read(rowId, offset));
        }
        return struct;
      }

      GenericRecord record = GenericRecord.create(structType);
      for (int i = 0; i < readers.size(); i++) {
        record.set(i, readers.get(i).read(rowId, offset));
      }
      return record;
    }
  }

  private static class IntValueReader implements ArrowValueReader {
    private final IntVector vector;

    IntValueReader(IntVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class LongValueReader implements ArrowValueReader {
    private final BigIntVector vector;

    LongValueReader(BigIntVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class FloatValueReader implements ArrowValueReader {
    private final Float4Vector vector;

    FloatValueReader(Float4Vector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class DoubleValueReader implements ArrowValueReader {
    private final Float8Vector vector;

    DoubleValueReader(Float8Vector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class StringValueReader implements ArrowValueReader {
    private final VarCharVector vector;

    StringValueReader(VarCharVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : new String(vector.get(rowId), StandardCharsets.UTF_8);
    }
  }

  private static class BooleanValueReader implements ArrowValueReader {
    private final BitVector vector;

    BooleanValueReader(BitVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId) != 0;
    }
  }

  private static class DecimalValueReader implements ArrowValueReader {
    private final DecimalVector vector;

    DecimalValueReader(DecimalVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.getObject(rowId);
    }
  }

  private static class DateValueReader implements ArrowValueReader {
    private final DateDayVector vector;

    DateValueReader(DateDayVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : DateTimeUtil.dateFromDays(vector.get(rowId));
    }
  }

  private static class TimeValueReader implements ArrowValueReader {
    private final TimeMicroVector vector;

    TimeValueReader(TimeMicroVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : DateTimeUtil.timeFromMicros(vector.get(rowId));
    }
  }

  private static class TimestampValueReader implements ArrowValueReader {
    private final TimeStampMicroVector vector;

    TimestampValueReader(TimeStampMicroVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : DateTimeUtil.timestampFromMicros(vector.get(rowId));
    }
  }

  private static class TimestampTzValueReader implements ArrowValueReader {
    private final TimeStampMicroTZVector vector;

    TimestampTzValueReader(TimeStampMicroTZVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : DateTimeUtil.timestamptzFromMicros(vector.get(rowId));
    }
  }

  private static class TimestampNanoValueReader implements ArrowValueReader {
    private final TimeStampNanoVector vector;

    TimestampNanoValueReader(TimeStampNanoVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class TimestampNanoTzValueReader implements ArrowValueReader {
    private final TimeStampNanoTZVector vector;

    TimestampNanoTzValueReader(TimeStampNanoTZVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class BinaryValueReader implements ArrowValueReader {
    private final VarBinaryVector vector;

    BinaryValueReader(VarBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : ByteBuffer.wrap(vector.get(rowId));
    }
  }

  private static class FixedValueReader implements ArrowValueReader {
    private final FixedSizeBinaryVector vector;

    FixedValueReader(FixedSizeBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }

  private static class UUIDValueReader implements ArrowValueReader {
    private final FixedSizeBinaryVector vector;

    UUIDValueReader(FixedSizeBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public Object read(int rowId, long offset) {
      return vector.isNull(rowId) ? null : vector.get(rowId);
    }
  }
}
