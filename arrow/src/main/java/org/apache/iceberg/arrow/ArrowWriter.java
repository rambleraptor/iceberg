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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class ArrowWriter<D> {
  private final List<ArrowValueWriter> writers;

  private ArrowWriter(List<ArrowValueWriter> writers) {
    this.writers = writers;
  }

  public static <D> ArrowWriter<D> create(Schema schema, VectorSchemaRoot root) {
    List<ArrowValueWriter> writers = Lists.newArrayList();
    List<Types.NestedField> fields = schema.columns();
    for (int i = 0; i < fields.size(); i++) {
      writers.add(createValueWriter(fields.get(i).type(), root.getVector(i)));
    }
    return new ArrowWriter<>(writers);
  }

  @SuppressWarnings("unchecked")
  public void write(int index, D datum) {
    StructLike struct = (StructLike) datum;
    for (int i = 0; i < writers.size(); i++) {
      writers.get(i).write(index, struct.get(i, Object.class));
    }
  }

  private static ArrowValueWriter createValueWriter(Type type, FieldVector vector) {
    switch (type.typeId()) {
      case INTEGER:
        return new IntValueWriter((IntVector) vector);
      case LONG:
        return new LongValueWriter((BigIntVector) vector);
      case FLOAT:
        return new FloatValueWriter((Float4Vector) vector);
      case DOUBLE:
        return new DoubleValueWriter((Float8Vector) vector);
      case STRING:
        return new StringValueWriter((VarCharVector) vector);
      case BOOLEAN:
        return new BooleanValueWriter((BitVector) vector);
      case DECIMAL:
        return new DecimalValueWriter((DecimalVector) vector);
      case DATE:
        return new DateValueWriter((DateDayVector) vector);
      case TIME:
        return new TimeValueWriter((TimeMicroVector) vector);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return new TimestampTzValueWriter((TimeStampMicroTZVector) vector);
        } else {
          return new TimestampValueWriter((TimeStampMicroVector) vector);
        }
      case TIMESTAMP_NANO:
        if (((Types.TimestampNanoType) type).shouldAdjustToUTC()) {
          return new TimestampNanoTzValueWriter((TimeStampNanoTZVector) vector);
        } else {
          return new TimestampNanoValueWriter((TimeStampNanoVector) vector);
        }
      case BINARY:
        return new BinaryValueWriter((VarBinaryVector) vector);
      case FIXED:
        return new FixedValueWriter((FixedSizeBinaryVector) vector);
      case UUID:
        return new UUIDValueWriter((FixedSizeBinaryVector) vector);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private interface ArrowValueWriter {
    void write(int index, Object value);
  }

  private static class IntValueWriter implements ArrowValueWriter {
    private final IntVector vector;

    IntValueWriter(IntVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Integer) value);
      }
    }
  }

  private static class LongValueWriter implements ArrowValueWriter {
    private final BigIntVector vector;

    LongValueWriter(BigIntVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Long) value);
      }
    }
  }

  private static class FloatValueWriter implements ArrowValueWriter {
    private final Float4Vector vector;

    FloatValueWriter(Float4Vector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Float) value);
      }
    }
  }

  private static class DoubleValueWriter implements ArrowValueWriter {
    private final Float8Vector vector;

    DoubleValueWriter(Float8Vector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Double) value);
      }
    }
  }

  private static class StringValueWriter implements ArrowValueWriter {
    private final VarCharVector vector;

    StringValueWriter(VarCharVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private static class BooleanValueWriter implements ArrowValueWriter {
    private final BitVector vector;

    BooleanValueWriter(BitVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Boolean) value ? 1 : 0);
      }
    }
  }

  private static class DecimalValueWriter implements ArrowValueWriter {
    private final DecimalVector vector;

    DecimalValueWriter(DecimalVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (BigDecimal) value);
      }
    }
  }

  private static class DateValueWriter implements ArrowValueWriter {
    private final DateDayVector vector;

    DateValueWriter(DateDayVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Integer) value);
      }
    }
  }

  private static class TimeValueWriter implements ArrowValueWriter {
    private final TimeMicroVector vector;

    TimeValueWriter(TimeMicroVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Long) value);
      }
    }
  }

  private static class TimestampValueWriter implements ArrowValueWriter {
    private final TimeStampMicroVector vector;

    TimestampValueWriter(TimeStampMicroVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Long) value);
      }
    }
  }

  private static class TimestampTzValueWriter implements ArrowValueWriter {
    private final TimeStampMicroTZVector vector;

    TimestampTzValueWriter(TimeStampMicroTZVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Long) value);
      }
    }
  }

  private static class TimestampNanoValueWriter implements ArrowValueWriter {
    private final TimeStampNanoVector vector;

    TimestampNanoValueWriter(TimeStampNanoVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Long) value);
      }
    }
  }

  private static class TimestampNanoTzValueWriter implements ArrowValueWriter {
    private final TimeStampNanoTZVector vector;

    TimestampNanoTzValueWriter(TimeStampNanoTZVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (Long) value);
      }
    }
  }

  private static class BinaryValueWriter implements ArrowValueWriter {
    private final VarBinaryVector vector;

    BinaryValueWriter(VarBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        ByteBuffer bb = (ByteBuffer) value;
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        vector.setSafe(index, bytes);
      }
    }
  }

  private static class FixedValueWriter implements ArrowValueWriter {
    private final FixedSizeBinaryVector vector;

    FixedValueWriter(FixedSizeBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        vector.setSafe(index, (byte[]) value);
      }
    }
  }

  private static class UUIDValueWriter implements ArrowValueWriter {
    private final FixedSizeBinaryVector vector;

    UUIDValueWriter(FixedSizeBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public void write(int index, Object value) {
      if (value == null) {
        vector.setNull(index);
      } else {
        if (value instanceof java.util.UUID) {
          java.util.UUID uuid = (java.util.UUID) value;
          ByteBuffer bb = ByteBuffer.allocate(16);
          bb.putLong(uuid.getMostSignificantBits());
          bb.putLong(uuid.getLeastSignificantBits());
          vector.setSafe(index, bb.array());
        } else {
          vector.setSafe(index, (byte[]) value);
        }
      }
    }
  }
}
