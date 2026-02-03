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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestArrowIPC {

  @TempDir
  File temp;

  @Test
  public void testWriteAndReadArrowIPC() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "long", Types.LongType.get()),
        Types.NestedField.optional(4, "float", Types.FloatType.get()),
        Types.NestedField.optional(5, "double", Types.DoubleType.get()),
        Types.NestedField.optional(6, "decimal", Types.DecimalType.of(10, 2)),
        Types.NestedField.optional(7, "boolean", Types.BooleanType.get()),
        Types.NestedField.optional(8, "binary", Types.BinaryType.get()),
        Types.NestedField.optional(9, "uuid", Types.UUIDType.get())
    );

    File arrowFile = new File(temp, "test.arrow");
    
    List<StructLike> rows = Lists.newArrayList();
    UUID uuid1 = UUID.randomUUID();
    rows.add(TestHelpers.Row.of(1, "a", 10L, 1.1f, 1.2d, new BigDecimal("12.34"), true, ByteBuffer.wrap(new byte[]{1, 2}), uuid1));
    rows.add(TestHelpers.Row.of(2, "b", 20L, 2.1f, 2.2d, new BigDecimal("23.45"), false, ByteBuffer.wrap(new byte[]{3, 4}), UUID.randomUUID()));
    rows.add(TestHelpers.Row.of(3, null, null, null, null, null, null, null, null));

    try (FileAppender<StructLike> appender = Arrow.write(Files.localOutput(arrowFile))
        .schema(schema)
        .build()) {
      appender.addAll(rows);
    }

    try (CloseableIterable<StructLike> reader = Arrow.read(Files.localInput(arrowFile))
        .project(schema)
        .build()) {
      List<StructLike> readRows = Lists.newArrayList(reader);
      assertThat(readRows).hasSize(3);
      
      assertThat(readRows.get(0).get(0, Integer.class)).isEqualTo(1);
      assertThat(readRows.get(0).get(1, String.class)).isEqualTo("a");
      assertThat(readRows.get(0).get(2, Long.class)).isEqualTo(10L);
      assertThat(readRows.get(0).get(3, Float.class)).isEqualTo(1.1f);
      assertThat(readRows.get(0).get(4, Double.class)).isEqualTo(1.2d);
      assertThat(readRows.get(0).get(5, BigDecimal.class)).isEqualTo(new BigDecimal("12.34"));
      assertThat(readRows.get(0).get(6, Boolean.class)).isTrue();
      assertThat(readRows.get(0).get(7, ByteBuffer.class)).isEqualTo(ByteBuffer.wrap(new byte[]{1, 2}));
      // UUID is read back as byte[] in my implementation for now
      assertThat(readRows.get(0).get(8, byte[].class)).isEqualTo(uuidToBytes(uuid1));

      assertThat(readRows.get(2).get(0, Integer.class)).isEqualTo(3);
      assertThat(readRows.get(2).get(1, String.class)).isNull();
      assertThat(readRows.get(2).get(2, Long.class)).isNull();
      assertThat(readRows.get(2).get(3, Float.class)).isNull();
      assertThat(readRows.get(2).get(4, Double.class)).isNull();
      assertThat(readRows.get(2).get(5, BigDecimal.class)).isNull();
      assertThat(readRows.get(2).get(6, Boolean.class)).isNull();
      assertThat(readRows.get(2).get(7, ByteBuffer.class)).isNull();
      assertThat(readRows.get(2).get(8, byte[].class)).isNull();
    }
  }

  @Test
  public void testWriteMultipleBatches() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get())
    );

    File arrowFile = new File(temp, "test_batches.arrow");
    
    // Default batch size is 1024, let's write 2500 records
    int numRecords = 2500;
    try (FileAppender<StructLike> appender = Arrow.write(Files.localOutput(arrowFile))
        .schema(schema)
        .build()) {
      for (int i = 0; i < numRecords; i++) {
        appender.add(TestHelpers.Row.of(i));
      }
    }

    try (CloseableIterable<StructLike> reader = Arrow.read(Files.localInput(arrowFile))
        .project(schema)
        .build()) {
      List<StructLike> readRows = Lists.newArrayList(reader);
      assertThat(readRows).hasSize(numRecords);
      for (int i = 0; i < numRecords; i++) {
        assertThat(readRows.get(i).get(0, Integer.class)).isEqualTo(i);
      }
    }
  }

  @Test
  public void testTemporalTypes() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "date", Types.DateType.get()),
        Types.NestedField.required(2, "time", Types.TimeType.get()),
        Types.NestedField.required(3, "timestamp", Types.TimestampType.withoutZone()),
        Types.NestedField.required(4, "timestamp_tz", Types.TimestampType.withZone())
    );

    File arrowFile = new File(temp, "test_temporal.arrow");
    
    List<StructLike> rows = Lists.newArrayList();
    rows.add(TestHelpers.Row.of(100, 1000L, 1000000L, 2000000L));

    try (FileAppender<StructLike> appender = Arrow.write(Files.localOutput(arrowFile))
        .schema(schema)
        .build()) {
      appender.addAll(rows);
    }

    try (CloseableIterable<StructLike> reader = Arrow.read(Files.localInput(arrowFile))
        .project(schema)
        .build()) {
      List<StructLike> readRows = Lists.newArrayList(reader);
      assertThat(readRows).hasSize(1);
      assertThat(readRows.get(0).get(0, Integer.class)).isEqualTo(100);
      assertThat(readRows.get(0).get(1, Long.class)).isEqualTo(1000L);
      assertThat(readRows.get(0).get(2, Long.class)).isEqualTo(1000000L);
      assertThat(readRows.get(0).get(3, Long.class)).isEqualTo(2000000L);
    }
  }

  private byte[] uuidToBytes(UUID uuid) {
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return bb.array();
  }
}
