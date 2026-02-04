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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrowEndToEndTest {

  @TempDir
  File tempDir;

  @Test
  public void testEndToEndArrowWorkflow() throws IOException {
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "message", Types.StringType.get()),
        Types.NestedField.optional(3, "timestamp", Types.TimestampType.withoutZone())
    );

    String location = tempDir.getAbsolutePath();
    HadoopTables tables = new HadoopTables(new Configuration());
    Table table = tables.create(
        schema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "arrow"),
        location
    );

    List<Record> records = Lists.newArrayList();

    GenericRecord record1 = GenericRecord.create(schema);
    record1.setField("id", 1);
    record1.setField("message", "Hello, Arrow!");
    record1.setField("timestamp", LocalDateTime.now());
    records.add(record1);

    GenericRecord record2 = GenericRecord.create(schema);
    record2.setField("id", 2);
    record2.setField("message", "Iceberg + Arrow = <3");
    record2.setField("timestamp", LocalDateTime.now());
    records.add(record2);

    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());

    File dataFile = new File(tempDir, "data/data.arrow");
    assertThat(dataFile.getParentFile().mkdirs()).isTrue();

    OutputFile outputFile = Files.localOutput(dataFile);
    try (FileAppender<Record> appender = appenderFactory.newAppender(outputFile, FileFormat.ARROW)) {
      appender.addAll(records);
    }

    DataFile file = DataFiles.builder(table.spec())
        .withPath(dataFile.getAbsolutePath())
        .withFileSizeInBytes(dataFile.length())
        .withRecordCount(records.size())
        .withFormat(FileFormat.ARROW)
        .build();

    table.newAppend()
        .appendFile(file)
        .commit();

    try (CloseableIterable<Record> result = IcebergGenerics.read(table).build()) {
      List<Record> readRecords = Lists.newArrayList(result);

      assertThat(readRecords).hasSize(2);
      assertThat(readRecords.get(0).getField("message")).isEqualTo("Hello, Arrow!");
      assertThat(readRecords.get(1).getField("message")).isEqualTo("Iceberg + Arrow = <3");
    }

    byte[] magic = new byte[6];
    try (java.io.FileInputStream fis = new java.io.FileInputStream(dataFile)) {
      assertThat(fis.read(magic)).isEqualTo(6);
    }
    assertThat(new String(magic)).isEqualTo("ARROW1");
  }
}

