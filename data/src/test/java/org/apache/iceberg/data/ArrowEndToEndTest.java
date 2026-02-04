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

/*
 * This test is for illustrative purposes only.
 */

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
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
    // 1. Define Schema
    Schema schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "message", Types.StringType.get()),
        Types.NestedField.optional(3, "timestamp", Types.TimestampType.withoutZone())
    );

    // 2. Create a Table with Arrow as the default file format
    String location = tempDir.getAbsolutePath();
    HadoopTables tables = new HadoopTables(new Configuration());
    Table table = tables.create(
        schema, 
        PartitionSpec.unpartitioned(), 
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "arrow"), 
        location
    );

    System.out.println("Created table at: " + location);

    // 3. Create Generic Records
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

    // 4. Append Data (uses GenericAppenderFactory -> Arrow.write)
    // We need to use the IcebergGenerics API or DataWriter directly. 
    // IcebergGenerics doesn't have a write helper easily accessible for tests without MapReduce/Spark usually,
    // but we can use the append API with a data file.
    
    // Actually, creating a data file manually using the factory is what internal tests do.
    // But for end-to-end, let's try to mimic a real write.
    // Since we are in 'iceberg-arrow', we might not have all the 'iceberg-data' write utilities easily wired up for a simple "table.append(records)" call 
    // because Iceberg core doesn't have a direct "insert records" API for tables, it works with DataFiles.
    
    // However, we can use the code from TestLocalScan to write the file.
    // Or simpler: use the Generics.write equivalent if it existed?
    
    // Let's manually write the file using the factory, which is what we want to test (Factory integration).
    
    // ... wait, the user wants to test end-to-end.
    // The easiest way is to use the `GenericAppenderFactory` implicitly via `GenericData.WriteBuilder` if it existed?
    // No, usually we create a DataWriter.
    
    org.apache.iceberg.io.FileAppenderFactory<Record> appenderFactory = 
        new org.apache.iceberg.data.GenericAppenderFactory(table.schema(), table.spec());
        
    // We need to write to a file first
    File dataFile = new File(tempDir, "data/data.arrow");
    dataFile.getParentFile().mkdirs();
    
    org.apache.iceberg.io.OutputFile outputFile = org.apache.iceberg.Files.localOutput(dataFile);
    org.apache.iceberg.io.FileAppender<Record> appender = appenderFactory.newAppender(outputFile, FileFormat.ARROW);
    
    try (org.apache.iceberg.io.FileAppender<Record> closeableAppender = appender) {
      closeableAppender.addAll(records);
    }
    
    DataFile file = org.apache.iceberg.DataFiles.builder(table.spec())
        .withPath(dataFile.getAbsolutePath())
        .withFileSizeInBytes(dataFile.length())
        .withRecordCount(records.size())
        .withFormat(FileFormat.ARROW)
        .build();

    // 5. Commit the append
    table.newAppend()
        .appendFile(file)
        .commit();
        
    System.out.println("Committed " + records.size() + " records to table.");

    // 6. Read back using IcebergGenerics (GenericReader)
    System.out.println("Reading data back...");
    CloseableIterable<Record> result = IcebergGenerics.read(table).build();
    
    List<Record> readRecords = Lists.newArrayList(result);
    
    for (Record r : readRecords) {
      System.out.println("Read: " + r);
    }

    // 7. Verify
    assertThat(readRecords).hasSize(2);
    assertThat(readRecords.get(0).getField("message")).isEqualTo("Hello, Arrow!");
    assertThat(readRecords.get(1).getField("message")).isEqualTo("Iceberg + Arrow = <3");
    
    // 8. Verify it really is an Arrow file by checking magic bytes "ARROW1"
    byte[] magic = new byte[6];
    try (java.io.FileInputStream fis = new java.io.FileInputStream(dataFile)) {
      fis.read(magic);
    }
    assertThat(new String(magic)).isEqualTo("ARROW1");
    System.out.println("Verified file magic bytes: ARROW1");
  }
}
