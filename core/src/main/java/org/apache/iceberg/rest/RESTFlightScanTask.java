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
package org.apache.iceberg.rest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.ArrowFlightInputFile;
import org.apache.iceberg.rest.responses.FlightScanTask;

public class RESTFlightScanTask implements FileScanTask, Serializable {
  private final FlightScanTask flightTask;
  private final DataFile dataFile;
  private final Schema schema;

  public RESTFlightScanTask(FlightScanTask flightTask, Schema schema) {
    this.flightTask = flightTask;
    this.schema = schema;
    
    // Register the endpoint so it can be retrieved by task ID
    ArrowFlightInputFile.register(flightTask.taskId(), flightTask.endpoint());
    this.dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(new ArrowFlightInputFile(flightTask.taskId()).location())
        .withFormat(FileFormat.ARROW_FLIGHT)
        .withFileSizeInBytes(1024)
        .withRecordCount(0)
        .build();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // Re-register on deserialization (e.g. on Spark executors)
    ArrowFlightInputFile.register(flightTask.taskId(), flightTask.endpoint());
  }

  public FlightScanTask flightTask() {
    return flightTask;
  }

  @Override
  public DataFile file() {
    return dataFile;
  }

  @Override
  public List<DeleteFile> deletes() {
    return Collections.emptyList();
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public PartitionSpec spec() {
    return PartitionSpec.unpartitioned();
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return 0;
  }

  @Override
  public Expression residual() {
    return Expressions.alwaysTrue();
  }

  @Override
  public Iterable<FileScanTask> split(long splitSize) {
    return Collections.singletonList(this);
  }
}
