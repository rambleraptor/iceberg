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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.TableScanContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.FlightScanTask;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;

public class RESTFlightTableScan extends DataTableScan {
  private final RESTClient client;
  private final Map<String, String> headers;
  private final TableOperations operations;
  private final Table table;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Set<Endpoint> supportedEndpoints;
  private final ParserContext parserContext;

  RESTFlightTableScan(
      Table table,
      Schema schema,
      TableScanContext context,
      RESTClient client,
      Map<String, String> headers,
      TableOperations operations,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths,
      Set<Endpoint> supportedEndpoints) {
    super(table, schema, context);
    this.table = table;
    this.client = client;
    this.headers = headers;
    this.operations = operations;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
    this.supportedEndpoints = supportedEndpoints;
    this.parserContext =
        ParserContext.builder()
            .add("specsById", table.specs())
            .add("caseSensitive", context().caseSensitive())
            .build();
  }

  @Override
  protected TableScan newRefinedScan(
      Table refinedTable, Schema refinedSchema, TableScanContext refinedContext) {
    return new RESTFlightTableScan(
        refinedTable,
        refinedSchema,
        refinedContext,
        client,
        headers,
        operations,
        tableIdentifier,
        resourcePaths,
        supportedEndpoints);
  }

  @Override
  public Snapshot snapshot() {
    System.out.println("DEBUG: RESTFlightTableScan.snapshot() called!");
    Snapshot snapshot = super.snapshot();
    if (snapshot == null && table.currentSnapshot() == null) {
      return new DummySnapshot();
    }
    return snapshot != null ? snapshot : table.currentSnapshot();
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    System.out.println("DEBUG: RESTFlightTableScan.planFiles() called!");
    return doPlanFiles();
  }

  @Override
  protected boolean useSnapshotSchema() {
    return false;
  }

  @Override
  public CloseableIterable<FileScanTask> doPlanFiles() {
    System.out.println("DEBUG: RESTFlightTableScan.doPlanFiles() called!");
    Long startSnapshotId = context().fromSnapshotId();
    Long endSnapshotId = context().toSnapshotId();
    Long snapshotId = snapshotId();
    List<String> selectedColumns =
        schema().columns().stream().map(Types.NestedField::name).collect(Collectors.toList());

    PlanTableScanRequest.Builder builder =
        PlanTableScanRequest.builder()
            .withSelect(selectedColumns)
            .withFilter(filter())
            .withCaseSensitive(isCaseSensitive())
            .withRequestedFormat("arrow-flight");

    if (startSnapshotId != null && endSnapshotId != null) {
      builder
          .withStartSnapshotId(startSnapshotId)
          .withEndSnapshotId(endSnapshotId)
          .withUseSnapshotSchema(true);
    } else if (snapshotId != null && snapshotId != -1) {
      boolean useSnapShotSchema =
          table.currentSnapshot() != null && snapshotId != table.currentSnapshot().snapshotId();
      builder.withSnapshotId(snapshotId).withUseSnapshotSchema(useSnapShotSchema);
    }

    PlanTableScanResponse response =
        client.post(
            resourcePaths.planTableScan(tableIdentifier),
            builder.build(),
            PlanTableScanResponse.class,
            headers,
            ErrorHandlers.tableErrorHandler(),
            stringStringMap -> {},
            parserContext);

    if (response.planStatus() != PlanStatus.COMPLETED) {
      // For simplicity in this POC, we only handle synchronous completion
      throw new UnsupportedOperationException("Async planning for Flight tasks not yet supported");
    }

    System.out.println("DEBUG: Received " + (response.flightScanTasks() != null ? response.flightScanTasks().size() : 0) + " flight scan tasks");

    List<FileScanTask> tasks = response.flightScanTasks().stream()
        .map(t -> new RESTFlightScanTask(t, schema()))
        .collect(Collectors.toList());

    return CloseableIterable.withNoopClose(tasks);
  }

  public CloseableIterable<FlightScanTask> planFlightTasks() {
    // Deprecated or kept for internal use
    return CloseableIterable.withNoopClose(
        StreamSupport.stream(doPlanFiles().spliterator(), false)
            .map(t -> ((RESTFlightScanTask) t).flightTask())
            .collect(Collectors.toList()));
  }

  public static class DummySnapshot implements Snapshot {
    @Override
    public long sequenceNumber() { return 1; }
    @Override
    public long snapshotId() { return 1; }
    @Override
    public Long parentId() { return null; }
    @Override
    public long timestampMillis() { return 1600000000000L; }
    @Override
    public List<ManifestFile> allManifests(FileIO io) { return Collections.emptyList(); }
    @Override
    public List<ManifestFile> dataManifests(FileIO io) { return Collections.emptyList(); }
    @Override
    public List<ManifestFile> deleteManifests(FileIO io) { return Collections.emptyList(); }
    @Override
    public String operation() { return "flight"; }
    @Override
    public Map<String, String> summary() { return Collections.emptyMap(); }
    @Override
    public Iterable<DataFile> addedDataFiles(FileIO io) { return Collections.emptyList(); }
    @Override
    public Iterable<DataFile> removedDataFiles(FileIO io) { return Collections.emptyList(); }
    @Override
    public String manifestListLocation() { return null; }
  }
}
