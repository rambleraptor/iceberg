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

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.BatchScanAdapter;
import org.apache.iceberg.ImmutableTableScanContext;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.metrics.MetricsReporter;

class RESTTable extends BaseTable implements SupportsDistributedScanPlanning, SupportsFlightScan {
  private final RESTClient client;
  private final Supplier<Map<String, String>> headers;
  private final MetricsReporter reporter;
  private final ResourcePaths resourcePaths;
  private final TableIdentifier tableIdentifier;
  private final Set<Endpoint> supportedEndpoints;
  private final Map<String, String> catalogProperties;
  private final Object hadoopConf;

  RESTTable(
      TableOperations ops,
      String name,
      MetricsReporter reporter,
      RESTClient client,
      Supplier<Map<String, String>> headers,
      TableIdentifier tableIdentifier,
      ResourcePaths resourcePaths,
      Set<Endpoint> supportedEndpoints,
      Map<String, String> catalogProperties,
      Object hadoopConf) {
    super(ops, name, reporter);
    this.reporter = reporter;
    this.client = client;
    this.headers = headers;
    this.tableIdentifier = tableIdentifier;
    this.resourcePaths = resourcePaths;
    this.supportedEndpoints = supportedEndpoints;
    this.catalogProperties = catalogProperties;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public Snapshot currentSnapshot() {
    Snapshot snapshot = super.currentSnapshot();
    if (snapshot == null && catalogProperties.getOrDefault("rest.use-flight", "false").equals("true")) {
      return new RESTFlightTableScan.DummySnapshot();
    }
    return snapshot;
  }

  @Override
  public TableOperations operations() {
    TableOperations ops = super.operations();
    if (catalogProperties.getOrDefault("rest.use-flight", "false").equals("true")) {
      return new FlightTableOperations(ops);
    }
    return ops;
  }

  private static class FlightTableOperations implements TableOperations {
    private final TableOperations delegate;

    FlightTableOperations(TableOperations delegate) {
      this.delegate = delegate;
    }

    @Override
    public TableMetadata current() {
      TableMetadata current = delegate.current();
      if (current.currentSnapshot() == null) {
        Snapshot dummy = new RESTFlightTableScan.DummySnapshot();
        return TableMetadata.buildFrom(current)
            .addSnapshot(dummy)
            .setBranchSnapshot(dummy.snapshotId(), "main")
            .build();
      }
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }
  }

  @Override
  public TableScan newScan() {
    System.out.println("DEBUG: RESTTable.newScan() called!");
    if (catalogProperties.getOrDefault("rest.use-flight", "false").equals("true") ||
        catalogProperties.getOrDefault("use-flight", "false").equals("true")) {
      return newFlightScan();
    }
    
    return new RESTTableScan(
        this,
        schema(),
        ImmutableTableScanContext.builder().metricsReporter(reporter).build(),
        client,
        headers.get(),
        operations(),
        tableIdentifier,
        resourcePaths,
        supportedEndpoints,
        io(),
        catalogProperties,
        hadoopConf);
  }

  @Override
  public RESTFlightTableScan newFlightScan() {
    return new RESTFlightTableScan(
        this,
        schema(),
        ImmutableTableScanContext.builder().metricsReporter(reporter).build(),
        client,
        headers.get(),
        operations(),
        tableIdentifier,
        resourcePaths,
        supportedEndpoints);
  }

  @Override
  public BatchScan newBatchScan() {
    return new BatchScanAdapter(newScan());
  }

  @Override
  public boolean allowDistributedPlanning() {
    return false;
  }
}
