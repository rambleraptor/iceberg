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
package org.apache.iceberg.rest.responses;

import java.io.Serializable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlightScanTask implements Serializable {
  private final FlightEndpoint endpoint;
  private final String taskId;
  private final Schema schema;

  private FlightScanTask(FlightEndpoint endpoint, String taskId, Schema schema) {
    this.endpoint = endpoint;
    this.taskId = taskId;
    this.schema = schema;
    validate();
  }

  public FlightEndpoint endpoint() {
    return endpoint;
  }

  public String taskId() {
    return taskId;
  }

  public Schema schema() {
    return schema;
  }

  public void validate() {
    Preconditions.checkArgument(endpoint != null, "Invalid flight scan task: endpoint is null");
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("endpoint", endpoint)
        .add("taskId", taskId)
        .add("schema", schema)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private FlightEndpoint endpoint;
    private String taskId;
    private Schema schema;

    public Builder withEndpoint(FlightEndpoint endpointValue) {
      this.endpoint = endpointValue;
      return this;
    }

    public Builder withTaskId(String taskIdValue) {
      this.taskId = taskIdValue;
      return this;
    }

    public Builder withSchema(Schema schemaValue) {
      this.schema = schemaValue;
      return this;
    }

    public FlightScanTask build() {
      return new FlightScanTask(endpoint, taskId, schema);
    }
  }
}
