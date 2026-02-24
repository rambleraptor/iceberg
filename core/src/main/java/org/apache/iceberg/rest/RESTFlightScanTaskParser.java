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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.responses.FlightEndpoint;
import org.apache.iceberg.rest.responses.FlightScanTask;
import org.apache.iceberg.util.JsonUtil;

class RESTFlightScanTaskParser {
  private static final String ENDPOINT = "endpoint";
  private static final String TICKET = "ticket";
  private static final String LOCATIONS = "locations";
  private static final String TASK_ID = "task-id";
  private static final String SCHEMA = "schema";

  private RESTFlightScanTaskParser() {}

  public static void toJson(FlightScanTask flightScanTask, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(flightScanTask != null, "Invalid flight scan task: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();
    generator.writeFieldName(ENDPOINT);
    toJson(flightScanTask.endpoint(), generator);

    if (flightScanTask.taskId() != null) {
      generator.writeStringField(TASK_ID, flightScanTask.taskId());
    }

    if (flightScanTask.schema() != null) {
      generator.writeFieldName(SCHEMA);
      SchemaParser.toJson(flightScanTask.schema(), generator);
    }

    generator.writeEndObject();
  }

  public static void toJson(FlightEndpoint endpoint, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(endpoint != null, "Invalid flight endpoint: null");
    Preconditions.checkArgument(generator != null, "Invalid JSON generator: null");

    generator.writeStartObject();
    generator.writeStringField(TICKET, Base64.getEncoder().encodeToString(endpoint.ticket()));
    JsonUtil.writeStringArray(LOCATIONS, endpoint.locations(), generator);
    generator.writeEndObject();
  }

  public static FlightScanTask fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for flight scan task: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for flight scan task: non-object (%s)", jsonNode);

    FlightEndpoint endpoint = endpointFromJson(JsonUtil.get(ENDPOINT, jsonNode));
    String taskId = JsonUtil.getStringOrNull(TASK_ID, jsonNode);

    Schema schema = null;
    if (jsonNode.has(SCHEMA)) {
      schema = SchemaParser.fromJson(jsonNode.get(SCHEMA));
    }

    return FlightScanTask.builder()
        .withEndpoint(endpoint)
        .withTaskId(taskId)
        .withSchema(schema)
        .build();
  }

  private static FlightEndpoint endpointFromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null, "Invalid JSON node for flight endpoint: null");
    Preconditions.checkArgument(
        jsonNode.isObject(), "Invalid JSON node for flight endpoint: non-object (%s)", jsonNode);

    byte[] ticket = Base64.getDecoder().decode(JsonUtil.getString(TICKET, jsonNode));
    List<String> locations = JsonUtil.getStringList(LOCATIONS, jsonNode);

    return FlightEndpoint.builder().withTicket(ticket).withLocations(locations).build();
  }
}
