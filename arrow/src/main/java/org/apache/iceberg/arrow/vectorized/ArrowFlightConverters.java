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
package org.apache.iceberg.arrow.vectorized;

import java.util.Collections;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

public class ArrowFlightConverters {

  public static CloseableIterable<ColumnarBatch> columnarBatchConverter(VectorSchemaRoot root, Schema projectSchema) {
    int rowCount = root.getRowCount();
    ColumnVector[] columns = new ColumnVector[projectSchema.columns().size()];
    
    for (int i = 0; i < projectSchema.columns().size(); i++) {
      Types.NestedField icebergField = projectSchema.columns().get(i);
      FieldVector vector = root.getVector(icebergField.name());
      if (vector == null) {
        // Handle missing columns as nulls
        columns[i] = new ColumnVector(VectorHolder.constantHolder(icebergField, rowCount, null));
      } else {
        NullabilityHolder nulls = new NullabilityHolder(rowCount);
        for (int j = 0; j < rowCount; j++) {
          if (vector.isNull(j)) {
            nulls.setNull(j);
          }
        }
        columns[i] = new ColumnVector(VectorHolder.vectorHolder(vector, icebergField, nulls));
      }
    }
    
    return CloseableIterable.withNoopClose(Collections.singletonList(new ColumnarBatch(rowCount, columns)));
  }

  private ArrowFlightConverters() {}
}
