/**
 * Copyright (C) 2017-2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.avro.compatibility;

import java.util.Collections;
import java.util.List;

import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;

import com.hotels.avro.compatibility.Compatibility.CheckType;
import com.hotels.avro.compatibility.Compatibility.ChronologyType;

public class ChronologyCompatibilityCheckResult {

  private final List<CompatibilityCheckResult> results;
  private final CheckType compatibility;
  private final ChronologyType chronology;

  ChronologyCompatibilityCheckResult(
      CheckType strategy,
      ChronologyType chronologyStrategy,
      List<CompatibilityCheckResult> results) {
    this.compatibility = strategy;
    this.chronology = chronologyStrategy;
    this.results = Collections.unmodifiableList(results);
  }

  public List<CompatibilityCheckResult> getResults() {
    return results;
  }

  public SchemaCompatibilityType getType() {
    return isCompatible() ? SchemaCompatibilityType.COMPATIBLE : SchemaCompatibilityType.INCOMPATIBLE;
  }

  public boolean isCompatible() {
    for (CompatibilityCheckResult result : results) {
      if (!result.isCompatible()) {
        return false;
      }
    }
    return true;
  }

  public void throwIfIncompatible() throws SchemaCompatibilityException {
    if (!isCompatible()) {
      throw new SchemaCompatibilityException(this);
    }
  }

  public CheckType getCompatibility() {
    return compatibility;
  }

  public ChronologyType getChronology() {
    return chronology;
  }

  public String asMessage() {
    if (isCompatible()) {
      return String.format("Compatibility type '%s' holds for '%s' schema(s).", compatibility, chronology);
    }

    int incompatibleCount = 0;
    for (CompatibilityCheckResult result : results) {
      if (!result.isCompatible()) {
        incompatibleCount++;
      }
    }
    StringBuilder message = new StringBuilder(
        String.format("Compatibility type '%s' does not hold between %s schema(s) in the chronology because: ",
            compatibility, incompatibleCount));
    int i = 0;
    for (CompatibilityCheckResult result : results) {
      if (!result.isCompatible()) {
        if (i > 0) {
          message.append(", ");
        }
        message.append(String.format("Schema[%s] has incompatibilities: [", i));
        boolean first = true;
        for (Incompatibility incompatibility : result.getResult().getIncompatibilities()) {
          if (first) {
            first = false;
          } else {
            message.append(',');
          }
          message.append(String.format("'%s: %s' at '%s'", incompatibility.getType(),
              incompatibility.getMessage(), incompatibility.getLocation()));
        }
        message.append(']');
      }
      i++;
    }
    message.append(".");
    return message.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((chronology == null) ? 0 : chronology.hashCode());
    result = prime * result + ((compatibility == null) ? 0 : compatibility.hashCode());
    result = prime * result + ((results == null) ? 0 : results.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ChronologyCompatibilityCheckResult other = (ChronologyCompatibilityCheckResult) obj;
    if (chronology != other.chronology)
      return false;
    if (compatibility != other.compatibility)
      return false;
    if (results == null) {
      if (other.results != null)
        return false;
    } else if (!results.equals(other.results))
      return false;
    return true;
  }

}
