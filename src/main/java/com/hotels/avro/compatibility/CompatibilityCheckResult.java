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

import org.apache.avro.Schema;

import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityResult;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

import com.hotels.avro.compatibility.Compatibility.CheckType;
import com.hotels.avro.compatibility.Compatibility.ChronologyType;

public class CompatibilityCheckResult {

  private final SchemaPairCompatibility schemaPairCompatibility;
  private final CheckType compatibility;
  private final ChronologyType chronology;

  CompatibilityCheckResult(
      CheckType compatibility,
      ChronologyType chronology,
      SchemaPairCompatibility schemaPairCompatibility) {
    this.compatibility = compatibility;
    this.chronology = chronology;
    this.schemaPairCompatibility = schemaPairCompatibility;
  }

  public SchemaCompatibilityType getType() {
    return schemaPairCompatibility.getType();
  }

  public SchemaCompatibilityResult getResult() {
    return schemaPairCompatibility.getResult();
  }

  public Schema getReader() {
    return schemaPairCompatibility.getReader();
  }

  public Schema getWriter() {
    return schemaPairCompatibility.getWriter();
  }

  public String getDescription() {
    return schemaPairCompatibility.getDescription();
  }

  public String asMessage() {
    if (isCompatible()) {
      return String.format("Compatibility type '%s' holds between schemas.", compatibility, chronology);
    } else {
      StringBuilder message = new StringBuilder();
      message.append(String.format("Compatibility type '%s' does not hold between schemas, incompatibilities: [",
          getCompatibility()));
      boolean first = true;
      for (Incompatibility incompatibility : getResult().getIncompatibilities()) {
        if (first) {
          first = false;
        } else {
          message.append(',');
        }
        message.append(String.format("'%s: %s' at '%s'", incompatibility.getType(),
            incompatibility.getMessage(), incompatibility.getLocation()));
      }
      message.append("].");
      return message.toString();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((chronology == null) ? 0 : chronology.hashCode());
    result = prime * result + ((compatibility == null) ? 0 : compatibility.hashCode());
    result = prime * result + ((schemaPairCompatibility == null) ? 0 : schemaPairCompatibility.hashCode());
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
    CompatibilityCheckResult other = (CompatibilityCheckResult) obj;
    if (chronology != other.chronology)
      return false;
    if (compatibility != other.compatibility)
      return false;
    if (schemaPairCompatibility == null) {
      if (other.schemaPairCompatibility != null)
        return false;
    } else if (!schemaPairCompatibility.equals(other.schemaPairCompatibility))
      return false;
    return true;
  }

  public boolean isCompatible() {
    return getType() == SchemaCompatibilityType.COMPATIBLE;
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

  @Override
  public String toString() {
    return "SchemaCompatibilityResult [compatibility="
        + compatibility
        + ", chronology="
        + chronology
        + ", schemaPairCompatibility="
        + schemaPairCompatibility
        + "]";
  }

}
