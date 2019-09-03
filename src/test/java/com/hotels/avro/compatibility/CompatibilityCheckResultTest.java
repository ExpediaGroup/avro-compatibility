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

import static com.hotels.avro.compatibility.Compatibility.CheckType.CAN_BE_READ_BY;
import static com.hotels.avro.compatibility.Compatibility.CheckType.CAN_READ;
import static com.hotels.avro.compatibility.Compatibility.ChronologyType.ALL;
import static com.hotels.avro.compatibility.Compatibility.ChronologyType.LATEST;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import org.apache.avro.SchemaCompatibility.SchemaCompatibilityResult;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

public class CompatibilityCheckResultTest {

  private Schema reader = SchemaBuilder.builder().intType();
  private Schema writer = SchemaBuilder.builder().longType();
  private SchemaPairCompatibility pairCompatible = new SchemaPairCompatibility(SchemaCompatibilityResult.compatible(),
      reader, writer, "description");
  private SchemaPairCompatibility pairIncompatible = new SchemaPairCompatibility(
      SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.FIXED_SIZE_MISMATCH, reader, writer, "message",
          Collections.singletonList("/")),
      reader, writer, "description");

  @Test
  public void testEquals() {
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .equals(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)), is(true));
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .equals(new CompatibilityCheckResult(CAN_BE_READ_BY, ALL, pairCompatible)), is(false));
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .equals(new CompatibilityCheckResult(CAN_READ, LATEST, pairCompatible)), is(false));
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .equals(new CompatibilityCheckResult(CAN_READ, ALL, pairIncompatible)), is(false));
  }

  @Test
  public void testHashCode() {
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .hashCode() == new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible).hashCode(), is(true));
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .hashCode() == new CompatibilityCheckResult(CAN_BE_READ_BY, ALL, pairCompatible).hashCode(), is(false));
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .hashCode() == new CompatibilityCheckResult(CAN_READ, LATEST, pairCompatible).hashCode(), is(false));
    assertThat(new CompatibilityCheckResult(CAN_READ, ALL, pairCompatible)
        .hashCode() == new CompatibilityCheckResult(CAN_READ, ALL, pairIncompatible).hashCode(), is(false));
  }

  @Test
  public void testAccessors() {
    CompatibilityCheckResult result = new CompatibilityCheckResult(CAN_READ, ALL, pairIncompatible);

    assertThat(result.getChronology(), is(ALL));
    assertThat(result.getCompatibility(), is(CAN_READ));
    assertThat(result.getDescription(), is("description"));
    assertThat(result.getResult(), is(pairIncompatible.getResult()));
    assertThat(result.getReader(), is(reader));
    assertThat(result.getWriter(), is(writer));
    assertThat(result.getType(), is(SchemaCompatibilityType.INCOMPATIBLE));
  }

  @Test
  public void testIsCompatible() {
    CompatibilityCheckResult result = new CompatibilityCheckResult(CAN_READ, ALL, pairIncompatible);
    assertThat(result.isCompatible(), is(false));
  }

  @Test
  public void testAsMessage() {
    CompatibilityCheckResult result = new CompatibilityCheckResult(CAN_READ, ALL, pairIncompatible);
    assertThat(result.asMessage(), is("Compatibility type 'CAN_READ' does not hold between schemas, incompatibilities: ['FIXED_SIZE_MISMATCH: message' at '/']."));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void testThrowIfIncompatible() throws SchemaCompatibilityException {
    new CompatibilityCheckResult(CAN_READ, ALL, pairIncompatible).throwIfIncompatible();
  }

}
