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

public class ChronologyCompatibilityCheckResultTest {

  private Schema reader = SchemaBuilder.builder().intType();
  private Schema writer = SchemaBuilder.builder().longType();
  private CompatibilityCheckResult resultCompatible = new CompatibilityCheckResult(CAN_READ, ALL,
      new SchemaPairCompatibility(SchemaCompatibilityResult.compatible(), reader, writer, "description"));
  private CompatibilityCheckResult resultIncompatible = new CompatibilityCheckResult(CAN_READ, ALL,
      new SchemaPairCompatibility(SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.FIXED_SIZE_MISMATCH,
          reader, writer, "message", Collections.singletonList("/")), reader, writer, "description"));

  @Test
  public void testEquals() {
    assertThat(
        new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible))
            .equals(new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible))),
        is(true));
    assertThat(
        new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible)).equals(
            new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultIncompatible))),
        is(false));
    assertThat(
        new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible)).equals(
            new ChronologyCompatibilityCheckResult(CAN_READ, LATEST, Collections.singletonList(resultCompatible))),
        is(false));
    assertThat(
        new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible)).equals(
            new ChronologyCompatibilityCheckResult(CAN_BE_READ_BY, ALL, Collections.singletonList(resultCompatible))),
        is(false));
  }

  @Test
  public void testHashCode() {
    assertThat(new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible))
        .hashCode() == new ChronologyCompatibilityCheckResult(CAN_READ, ALL,
            Collections.singletonList(resultCompatible)).hashCode(),
        is(true));
    assertThat(new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible))
        .hashCode() == new ChronologyCompatibilityCheckResult(CAN_READ, ALL,
            Collections.singletonList(resultIncompatible)).hashCode(),
        is(false));
    assertThat(new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible))
        .hashCode() == new ChronologyCompatibilityCheckResult(CAN_READ, LATEST,
            Collections.singletonList(resultCompatible)).hashCode(),
        is(false));
    assertThat(new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultCompatible))
        .hashCode() == new ChronologyCompatibilityCheckResult(CAN_BE_READ_BY, ALL,
            Collections.singletonList(resultCompatible)).hashCode(),
        is(false));
  }

  @Test
  public void testAccessors() {
    ChronologyCompatibilityCheckResult result = new ChronologyCompatibilityCheckResult(CAN_READ, ALL,
        Collections.singletonList(resultIncompatible));

    assertThat(result.getChronology(), is(ALL));
    assertThat(result.getCompatibility(), is(CAN_READ));
    assertThat(result.getResults(), is(Collections.singletonList(resultIncompatible)));
    assertThat(result.getType(), is(SchemaCompatibilityType.INCOMPATIBLE));
  }

  @Test
  public void testIsCompatible() {
    ChronologyCompatibilityCheckResult result = new ChronologyCompatibilityCheckResult(CAN_READ, ALL,
        Collections.singletonList(resultIncompatible));
    assertThat(result.isCompatible(), is(false));
  }

  @Test
  public void testAsMessage() {
    ChronologyCompatibilityCheckResult result = new ChronologyCompatibilityCheckResult(CAN_READ, ALL,
        Collections.singletonList(resultIncompatible));
    assertThat(result.asMessage(), is(
        "Compatibility type 'CAN_READ' does not hold between 1 schema(s) in the chronology because: Schema[0] has incompatibilities: ['FIXED_SIZE_MISMATCH: message' at '/']."));
  }

  @Test(expected = SchemaCompatibilityException.class)
  public void testThrowIfIncompatible() throws SchemaCompatibilityException {
    new ChronologyCompatibilityCheckResult(CAN_READ, ALL, Collections.singletonList(resultIncompatible))
        .throwIfIncompatible();
  }

}
