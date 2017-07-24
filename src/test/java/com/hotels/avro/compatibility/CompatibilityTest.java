/**
 * Copyright 2014-2017 Confluent Inc. and Expedia Inc.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.junit.Test;

/** Based on code from Confluent schema-registry project. */
public class CompatibilityTest {

  private final String schemaString1 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"}]}";
  private final Schema schema1 = parseSchema(schemaString1);

  private final String schemaString2 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]}";
  private final Schema schema2 = parseSchema(schemaString2);

  private final String schemaString3 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\"}]}";
  private final Schema schema3 = parseSchema(schemaString3);

  // private final String schemaString4 = "{\"type\":\"record\","
  // + "\"name\":\"myrecord\","
  // + "\"fields\":"
  // + "[{\"type\":\"string\",\"name\":\"f1_new\", \"aliases\": [\"f1\"]}]}";
  // private final Schema schema4 = parseSchema(schemaString4);

  private final String schemaString6 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":[\"null\", \"string\"],\"name\":\"f1\","
      + " \"doc\":\"doc of f1\"}]}";
  private final Schema schema6 = parseSchema(schemaString6);

  private final String schemaString7 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":[\"null\", \"string\", \"int\"],\"name\":\"f1\","
      + " \"doc\":\"doc of f1\"}]}";
  private final Schema schema7 = parseSchema(schemaString7);

  private final String schemaString8 = "{\"type\":\"record\","
      + "\"name\":\"myrecord\","
      + "\"fields\":"
      + "[{\"type\":\"string\",\"name\":\"f1\"},"
      + " {\"type\":\"string\",\"name\":\"f2\", \"default\": \"foo\"}]},"
      + " {\"type\":\"string\",\"name\":\"f3\", \"default\": \"bar\"}]}";
  private final Schema schema8 = parseSchema(schemaString8);

  private static Schema parseSchema(String schemaString) {
    try {
      Schema.Parser parser1 = new Schema.Parser();
      Schema schema = parser1.parse(schemaString);
      return schema;
    } catch (SchemaParseException e) {
      return null;
    }
  }

  /*
   * Backward compatibility: A new schema is backward compatible if it can be used to read the data written in the
   * previous schema.
   */
  @Test
  public void testBasicBackwardsCompatibility() {
    assertTrue("adding a field with default is a backward compatible change",
        Compatibility.checkThat(schema2).canRead(schema1).isCompatible());
    assertFalse("adding a field w/o default is not a backward compatible change",
        Compatibility.checkThat(schema3).canRead(schema1).isCompatible());

    // This assertion is a hang over from the earlier use of Avro's
    // SchemaValidatorBuilder. This did not consider aliased field names as
    // compatible. We now use Avro's SchemaCompatibility class which does
    // permit aliases. I'm leaving this assertion here, commented to document
    // that we've modified the earlier behaviour of our system.
    //
    // assertFalse("changing field name is not a backward compatible change",
    // Compatibility.checkThat(schema4).canRead(schema1).isCompatible());
    assertTrue("evolving a field type to a union is a backward compatible change",
        Compatibility.checkThat(schema6).canRead(schema1).isCompatible());
    assertFalse("removing a type from a union is not a backward compatible change",
        Compatibility.checkThat(schema1).canRead(schema6).isCompatible());
    assertTrue("adding a new type in union is a backward compatible change",
        Compatibility.checkThat(schema7).canRead(schema6).isCompatible());
    assertFalse("removing a type from a union is not a backward compatible change",
        Compatibility.checkThat(schema6).canRead(schema7).isCompatible());

    // Only schema 2 is checked
    assertTrue("removing a default is not a transitively compatible change",
        Compatibility.checkThat(schema3).canRead().latestOf(schema1, schema2).isCompatible());
  }

  /*
   * Backward transitive compatibility: A new schema is backward compatible if it can be used to read the data written
   * in all previous schemas.
   */
  @Test
  public void backwardsTransitive() {
    assertTrue("iteratively adding fields with defaults is a compatible change",
        Compatibility.checkThat(schema8).canRead().all(schema1, schema2).isCompatible());

    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding a field with default is a backward compatible change",
        Compatibility.checkThat(schema2).canRead(schema1).isCompatible());
    assertTrue("removing a default is a compatible change, but not transitively",
        Compatibility.checkThat(schema3).canRead(schema2).isCompatible());
    assertFalse("removing a default is not a transitively compatible change",
        Compatibility.checkThat(schema3).canRead().all(schema2, schema1).isCompatible());
  }

  /*
   * Forward compatibility: A new schema is forward compatible if the previous schema can read data written in this
   * schema.
   */
  @Test
  public void testBasicForwardsCompatibility() {
    assertTrue("adding a field is a forward compatible change",
        Compatibility.checkThat(schema2).canBeReadBy(schema1).isCompatible());
    assertTrue("adding a field is a forward compatible change",
        Compatibility.checkThat(schema3).canBeReadBy(schema1).isCompatible());
    assertTrue("adding a field is a forward compatible change",
        Compatibility.checkThat(schema3).canBeReadBy(schema2).isCompatible());
    assertTrue("adding a field is a forward compatible change",
        Compatibility.checkThat(schema2).canBeReadBy(schema3).isCompatible());

    // Only schema 2 is checked
    assertTrue("removing a default is not a transitively compatible change",
        Compatibility.checkThat(schema1).canBeReadBy().latestOf(schema3, schema2).isCompatible());
  }

  /*
   * Forward transitive compatibility: A new schema is forward compatible if all previous schemas can read data written
   * in this schema.
   */
  @Test
  public void testBasicForwardsTransitiveCompatibility() {
    // All compatible
    assertTrue("iteratively removing fields with defaults is a compatible change",
        Compatibility.checkThat(schema1).canBeReadBy().all(schema8, schema2).isCompatible());

    // 1 == 2, 2 == 3, 3 != 1
    assertTrue("adding default to a field is a compatible change",
        Compatibility.checkThat(schema2).canBeReadBy(schema3).isCompatible());
    assertTrue("removing a field with a default is a compatible change",
        Compatibility.checkThat(schema1).canBeReadBy().all(schema2).isCompatible());
    assertFalse("removing a default is not a transitively compatible change",
        Compatibility.checkThat(schema1).canBeReadBy().all(schema2, schema3).isCompatible());
  }

  /*
   * Full compatibility: A new schema is fully compatible if it’s both backward and forward compatible.
   */
  @Test
  public void testBasicFullCompatibility() {
    assertTrue("adding a field with default is a backward and a forward compatible change",
        Compatibility.checkThat(schema2).mutualReadWith(schema1).isCompatible());

    // Only schema 2 is checked!
    assertTrue("transitively adding a field without a default is not a compatible change",
        Compatibility.checkThat(schema3).mutualReadWith().latestOf(schema1, schema2).isCompatible());
    // Only schema 2 is checked!
    assertTrue("transitively removing a field without a default is not a compatible change",
        Compatibility.checkThat(schema1).mutualReadWith().latestOf(schema3, schema2).isCompatible());
  }

  /*
   * Full transitive compatibility: A new schema is fully compatible if it’s both transitively backward and transitively
   * forward compatible with the entire schema history.
   */
  @Test
  public void testBasicFullTransitiveCompatibility() {
    // Simple check
    assertTrue("iteratively adding fields with defaults is a compatible change",
        Compatibility.checkThat(schema8).mutualReadWith().all(schema1, schema2).isCompatible());
    assertTrue("iteratively removing fields with defaults is a compatible change",
        Compatibility.checkThat(schema1).mutualReadWith().all(schema8, schema2).isCompatible());

    assertTrue("adding default to a field is a compatible change",
        Compatibility.checkThat(schema2).mutualReadWith(schema3).isCompatible());
    assertTrue("removing a field with a default is a compatible change",
        Compatibility.checkThat(schema1).mutualReadWith().all(schema2).isCompatible());

    assertTrue("adding a field with default is a compatible change",
        Compatibility.checkThat(schema2).mutualReadWith(schema1).isCompatible());
    assertTrue("removing a default from a field compatible change",
        Compatibility.checkThat(schema3).mutualReadWith().all(schema2).isCompatible());

    assertFalse("transitively adding a field without a default is not a compatible change",
        Compatibility.checkThat(schema3).mutualReadWith().all(schema2, schema1).isCompatible());
    assertFalse("transitively removing a field without a default is not a compatible change",
        Compatibility.checkThat(schema1).mutualReadWith().all(schema2, schema3).isCompatible());
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedCannotBeNull() {
    Compatibility.checkThat(null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemaReadCannotBeNull() {
    Compatibility.checkThat(schema1).canRead(null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemaReadByCannotBeNull() {
    Compatibility.checkThat(schema1).canBeReadBy(null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemaMutualReadWithCannotBeNull() {
    Compatibility.checkThat(schema1).mutualReadWith(null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadAllCannotBeNull() {
    Compatibility.checkThat(schema1).canRead().all((Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadByAllCannotBeNull() {
    Compatibility.checkThat(schema1).canBeReadBy().all((Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasMutualReadWithAllCannotBeNull() {
    Compatibility.checkThat(schema1).mutualReadWith().all((Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadAllVargsCannotBeNull() {
    Compatibility.checkThat(schema1).canRead().all((Schema[]) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadByAllVargsCannotBeNull() {
    Compatibility.checkThat(schema1).canBeReadBy().all((Schema[]) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasMutualReadWithAllVargsCannotBeNull() {
    Compatibility.checkThat(schema1).mutualReadWith().all((Schema[]) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadLatestCannotBeNull() {
    Compatibility.checkThat(schema1).canRead().latestOf((Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadByLatestCannotBeNull() {
    Compatibility.checkThat(schema1).canBeReadBy().latestOf((Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasMutualReadWithLatestCannotBeNull() {
    Compatibility.checkThat(schema1).mutualReadWith().latestOf((Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadLatestVargsCannotBeNull() {
    Compatibility.checkThat(schema1).canRead().latestOf((Schema[]) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasReadByLatestVargsCannotBeNull() {
    Compatibility.checkThat(schema1).canBeReadBy().latestOf((Schema[]) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasMutualReadWithLatestVargsCannotBeNull() {
    Compatibility.checkThat(schema1).mutualReadWith().latestOf((Schema[]) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasModeMutualReadWithAllCheckerCannotBeNull() {
    Compatibility.Mode.MUTUAL_READ_WITH_ALL.check(schema1, (Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasModeReadByAllCheckerCannotBeNull() {
    Compatibility.Mode.CAN_BE_READ_BY_ALL.check(schema1, (Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasModeReadAllCheckerCannotBeNull() {
    Compatibility.Mode.CAN_READ_ALL.check(schema1, (Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedModeMutualReadWithAllCheckerCannotBeNull() {
    Compatibility.Mode.MUTUAL_READ_WITH_ALL.check(null, Collections.singletonList(schema1));
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedModeReadByAllCheckerCannotBeNull() {
    Compatibility.Mode.CAN_BE_READ_BY_ALL.check(null, Collections.singletonList(schema1));
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedModeReadAllCheckerCannotBeNull() {
    Compatibility.Mode.CAN_READ_ALL.check(null, Collections.singletonList(schema1));
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasModeMutualReadWithLatestCheckerCannotBeNull() {
    Compatibility.Mode.MUTUAL_READ_WITH_LATEST.check(schema1, (Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasModeReadByLatestCheckerCannotBeNull() {
    Compatibility.Mode.CAN_BE_READ_BY_LATEST.check(schema1, (Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testExistingSchemasModeReadLatestCheckerCannotBeNull() {
    Compatibility.Mode.CAN_READ_LATEST.check(schema1, (Iterable<Schema>) null);
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedModeMutualReadWithLatestCheckerCannotBeNull() {
    Compatibility.Mode.MUTUAL_READ_WITH_LATEST.check(null, Collections.singletonList(schema1));
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedModeReadByLatestCheckerCannotBeNull() {
    Compatibility.Mode.CAN_BE_READ_BY_LATEST.check(null, Collections.singletonList(schema1));
  }

  @Test(expected = NullPointerException.class)
  public void testSchemaToBeCheckedModeReadLatestCheckerCannotBeNull() {
    Compatibility.Mode.CAN_READ_LATEST.check(null, Collections.singletonList(schema1));
  }

}
