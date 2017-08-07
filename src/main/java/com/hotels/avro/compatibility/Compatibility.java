/**
 * Copyright (C) 2017 Expedia Inc.
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;

import patched.org.apache.avro.SchemaCompatibility;
import patched.org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import patched.org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

public final class Compatibility {

  public enum Mode {
    /** Also known as 'backwards'. Can read the data written by the most recent previous schema. */
    CAN_READ_LATEST(ChronologyType.LATEST, CheckType.CAN_READ),
    /** Also known as 'backwards transitive'. Can read the data written by all earlier schemas. */
    CAN_READ_ALL(ChronologyType.ALL, CheckType.CAN_READ),
    /** Also known as 'forwards'. The data written by this schema can be read by the most recent previous schema. */
    CAN_BE_READ_BY_LATEST(ChronologyType.LATEST, CheckType.CAN_BE_READ_BY),
    /** Also known as 'forwards transitive'. The data written by this schema can be read by all earlier schemas. */
    CAN_BE_READ_BY_ALL(ChronologyType.ALL, CheckType.CAN_BE_READ_BY),
    /** Also known as 'full'. Can read the data written by, a write data readable by the most recent previous schema. */
    MUTUAL_READ_WITH_LATEST(ChronologyType.LATEST, CheckType.MUTUAL_READ),
    /** Also known as 'full transitive'. Can read the data written by, a write data readable by all earlier schemas. */
    MUTUAL_READ_WITH_ALL(ChronologyType.ALL, CheckType.MUTUAL_READ);

    private final ChronologyType chronology;
    private final CheckType check;

    Mode(ChronologyType chronology, CheckType check) {
      this.chronology = chronology;
      this.check = check;
    }

    /**
     * @param schemasInOrder existing schemas, latest last.
     */
    public ChronologyCompatibilityCheckResult check(Schema toValidate, Iterable<Schema> schemasInOrder) {
      checkNotNull(toValidate, "Schema to be checked cannot be null.");
      checkNotNull(schemasInOrder, "Schemas to compare against cannot be null.");
      return new Checker(chronology, check, toValidate, schemasInOrder).check();
    }
  }

  private Compatibility() {}

  public static CheckSelector checkThat(Schema schema) {
    checkNotNull(schema, "Schema to be checked cannot be null.");
    return new CheckSelector(schema);
  }

  public static class CheckSelector {

    private final Schema toValidate;

    private CheckSelector(Schema toValidate) {
      this.toValidate = toValidate;
    }

    public CompatibilityCheckResult canRead(Schema schema) {
      checkNotNull(schema, "Schema to compare against cannot be null.");
      return new Checker(ChronologyType.LATEST, CheckType.CAN_READ, toValidate, Collections.singletonList(schema))
          .check()
          .getResults()
          .get(0);
    }

    public CompatibilityCheckResult canBeReadBy(Schema schema) {
      checkNotNull(schema, "Schema to compare against cannot be null.");
      return new Checker(ChronologyType.LATEST, CheckType.CAN_BE_READ_BY, toValidate, Collections.singletonList(schema))
          .check()
          .getResults()
          .get(0);
    }

    public CompatibilityCheckResult mutualReadWith(Schema schema) {
      checkNotNull(schema, "Schema to compare against cannot be null.");
      return new Checker(ChronologyType.LATEST, CheckType.MUTUAL_READ, toValidate, Collections.singletonList(schema))
          .check()
          .getResults()
          .get(0);
    }

    public ChronologySelector canRead() {
      return new ChronologySelector(toValidate, CheckType.CAN_READ);
    }

    public ChronologySelector canBeReadBy() {
      return new ChronologySelector(toValidate, CheckType.CAN_BE_READ_BY);
    }

    public ChronologySelector mutualReadWith() {
      return new ChronologySelector(toValidate, CheckType.MUTUAL_READ);
    }

    public static class ChronologySelector {
      private final Schema toValidate;
      private final CheckType compatibilityStrategy;

      private ChronologySelector(Schema toValidate, CheckType check) {
        this.toValidate = toValidate;
        this.compatibilityStrategy = check;
      }

      /**
       * @param schemasInOrder existing schemas, latest last.
       */
      public ChronologyCompatibilityCheckResult all(Iterable<Schema> schemasInOrder) {
        checkNotNull(schemasInOrder, "Schemas to compare against cannot be null.");
        return new Checker(ChronologyType.ALL, compatibilityStrategy, toValidate, schemasInOrder).check();
      }

      /**
       * @param schemasInOrder existing schemas, latest last.
       */
      public ChronologyCompatibilityCheckResult all(Schema... schemasInOrder) {
        checkNotNull(schemasInOrder, "Schemas to compare against cannot be null.");
        return all(Arrays.asList(schemasInOrder));
      }

      /**
       * @param schemasInOrder existing schemas, latest last.
       */
      public ChronologyCompatibilityCheckResult latestOf(Iterable<Schema> schemasInOrder) {
        checkNotNull(schemasInOrder, "Schemas to compare against cannot be null.");
        return new Checker(ChronologyType.LATEST, compatibilityStrategy, toValidate, schemasInOrder).check();
      }

      /**
       * @param schemasInOrder existing schemas, latest last.
       */
      public ChronologyCompatibilityCheckResult latestOf(Schema... schemasInOrder) {
        checkNotNull(schemasInOrder, "Schemas to compare against cannot be null.");
        return latestOf(Arrays.asList(schemasInOrder));
      }
    }

  }

  private static class Checker {
    private final ChronologyType chronology;
    private final CheckType check;
    private final Schema toValidate;
    private final Iterable<Schema> schemasInOrder;

    private Checker(ChronologyType chronology, CheckType check, Schema toValidate, Iterable<Schema> schemasInOrder) {
      this.chronology = chronology;
      this.check = check;
      this.toValidate = toValidate;
      this.schemasInOrder = schemasInOrder;
    }

    ChronologyCompatibilityCheckResult check() {
      return chronology.check(check, toValidate, schemasInOrder);
    }
  }

  public enum ChronologyType {
    ALL() {
      @Override
      public ChronologyCompatibilityCheckResult check(
          CheckType check,
          Schema toValidate,
          Iterable<Schema> schemasInOrder) {
        List<CompatibilityCheckResult> results = new ArrayList<>();
        Iterator<Schema> schemas = schemasInOrder.iterator();
        while (schemas.hasNext()) {
          Schema existing = schemas.next();
          SchemaPairCompatibility schemaPairCompatibility = check.validate(toValidate, existing);
          CompatibilityCheckResult result = new CompatibilityCheckResult(check, this, schemaPairCompatibility);
          results.add(result);
        }
        return new ChronologyCompatibilityCheckResult(check, this, results);
      }
    },
    LATEST() {
      @Override
      public ChronologyCompatibilityCheckResult check(
          CheckType check,
          Schema toValidate,
          Iterable<Schema> schemasInOrder) {
        Iterator<Schema> schemas = schemasInOrder.iterator();
        Schema existing = null;
        while (schemas.hasNext()) {
          existing = schemas.next();
        }
        if (existing != null) {
          SchemaPairCompatibility schemaPairCompatibility = check.validate(toValidate, existing);
          CompatibilityCheckResult result = new CompatibilityCheckResult(check, this, schemaPairCompatibility);
          return new ChronologyCompatibilityCheckResult(check, this, Collections.singletonList(result));
        }
        return new ChronologyCompatibilityCheckResult(check, this, Collections.<CompatibilityCheckResult> emptyList());
      }
    };

    /**
     * @param schemasInOrder existing schemas, latest last.
     */
    abstract ChronologyCompatibilityCheckResult check(
        CheckType check,
        Schema toValidate,
        Iterable<Schema> schemasInOrder);
  }

  public enum CheckType {
    CAN_READ() {
      @Override
      public SchemaPairCompatibility validate(Schema toValidate, Schema existing) {
        return canRead(existing, toValidate);
      }
    },
    CAN_BE_READ_BY() {
      @Override
      public SchemaPairCompatibility validate(Schema toValidate, Schema existing) {
        return canRead(toValidate, existing);
      }
    },
    MUTUAL_READ() {
      @Override
      public SchemaPairCompatibility validate(Schema toValidate, Schema existing) {
        SchemaPairCompatibility canBeRead = canRead(toValidate, existing);
        if (canBeRead.getType() == SchemaCompatibilityType.INCOMPATIBLE) {
          return canBeRead;
        }
        SchemaPairCompatibility canRead = canRead(existing, toValidate);
        return canRead;
      }
    };

    abstract SchemaPairCompatibility validate(Schema toValidate, Schema existing);
  }

  private static SchemaPairCompatibility canRead(Schema writtenWith, Schema readUsing) {
    return SchemaCompatibility.checkReaderWriterCompatibility(readUsing, writtenWith);
  }

}
