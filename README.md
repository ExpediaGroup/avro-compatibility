# Avro compatibility
A user friendly API for checking for and reporting on [Avro](https://avro.apache.org) schema incompatibilities.

## Start using
You can obtain `avro-compatibility` from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/avro-compatibility/badge.svg?subject=com.hotels:avro-compatibility)](https://maven-badges.herokuapp.com/maven-central/com.hotels/avro-compatibility) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/avro-compatibility.svg)

## Overview
Although Avro is excellent at determining the compatibility of schemas, it is not very good at reporting the nature and the location of any schema incompatibilities it finds. Rather than supplement or modify any existing Avro APIs (which are already overloaded), `avro-compatibility` introduces an entirely distinct API that leverages these new features.

## Features
#### Incompatibility type detection
Detects specific incompatibility conditions (via [AVRO-1933](https://issues.apache.org/jira/browse/AVRO-1933)):
* Type name mismatch.
* Fixed type size mismatch.
* Missing enum symbols.
* Missing field default values.
* Type mismatches.
* Missing union type branches.

#### Incompatibility location detection
Describes the location in the schema where incompatibilities occur using [JSON Pointer](https://tools.ietf.org/html/rfc6901) (via [AVRO-2003](https://issues.apache.org/jira/browse/AVRO-2003)). Examples (source schema not shown):

* field type incompatibility: `/fields/0/type`
* missing union branch: `/fields/1/type/2`
* missing enum symbol: `/fields/2/type/symbols`
* array element type incompatibility: `/fields/3/type/items`
* fixed size type incompatibility: `/fields/5/type/size` 
* missing default from field: `/fields/3`

### Descriptive messages
Provides user friendly incompatibility messages such as:

    Compatibility type 'CAN_READ' does not hold between schemas, incompatibilities: ['MISSING_UNION_BRANCH: reader union lacking writer type: INT' at '/fields/0/type/2'].
    Compatibility type 'CAN_READ' does not hold between one or more schemas because: Schema[1] has incompatibilities: ['READER_FIELD_MISSING_DEFAULT_VALUE: f2' at '/fields/1]'.

    
## Usage
### Checking compatibility
With a fluent API:

    // 'Backwards'
    Compatibility.checkThat(schema2).canRead(schema1);
    // 'Backwards, latest of chronology'
    Compatibility.checkThat(schema1).canRead().latestOf(schema3, schema2);
    // 'Forwards, transitive'
    Compatibility.checkThat(schema1).canBeReadBy().all(schema2, schema3);

Alternatively use predefined compatibility checkers:

    Compatibility.Mode.CAN_READ_LATEST
      .check(schema1, Collections.singletonList(schema3, schema2));
    Compatibility.Mode.MUTUAL_READ_WITH_ALL
      .check(schema1, Arrays.asList(schema3, schema2));


### Compatibility check results
Interrogate the results programmatically with `CompatibilityCheckResult` and `ChronologyCompatibilityCheckResult` or simply throw an exception or get a message:

    Compatibility.checkThat(schema2).canRead(schema1).throwIfIncompatible();
    System.out.println(
      Compatibility.checkThat(schema2).canRead(schema1).asMessage()
    );

## Notes
* The compatibility/evolution rule implementation used by the library supports `aliases`; the implementation accessed via `org.apache.avro.SchemaValidatorBuilder` does not. Exercise care if migrating from one to the other. Note that this isn't something that we've introduced, Avro just happens to contain two implementations of said rules that unfortunately have subtle differences in behaviour. 

## Prior art
This project is based on the [`SchemaCompatibility`](https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/SchemaCompatibility.java) class from the [Avro project](https://avro.apache.org). Tests are based on the [`AvroCompatibilityTest`](https://github.com/confluentinc/schema-registry/blob/master/core/src/test/java/io/confluent/kafka/schemaregistry/avro/AvroCompatibilityTest.java) suite from the [Confluent/schema-registry project](https://github.com/confluentinc/schema-registry).

## Credits
* Created by [Elliot West](https://github.com/teabot).
* Thanks to [@epkanol](https://github.com/epkanol) for the foundational work in [AVRO-1933](https://issues.apache.org/jira/browse/AVRO-1933).
* Thanks to [@chids](https://github.com/chids) for insightful feature suggestions. 
* Further thanks to [Adrian Woodhead](https://github.com/massdosage) and [Dave Maughan](https://github.com/nahguam).

## Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html). See `NOTICE` for further information.

Copyright 2017 Expedia Inc.
