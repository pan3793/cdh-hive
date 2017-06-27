/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet.read;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import parquet.filter2.predicate.FilterPredicate;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class TestParquetFilterPredicate {
  @Test
  public void testFilterColumnsThatDoNoExistOnSchema() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 a; required binary stinger; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startNot()
            .startOr()
            .isNull("a")
            .between("y", 10L, 20L) // Column will be removed from filter
            .in("z", 1L, 2L, 3L) // Column will be removed from filter
            .nullSafeEquals("a", "stinger")
            .end()
            .end()
            .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected = "and(not(eq(a, null)), not(eq(a, Binary{\"stinger\"})))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterColumnsThatDoNoExistOnSchemaHighOrder1() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 a; required int32 b; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startOr()
        .startAnd()
        .equals("a", 1L)
        .equals("none", 1L)
        .end()
        .startAnd()
        .equals("a", 999L)
        .equals("none", 999L)
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected = "or(eq(a, 1), eq(a, 999))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterColumnsThatDoNoExistOnSchemaHighOrder2() {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 a; required int32 b; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .startOr()
        .equals("a", 1L)
        .equals("b", 1L)
        .end()
        .startOr()
        .equals("a", 999L)
        .equals("none", 999L)
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected = "or(eq(a, 1), eq(b, 1))";
    assertEquals(expected, p.toString());
  }

  @Test
  public void testFilterFloatColumns() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message test {  required float a; required int32 b; }");
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .startOr()
        .isNull("a")
        .between("a", 10.2, 20.3)
        .in("b", 1L, 2L, 3L)
        .end()
        .end()
        .build();

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema);

    String expected =
        "and(and(not(eq(a, null)), not(and(lteq(a, 20.3), not(lt(a, 10.2))))), not(or(or(eq(b, 1), eq(b, 2)), eq(b, 3))))";
    assertEquals(expected, p.toString());
  }

}
