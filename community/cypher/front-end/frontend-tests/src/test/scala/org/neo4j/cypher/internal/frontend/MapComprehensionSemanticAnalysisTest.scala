/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.CypherVersion

class MapComprehensionSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  test("RETURN { k IN [1, 2, 3] | toString(k) : k * 10 }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { a in [{foo: 123}, {foo: 456}] WHERE a.foo > 200 | a.name : a.foo }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("WITH { a: 1, b: 2, c: 3} AS map RETURN { k IN keys(map) | 'a' : map[k] * 10 }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k IN ['a', 'b'] | k : { innerK IN [1, 2] | toString(innerK) : innerK * 10 } }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { i IN null | 'a' : i }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { i IN [1, 2] WHERE null | 'a': i }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { i IN [1, 2] WHERE 1 | 'a': i }") {
    run(Set(CypherVersion.Cypher5)).hasErrorMessages(
      "Type mismatch: expected Boolean but was Integer"
    )
  }

  test("RETURN { i IN [1, 2] | null : i }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { i IN [1, 2] | i : i }") {
    run(Set(CypherVersion.Cypher5)).hasErrorMessages(
      "Type mismatch: expected String but was Integer"
    )
  }

  test("WITH { i IN [1, 2, 3] | toString(i) : i } AS map RETURN map") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("WITH 'a' AS i RETURN { i IN [1, 2] | toString(i) : i }, i") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  // Map entries comprehension

  test("RETURN { k: v IN {a: 1, b: 2, c: 3} | k : v * 10 }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN {a: 1, b: 2, c: 3} WHERE v > 1 | k : v }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("WITH {a: 1, b: 2, c: 3} AS map RETURN { k: v IN map | toString(v) : v * 10 }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN {a: {aa: 1}, b: {ba: 1}} | k : { innerK: innerV IN v | innerK : innerV * 10 } }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN null | 'a' : v }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN {a: 1} WHERE null | 'a': v }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN {a: 1} WHERE 1 | 'a': v }") {
    run(Set(CypherVersion.Cypher5)).hasErrorMessages(
      "Type mismatch: expected Boolean but was Integer"
    )
  }

  test("RETURN { k: v IN {a: 1} | null : v }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN {a: 1} | v : v }") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN { k: v IN [1, 2, 3] | k : v }") {
    run(Set(CypherVersion.Cypher5)).hasErrorMessages(
      "Type mismatch: expected Map but was List<Integer>"
    )
  }

  test("MATCH (n) RETURN { k: v IN n | k : v }") {
    run(Set(CypherVersion.Cypher5)).hasErrorMessages(
      "Type mismatch: expected Map but was Node"
    )
  }

  test("MATCH ()-[r]->() RETURN { k: v IN r | k : v }") {
    run(Set(CypherVersion.Cypher5)).hasErrorMessages(
      "Type mismatch: expected Map but was Relationship"
    )
  }

  test("WITH { k: v IN {a: 1, b: 2, c: 3} | toString(k) : v } AS map RETURN map") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("WITH 'a' AS k, 'b' AS v RETURN { k: v IN {a: 1} | toString(k) : v }, k, v") {
    run(Set(CypherVersion.Cypher5)).hasNoErrors
  }
}
