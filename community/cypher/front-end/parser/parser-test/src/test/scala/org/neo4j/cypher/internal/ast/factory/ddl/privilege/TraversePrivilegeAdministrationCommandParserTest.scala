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
package org.neo4j.cypher.internal.ast.factory.ddl.privilege

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class TraversePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: noResourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: noResourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: noResourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: noResourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(TraverseAction, HomeGraphScope()(pos))(pos),
              List(ElementsAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH NODE A $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(TraverseAction, HomeGraphScope()(pos))(pos),
              List(labelQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH RELATIONSHIP * $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(TraverseAction, HomeGraphScope()(pos))(pos),
              List(ast.RelationshipAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRAVERSE ON HOME GRAPH ELEMENT A $preposition role") {
            parsesTo[Statements](func(
              GraphPrivilege(TraverseAction, HomeGraphScope()(pos))(pos),
              List(elemQualifierA),
              Seq(literalRole),
              immutable
            )(pos))
          }

          Seq("GRAPH", "GRAPHS").foreach {
            graphKeyword =>
              test(s"$verb$immutableString TRAVERSE ON $graphKeyword * $preposition $$role") {
                parsesTo[Statements](func(
                  GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                  List(ElementsAllQualifier()(pos)),
                  Seq(paramRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString TRAVERSE ON $graphKeyword foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                  List(ElementsAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString TRAVERSE ON $graphKeyword $$foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(TraverseAction, graphScopeParamFoo)(pos),
                  List(ElementsAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              Seq("NODE", "NODES").foreach {
                nodeKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $nodeKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                          List(ast.LabelAllQualifier()(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                              List(ast.LabelAllQualifier()(pos)),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword A (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                              List(labelQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.NamedGraphsScope(Seq(literal("*")))(pos))(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(ast.LabelAllQualifier()(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword * (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                              List(ast.LabelAllQualifier()(pos)),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                              List(labelQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition role1, $$role2" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRole1, paramRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.NamedGraphsScope(Seq(literal("2foo")))(pos))(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A $preposition `r:ole`" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA),
                          Seq(literalRColonOle),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword `A B` $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(ast.LabelQualifier("A B")(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A, B $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA, labelQualifierB),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A, B $preposition role1, role2" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(labelQualifierA, labelQualifierB),
                          Seq(literalRole1, literalRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $nodeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFooBaz)(pos),
                          List(labelQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(s"traverseParsingErrors $verb$immutableString $graphKeyword $nodeKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE $graphKeyword * $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A B (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (foo) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword * $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword A $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword * (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $nodeKeyword A (*) $preposition r:ole" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $nodeKeyword A (*) $preposition role" should
                      notParse[Statements]
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $nodeKeyword * (*)" should notParse[Statements]
                  }
              }

              Seq("RELATIONSHIP", "RELATIONSHIPS").foreach {
                relTypeKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $relTypeKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                          List(ast.RelationshipAllQualifier()(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                              List(ast.RelationshipAllQualifier()(pos)),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword A (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                              List(relQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }

                    s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.NamedGraphsScope(Seq(literal("*")))(pos))(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(ast.RelationshipAllQualifier()(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword * (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                              List(ast.RelationshipAllQualifier()(pos)),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                              List(relQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition $$role1, role2" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(paramRole1, literalRole2),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.NamedGraphsScope(Seq(literal("2foo")))(pos))(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A $preposition `r:ole`" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA),
                          Seq(literalRColonOle),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword `A B` $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(ast.RelationshipQualifier("A B")(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA, relQualifierB),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A, B $preposition role1, role2" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(relQualifierA, relQualifierB),
                          Seq(literalRole1, literalRole2),
                          immutable
                        )(pos)
                      )

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $relTypeKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFooBaz)(pos),
                          List(relQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(s"traverseParsingErrors$verb$immutableString $graphKeyword $relTypeKeyword $preposition") {

                    s"$verb$immutableString TRAVERSE $graphKeyword * $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A B (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (foo) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $relTypeKeyword A (*) $preposition r:ole" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $relTypeKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $relTypeKeyword * (*)" should
                      notParse[Statements]
                  }
              }

              Seq("ELEMENT", "ELEMENTS").foreach {
                elementKeyword =>
                  test(s"validExpressions $verb$immutableString $graphKeyword $elementKeyword $preposition") {
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                          List(ElementsAllQualifier()(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                              List(ElementsAllQualifier()(pos)),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword A (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, ast.AllGraphsScope()(pos))(pos),
                              List(elemQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `*` $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.NamedGraphsScope(Seq(literal("*")))(pos))(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword * $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(ElementsAllQualifier()(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword * (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                              List(ElementsAllQualifier()(pos)),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition role" should
                      parseIn[Statements] {
                        case Cypher5 => _.toAst(Statements(Seq(
                            func(
                              GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                              List(elemQualifierA),
                              Seq(literalRole),
                              immutable
                            )(pos)
                          )))
                        case _ => _.throws[SyntaxException].withMessageContaining("Invalid input")
                      }
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition role1, role2" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRole1, literalRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword `2foo` $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, ast.NamedGraphsScope(Seq(literal("2foo")))(pos))(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A $preposition `r:ole`" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA),
                          Seq(literalRColonOle),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword `A B` $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(ast.ElementQualifier("A B")(pos)),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A, B $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA, elemQualifierB),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A, B $preposition $$role1, $$role2" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFoo)(pos),
                          List(elemQualifierA, elemQualifierB),
                          Seq(paramRole1, paramRole2),
                          immutable
                        )(pos)
                      )
                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo, baz $elementKeyword A $preposition role" should
                      parseTo[Statements](
                        func(
                          GraphPrivilege(TraverseAction, graphScopeFooBaz)(pos),
                          List(elemQualifierA),
                          Seq(literalRole),
                          immutable
                        )(pos)
                      )
                  }

                  test(s"traverseParsingErrors $verb$immutableString $graphKeyword $elementKeyword $preposition") {

                    s"$verb$immutableString TRAVERSE $graphKeyword * $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A B (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (foo) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword * $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword A $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword * (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword foo $elementKeyword A (*) $preposition r:ole" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword 2foo $elementKeyword A (*) $preposition role" should
                      notParse[Statements]

                    s"$verb$immutableString TRAVERSE ON $graphKeyword * $elementKeyword * (*)" should
                      notParse[Statements]
                  }
              }
          }

          // Default graph should not be allowed

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 => _.withOldSyntax("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH NODE A $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 => _.withOldSyntax("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH RELATIONSHIP * $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 => _.withOldSyntax("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT GRAPH ELEMENT A $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 => _.withOldSyntax("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
              case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
            }
          }

          // Mix of specific graph and *

          test(s"$verb$immutableString TRAVERSE ON GRAPH foo, * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRAVERSE ON GRAPH *, foo $preposition role") {
            failsParsing[Statements]
          }

          // Database instead of graph keyword

          test(s"$verb$immutableString TRAVERSE ON DATABASES * $preposition role") {
            val offset = verb.length + immutableString.length + 13
            failsParsing[Statements].in {
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASES': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASES': expected 'GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString TRAVERSE ON DATABASE foo $preposition role") {
            val offset = verb.length + immutableString.length + 13
            failsParsing[Statements].in {
              case Cypher5 => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected 'GRAPH', 'DEFAULT GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
              case _ => _.withSyntaxErrorContaining(
                  s"""Invalid input 'DATABASE': expected 'GRAPH', 'HOME GRAPH' or 'GRAPHS' (line 1, column ${offset + 1} (offset: $offset))"""
                )
            }
          }

          test(s"$verb$immutableString TRAVERSE ON HOME DATABASE $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRAVERSE ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements]
          }

          // Alias with too many components

          test(s"$verb$immutableString TRAVERSE ON GRAPH `a`.`b`.`c` $preposition role") {
            // more than two components
            failsParsing[Statements]
              .in {
                case Cypher5 => _.withMessageStart(
                    "Invalid input ``a`.`b`.`c`` for name. Expected name to contain at most two components separated by `.`."
                  )
                    .withSyntaxErrorGqlStatus(
                      gqlStatus(
                        GqlStatusInfoCodes.STATUS_22N05,
                        "error: data exception - input failed validation. Invalid input '`a`.`b`.`c`' for name."
                      )
                        .withCause(
                          GqlStatusInfoCodes.STATUS_22N83,
                          "error: data exception - input consists of too many components. Expected name to contain at most 2 components separated by '.'."
                        )
                    )
                case _ => _.withMessageStart(
                    "Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
                  )
                    .withSyntaxErrorGqlStatus(
                      gqlStatus(
                        GqlStatusInfoCodes.STATUS_42NAA,
                        "error: syntax error or access rule violation - incorrectly formatted graph reference. Incorrectly formatted graph reference '`a`.`b`.`c`'. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually."
                      )
                    )
              }
          }
      }
  }
}
