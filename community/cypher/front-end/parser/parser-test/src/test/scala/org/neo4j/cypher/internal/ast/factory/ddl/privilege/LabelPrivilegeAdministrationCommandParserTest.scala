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

import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class LabelPrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val labelResource = LabelsResource(Seq("label"))(pos)

  Seq(
    ("GRANT", "TO", grantGraphPrivilege: resourcePrivilegeFunc),
    ("DENY", "TO", denyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyGraphPrivilege: resourcePrivilegeFunc),
    ("REVOKE", "FROM", revokeGraphPrivilege: resourcePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, func: resourcePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          Seq(
            ("SET", SetLabelAction),
            ("REMOVE", RemoveLabelAction)
          ).foreach {
            case (setOrRemove, action) =>
              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Multiple labels should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL * ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(pos),
                  AllLabelResource()(pos),
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL label1, label2 ON GRAPH foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(pos),
                  LabelsResource(Seq("label1", "label2"))(pos),
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Multiple graphs should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPHS * $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, AllGraphsScope()(pos))(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPHS foo,baz $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFooBaz)(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Home graph should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON HOME GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(pos))(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL * ON HOME GRAPH $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, HomeGraphScope()(pos))(pos),
                  AllLabelResource()(pos),
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              // Multiple roles should be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPHS foo $preposition role1, role2") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole1, literalRole2),
                  immutable
                )(pos))
              }

              // Parameter values

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH $$foo $preposition role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeParamFoo)(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(literalRole),
                  immutable
                )(pos))
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH foo $preposition $$role") {
                parsesTo[Statements](func(
                  GraphPrivilege(action, graphScopeFoo)(pos),
                  labelResource,
                  List(LabelAllQualifier()(pos)),
                  Seq(paramRole),
                  immutable
                )(pos))
              }

              // TODO: should this one be supported?
              test(s"$verb$immutableString $setOrRemove LABEL $$label ON GRAPH foo $preposition role") {
                failsParsing[Statements]
              }

              // LABELS instead of LABEL

              test(s"$verb$immutableString $setOrRemove LABELS label ON GRAPH * $preposition role") {
                failsParsing[Statements].withMessageStart("""Invalid input 'LABELS': expected""")
              }

              // Default graph should not be allowed

              test(s"$verb$immutableString $setOrRemove LABEL label ON DEFAULT GRAPH $preposition role") {
                failsParsing[Statements].in {
                  case Cypher5 =>
                    _.withOldSyntax("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
                  case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
                }
              }

              test(s"$verb$immutableString $setOrRemove LABEL * ON DEFAULT GRAPH $preposition role") {
                failsParsing[Statements].in {
                  case Cypher5 =>
                    _.withOldSyntax("`ON DEFAULT GRAPH` is not supported. Use `ON HOME GRAPH` instead.")
                  case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected ")
                }
              }

              // Database instead of graph keyword

              test(s"$verb$immutableString $setOrRemove LABEL label ON DATABASES * $preposition role") {
                failsParsing[Statements].withSyntaxErrorContaining(
                  """Invalid input 'DATABASES': expected"""
                )
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON DATABASE foo $preposition role") {
                failsParsing[Statements].withSyntaxErrorContaining(
                  """Invalid input 'DATABASE': expected"""
                )
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON HOME DATABASE $preposition role") {
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $setOrRemove LABEL label ON DEFAULT DATABASE $preposition role") {
                failsParsing[Statements]
              }

              // Alias with too many components
              test(s"$verb$immutableString $setOrRemove LABEL label ON GRAPH `a`.`b`.`c` $preposition role") {
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
}
