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

import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.AllConstraintActions
import org.neo4j.cypher.internal.ast.AllDatabaseAction
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AllTokenActions
import org.neo4j.cypher.internal.ast.AllTransactionActions
import org.neo4j.cypher.internal.ast.AlterCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetDatabaseDefaultLanguageAction
import org.neo4j.cypher.internal.ast.ShowConstraintAction
import org.neo4j.cypher.internal.ast.ShowIndexAction
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.factory.ddl.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier.maybeImmutable
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher25
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsing.ParserInTest
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class DatabasePrivilegeAdministrationCommandParserTest extends AdministrationAndSchemaCommandParserTestBase {
  private val databaseScopeFoo = NamedDatabasesScope(Seq(literalFoo))(pos)
  private val databaseScopeParamFoo = NamedDatabasesScope(Seq(namespacedParamFoo))(pos)
  private val databaseScopeFooBar = NamedDatabasesScope(Seq(literalFoo, namespacedName("bar")))(pos)
  private val databaseScopeFooParamBar = NamedDatabasesScope(Seq(literalFoo, stringParamName("bar")))(pos)

  override protected def ignorePrettifier: Boolean = true

  case class PrivilegeAction(
    privilege: String,
    action: DatabaseAction,
    minCypherVersion: ParserInTest = Cypher5
  )

  Seq(
    ("GRANT", "TO", grantDatabasePrivilege: databasePrivilegeFunc),
    ("DENY", "TO", denyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE", "FROM", revokeDatabasePrivilege: databasePrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, privilegeFunc: databasePrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          val privilegesInAllVersions = Seq(
            PrivilegeAction("ACCESS", AccessDatabaseAction),
            PrivilegeAction("START", StartDatabaseAction),
            PrivilegeAction("STOP", StopDatabaseAction),
            PrivilegeAction("CREATE INDEX", CreateIndexAction),
            PrivilegeAction("CREATE INDEXES", CreateIndexAction),
            PrivilegeAction("DROP INDEX", DropIndexAction),
            PrivilegeAction("DROP INDEXES", DropIndexAction),
            PrivilegeAction("SHOW INDEX", ShowIndexAction),
            PrivilegeAction("SHOW INDEXES", ShowIndexAction),
            PrivilegeAction("INDEX", AllIndexActions),
            PrivilegeAction("INDEXES", AllIndexActions),
            PrivilegeAction("INDEX MANAGEMENT", AllIndexActions),
            PrivilegeAction("INDEXES MANAGEMENT", AllIndexActions),
            PrivilegeAction("CREATE CONSTRAINT", CreateConstraintAction),
            PrivilegeAction("CREATE CONSTRAINTS", CreateConstraintAction),
            PrivilegeAction("DROP CONSTRAINT", DropConstraintAction),
            PrivilegeAction("DROP CONSTRAINTS", DropConstraintAction),
            PrivilegeAction("SHOW CONSTRAINT", ShowConstraintAction),
            PrivilegeAction("SHOW CONSTRAINTS", ShowConstraintAction),
            PrivilegeAction("CONSTRAINT", AllConstraintActions),
            PrivilegeAction("CONSTRAINTS", AllConstraintActions),
            PrivilegeAction("CONSTRAINT MANAGEMENT", AllConstraintActions),
            PrivilegeAction("CONSTRAINTS MANAGEMENT", AllConstraintActions),
            PrivilegeAction("CREATE NEW LABEL", CreateNodeLabelAction),
            PrivilegeAction("CREATE NEW LABELS", CreateNodeLabelAction),
            PrivilegeAction("CREATE NEW NODE LABEL", CreateNodeLabelAction),
            PrivilegeAction("CREATE NEW NODE LABELS", CreateNodeLabelAction),
            PrivilegeAction("CREATE NEW TYPE", CreateRelationshipTypeAction),
            PrivilegeAction("CREATE NEW TYPES", CreateRelationshipTypeAction),
            PrivilegeAction("CREATE NEW RELATIONSHIP TYPE", CreateRelationshipTypeAction),
            PrivilegeAction("CREATE NEW RELATIONSHIP TYPES", CreateRelationshipTypeAction),
            PrivilegeAction("CREATE NEW NAME", CreatePropertyKeyAction),
            PrivilegeAction("CREATE NEW NAMES", CreatePropertyKeyAction),
            PrivilegeAction("CREATE NEW PROPERTY NAME", CreatePropertyKeyAction),
            PrivilegeAction("CREATE NEW PROPERTY NAMES", CreatePropertyKeyAction),
            PrivilegeAction("NAME", AllTokenActions),
            PrivilegeAction("NAME MANAGEMENT", AllTokenActions),
            PrivilegeAction("ALL", AllDatabaseAction),
            PrivilegeAction("ALL PRIVILEGES", AllDatabaseAction),
            PrivilegeAction("ALL DATABASE PRIVILEGES", AllDatabaseAction)
          )
          val alterDatabasePrivilegesInCypher25 = Seq(
            PrivilegeAction("ALTER DATABASE", AlterDatabaseAction(false), Cypher25),
            PrivilegeAction("SET DATABASE ACCESS", SetDatabaseAccessAction(false), Cypher25),
            PrivilegeAction("SET DATABASE DEFAULT LANGUAGE", SetDatabaseDefaultLanguageAction(false), Cypher25),
            PrivilegeAction("ALTER COMPOSITE DATABASE", AlterCompositeDatabaseAction(false), Cypher25)
          )

          (privilegesInAllVersions ++ alterDatabasePrivilegesInCypher25).foreach {
            case PrivilegeAction(privilege: String, action: DatabaseAction, minVersion: ParserInTest) =>
              val supportedInCypher5: Boolean = minVersion == Cypher5

              test(s"$verb$immutableString $privilege ON DATABASE * $preposition $$role") {
                assertAst(
                  privilegeFunc(action, AllDatabasesScope()(pos), Seq(paramRole), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASES * $preposition role") {
                assertAst(
                  privilegeFunc(action, AllDatabasesScope()(pos), Seq(literalRole), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE * $preposition role1, role2") {
                assertAst(
                  privilegeFunc(
                    action,
                    AllDatabasesScope()(pos),
                    Seq(literalRole1, literalRole2),
                    immutable
                  )(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition role") {
                assertAst(
                  privilegeFunc(action, databaseScopeFoo, Seq(literalRole), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE `fo:o` $preposition role") {
                assertAst(
                  privilegeFunc(
                    action,
                    NamedDatabasesScope(Seq(literal("fo:o")))(pos),
                    Seq(literalRole),
                    immutable
                  )(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE more.Dots.more.Dots $preposition role") {
                assertAstVersionBased(
                  fromCypher5 =>
                    privilegeFunc(
                      action,
                      NamedDatabasesScope(Seq(namespacedName(fromCypher5, "more", "Dots", "more", "Dots")))(pos),
                      Seq(literalRole),
                      immutable
                    )(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition `r:ole`") {
                assertAst(
                  privilegeFunc(action, databaseScopeFoo, Seq(literalRColonOle), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition role1, $$role2") {
                assertAst(
                  privilegeFunc(action, databaseScopeFoo, Seq(literalRole1, paramRole2), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE $$foo $preposition role") {
                assertAst(
                  privilegeFunc(action, databaseScopeParamFoo, Seq(literalRole), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo, bar $preposition role") {
                assertAst(
                  privilegeFunc(action, databaseScopeFooBar, Seq(literalRole), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON DATABASES foo, $$bar $preposition role") {
                assertAst(
                  privilegeFunc(
                    action,
                    databaseScopeFooParamBar,
                    Seq(literalRole),
                    immutable
                  )(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE $preposition role") {
                assertAst(
                  privilegeFunc(action, HomeDatabaseScope()(pos), Seq(literalRole), immutable)(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON HOME DATABASE $preposition $$role1, role2") {
                assertAst(
                  privilegeFunc(
                    action,
                    HomeDatabaseScope()(pos),
                    Seq(paramRole1, literalRole2),
                    immutable
                  )(pos),
                  supportedInCypher5 = supportedInCypher5
                )
              }

              test(s"$verb$immutableString $privilege ON GRAPH * $preposition role") {
                // GRAPH instead of DATABASE
                if (!(privilege.equals("ALL") || privilege.equals("ALL PRIVILEGES"))) {
                  failsParsing[Statements]
                }
              }

              test(s"$verb$immutableString $privilege ON DATABASE fo:o $preposition role") {
                // invalid database name
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo, * $preposition role") {
                // specific database followed by *
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASE *, foo $preposition role") {
                // * followed by specific database
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASE foo $preposition r:ole") {
                // invalid role name
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASES * $preposition") {
                // Missing role
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASES *") {
                // Missing role and preposition
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON DATABASES $preposition role") {
                // Missing dbName
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege ON * $preposition role") {
                // Missing DATABASE keyword
                failsParsing[Statements]
              }

              test(s"$verb$immutableString $privilege DATABASE foo $preposition role") {
                // Missing ON keyword
                failsParsing[Statements]
              }
          }

          privilegesInAllVersions.foreach { case PrivilegeAction(privilege, _, _) =>
            test(s"$verb$immutableString $privilege ON DEFAULT DATABASE $preposition role") {
              failsParsing[Statements].in {
                case Cypher5 =>
                  _.withOldSyntax("`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.")
                case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }

            test(s"$verb$immutableString $privilege ON DATABASE `a`.`b`.`c` $preposition role") {
              // more than two components
              failsParsing[Statements].in {
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

            test(s"$verb$immutableString $privilege ON HOME DATABASES $preposition role") {
              // 'databases' instead of 'database'
              failsParsing[Statements].withSyntaxErrorContaining(
                """Invalid input 'DATABASES': expected 'DATABASE'"""
              )

            }

            test(s"$verb$immutableString $privilege ON HOME DATABASE foo $preposition role") {
              // both home and database name
              failsParsing[Statements].withSyntaxErrorContaining(
                s"""Invalid input 'foo': expected '$preposition'"""
              )
            }

            test(s"$verb$immutableString $privilege ON HOME DATABASE * $preposition role") {
              // both home and *
              failsParsing[Statements].withSyntaxErrorContaining(
                s"""Invalid input '*': expected '$preposition'"""
              )
            }

            test(s"$verb$immutableString $privilege ON DEFAULT DATABASES $preposition role") {
              // 'databases' instead of 'database'
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining(
                    """Invalid input 'DATABASES': expected 'DATABASE'""".stripMargin
                  )
                case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }

            test(s"$verb$immutableString $privilege ON DEFAULT DATABASE foo $preposition role") {
              // both default and database name
              failsParsing[Statements].in {
                case Cypher5 =>
                  _.withOldSyntax("`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.")
                case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }

            test(s"$verb$immutableString $privilege ON DEFAULT DATABASE * $preposition role") {
              // both default and *
              failsParsing[Statements].in {
                case Cypher5 =>
                  _.withOldSyntax("`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.")
                case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }
          }

          alterDatabasePrivilegesInCypher25.foreach { case PrivilegeAction(privilege, _, _) =>
            test(s"$verb$immutableString $privilege ON DEFAULT DATABASE $preposition role") {
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DBMS'")
                case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }

            test(s"$verb$immutableString $privilege ON DATABASE `a`.`b`.`c` $preposition role") {
              // more than two components
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'DATABASE': expected 'DBMS'")
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

            test(s"$verb$immutableString $privilege ON HOME DATABASES $preposition role") {
              // 'databases' instead of 'database'
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'HOME': expected 'DBMS'")
                case _       => _.withSyntaxErrorContaining("""Invalid input 'DATABASES': expected 'DATABASE'""")
              }
            }

            test(s"$verb$immutableString $privilege ON HOME DATABASE foo $preposition role") {
              // both home and database name
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'HOME': expected 'DBMS'")
                case _       => _.withSyntaxErrorContaining(s"""Invalid input 'foo': expected '$preposition'""")
              }
            }

            test(s"$verb$immutableString $privilege ON HOME DATABASE * $preposition role") {
              // both home and *
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'HOME': expected 'DBMS'")
                case _       => _.withSyntaxErrorContaining(s"""Invalid input '*': expected '$preposition'""")
              }
            }

            test(s"$verb$immutableString $privilege ON DEFAULT DATABASES $preposition role") {
              // 'databases' instead of 'database'
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("""Invalid input 'DEFAULT': expected 'DBMS'""")
                case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }

            test(s"$verb$immutableString $privilege ON DEFAULT DATABASE foo $preposition role") {
              // both default and database name
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("""Invalid input 'DEFAULT': expected 'DBMS'""")
                case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }

            test(s"$verb$immutableString $privilege ON DEFAULT DATABASE * $preposition role") {
              // both default and *
              failsParsing[Statements].in {
                case Cypher5 => _.withSyntaxErrorContaining("""Invalid input 'DEFAULT': expected 'DBMS'""")
                case _       => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
              }
            }
          }

          // Dropping instead of creating name management privileges

          test(s"$verb$immutableString DROP NEW LABEL ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP NEW TYPE ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP NEW NAME ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP LABEL ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP TYPE ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString DROP NAME ON DATABASE * $preposition role") {
            failsParsing[Statements]
          }
      }
  }

  // transaction management

  Seq(
    ("GRANT", "TO", grantTransactionPrivilege: transactionPrivilegeFunc),
    ("DENY", "TO", denyTransactionPrivilege: transactionPrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantTransactionPrivilege: transactionPrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyTransactionPrivilege: transactionPrivilegeFunc),
    ("REVOKE", "FROM", revokeTransactionPrivilege: transactionPrivilegeFunc)
  ).foreach {
    case (verb: String, preposition: String, privilegeFunc: transactionPrivilegeFunc) =>
      Seq[Immutable](true, false).foreach {
        immutable =>
          val immutableString = maybeImmutable(immutable)
          test(s"$verb$immutableString SHOW TRANSACTION (*) ON DATABASE * $preposition role") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              AllDatabasesScope()(pos),
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              databaseScopeFoo,
              List(UserAllQualifier()(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (*) ON DATABASES $$foo $preposition $$role1, $$role2") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              databaseScopeParamFoo,
              List(UserAllQualifier()(pos)),
              Seq(paramRole1, paramRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON HOME DATABASE $preposition role") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              HomeDatabaseScope()(pos),
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ($$user) ON HOME DATABASE $preposition role") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              HomeDatabaseScope()(pos),
              List(UserQualifier(paramUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 =>
                _.withOldSyntax("`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.")
              case _ => _.withSyntaxErrorContaining(
                  "Invalid input 'DEFAULT': expected 'DATABASE', 'HOME DATABASE' or 'DATABASES'"
                )
            }
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              AllDatabasesScope()(pos),
              List(UserQualifier(literalUser1)(pos), UserQualifier(literal("user2"))(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTIONS ON DATABASES * $preposition role") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              AllDatabasesScope()(pos),
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION ON DATABASE foo, bar $preposition role") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              databaseScopeFooBar,
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString SHOW TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            assertAst(privilegeFunc(
              ShowTransactionAction,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (*) ON DATABASE * $preposition $$role") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope()(pos),
              List(UserAllQualifier()(pos)),
              Seq(paramRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (*) ON DATABASES foo $preposition role1, role2") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              databaseScopeFoo,
              List(UserAllQualifier()(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (*) ON DATABASES $$foo $preposition role") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              databaseScopeParamFoo,
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON HOME DATABASE $preposition role") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              HomeDatabaseScope()(pos),
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 =>
                _.withOldSyntax("`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.")
              case _ => _.withSyntaxErrorContaining("Invalid input 'DEFAULT': expected 'DATABASE',")
            }
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS (user1,user2) ON DATABASES * $preposition role1, role2") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope()(pos),
              List(UserQualifier(literalUser1)(pos), UserQualifier(literal("user2"))(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(
            s"$verb$immutableString TERMINATE TRANSACTIONS ($$user1,$$user2) ON DATABASES * $preposition role1, role2"
          ) {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope()(pos),
              List(UserQualifier(stringParam("user1"))(pos), UserQualifier(stringParam("user2"))(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTIONS ON DATABASES * $preposition role") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              AllDatabasesScope()(pos),
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION ON DATABASE foo, bar $preposition role") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              databaseScopeFooBar,
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TERMINATE TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            assertAst(privilegeFunc(
              TerminateTransactionAction,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASES * $preposition role1, role2") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope()(pos),
              List(UserAllQualifier()(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (*) ON DATABASES foo $preposition role1, $$role2") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              databaseScopeFoo,
              List(UserAllQualifier()(pos)),
              Seq(literalRole1, paramRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (*) ON DATABASES $$foo $preposition role1, role2") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              databaseScopeParamFoo,
              List(UserAllQualifier()(pos)),
              Seq(literalRole1, literalRole2),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (user) ON DATABASES * $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope()(pos),
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE foo, bar $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              databaseScopeFooBar,
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION (user) ON DATABASES foo, $$bar $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON HOME DATABASE $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              HomeDatabaseScope()(pos),
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON DEFAULT DATABASE $preposition role") {
            failsParsing[Statements].in {
              case Cypher5 =>
                _.withOldSyntax("`ON DEFAULT DATABASE` is not supported. Use `ON HOME DATABASE` instead.")
              case _ => _.withSyntaxErrorContaining(
                  "Invalid input 'DEFAULT': expected 'DATABASE', 'HOME DATABASE' or 'DATABASES'"
                )
            }
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user) ON DATABASES * $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope()(pos),
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user1, $$user2) ON DATABASES * $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              AllDatabasesScope()(pos),
              List(UserQualifier(literalUser1)(pos), UserQualifier(stringParam("user2"))(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT ON DATABASE foo, bar $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              databaseScopeFooBar,
              List(UserAllQualifier()(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION MANAGEMENT (user) ON DATABASES foo, $$bar $preposition role") {
            assertAst(privilegeFunc(
              AllTransactionActions,
              databaseScopeFooParamBar,
              List(UserQualifier(literalUser)(pos)),
              Seq(literalRole),
              immutable
            )(pos))
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE foo, * $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRANSACTION ON DATABASE `a`.`b`.`c` $preposition role") {
            failsParsing[Statements].in {
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

          test(s"$verb$immutableString TRANSACTION ON DATABASE *, foo $preposition role") {
            failsParsing[Statements]
          }

          test(s"$verb$immutableString TRANSACTIONS ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            failsParsing[Statements].withSyntaxErrorContaining("""Invalid input 'TRANSACTIONS': expected""")
          }

          test(s"$verb$immutableString TRANSACTIONS (*) ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            failsParsing[Statements].withSyntaxErrorContaining("""Invalid input 'TRANSACTIONS': expected""")
          }

          test(s"$verb$immutableString TRANSACTIONS MANAGEMENT ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            failsParsing[Statements].withSyntaxErrorContaining("""Invalid input 'TRANSACTIONS': expected""")
          }

          test(s"$verb$immutableString TRANSACTIONS MANAGEMENT (*) ON DATABASES * $preposition role") {
            // can't have the complete error message, since grant and revoke have more accepted keywords than deny
            failsParsing[Statements].withSyntaxErrorContaining("""Invalid input 'TRANSACTIONS': expected""")
          }
      }
  }
}
