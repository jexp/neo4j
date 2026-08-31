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
grammar CypherPreparser;

SPACE
   : ( '\u0009'
      | '\n' //can't parse this in unicode
      | '\u000B'
      | '\u000C'
      | '\r' //can't parse this in unicode
      | '\u001C'
      | '\u001D'
      | '\u001E'
      | '\u001F'
      | '\u0020'
      | '\u0085'
      | '\u00A0'
      | '\u1680'
      | '\u2000'
      | '\u2001'
      | '\u2002'
      | '\u2003'
      | '\u2004'
      | '\u2005'
      | '\u2006'
      | '\u2007'
      | '\u2008'
      | '\u2009'
      | '\u200A'
      | '\u2028'
      | '\u2029'
      | '\u202F'
      | '\u205F'
      | '\u3000'
   ) -> channel (HIDDEN)
   ;

SINGLE_LINE_COMMENT
   : '//' ~[\r\n]* -> channel (HIDDEN)
   ;

MULTI_LINE_COMMENT
   : '/*' .*? '*/' -> channel (HIDDEN)
   ;

CYPHER
   : C Y P H E R
   ;

VERSION
   : ([0-9] | '.')+
   ;

EXPLAIN
   : E X P L A I N
   ;

PLAN
   : P L A N
   ;

PROFILE
   : P R O F I L E
   ;

SCOPE
   : S C O P E
   ;

IDENTIFIER
   : Letter LetterOrDigit*
   ;

fragment LetterOrDigit
   : Letter
   | [0-9]
   ;

fragment Letter
   : [a-zA-Z$_]                      // these are the "java letters" below 0x7F
   // covers all characters above 0x7F which are neither space (see SPACE) nor a surrogate
   | ~[\u0000-\u007F\u0085\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000\uD800-\uDBFF]
   | [\uD800-\uDBFF] [\uDC00-\uDFFF] // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
   ;

fragment A
   : [aA]
   ;

fragment B
   : [bB]
   ;

fragment C
   : [cC]
   ;

fragment D
   : [dD]
   ;

fragment E
   : [eE]
   ;

fragment F
   : [fF]
   ;

fragment G
   : [gG]
   ;

fragment H
   : [hH]
   ;

fragment I
   : [iI]
   ;

fragment J
   : [jJ]
   ;

fragment K
   : [kK]
   ;

fragment L
   : [lL]
   ;

fragment M
   : [mM]
   ;

fragment N
   : [nN]
   ;

fragment O
   : [oO]
   ;

fragment P
   : [pP]
   ;

fragment Q
   : [qQ]
   ;

fragment R
   : [rR]
   ;

fragment S
   : [sS]
   ;

fragment T
   : [tT]
   ;

fragment U
   : [uU]
   ;

fragment V
   : [vV]
   ;

fragment W
   : [wW]
   ;

fragment X
   : [xX]
   ;

fragment Y
   : [yY]
   ;

fragment Z
   : [zZ]
   ;

// Should always be last in the file before modes
ErrorChar
    : .
    ;

/*
 * Important!
 * Preparsing is skipped if the first token is not CYPHER, EXPLAIN or PROFILE.
 * The assumption is that all preparser options starts with one of those tokens.
 * If you make changes that breaks this assumption, you need to update the preparser implementation.
 */
preparserOptions
   : option* statement?
   ;

/*
 This is added just for preparsing cypher shell.
 This rule will fail in option instead of statement,
 providing better token candidates
 */
strictlyPreparserOptions
  : option* EOF;

option
   : cypher VERSION? (setting)*
   | EXPLAIN planMode?
   | PROFILE
   ;

planMode
   : PLAN
   | SCOPE
   ;

cypher
  : CYPHER
  ;

setting
   : IDENTIFIER '=' (IDENTIFIER | VERSION)
   ;

// Matches the first token after the preparser options. Used to determine end of preparser options.
statement
   : .
   ;

