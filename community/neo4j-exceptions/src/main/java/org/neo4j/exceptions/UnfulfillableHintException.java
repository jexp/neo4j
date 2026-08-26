/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.exceptions;

import java.util.List;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public abstract class UnfulfillableHintException extends Neo4jException {
    protected UnfulfillableHintException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    @Override
    public Status status() {
        return Status.Statement.ExecutionFailed;
    }

    public static UnfulfillableHintException duplicateHints(
            List<String> prettifiedHints, List<String> prettifiedVariableNames) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
                .withParam(GqlParams.ListParam.hintList, prettifiedHints)
                .build();
        return new UnfulfillableHintException(
                gql,
                "Unable to plan multiple hints for the variable(s): %s."
                        .formatted(String.join(", ", prettifiedVariableNames))) {};
    }

    public static UnfulfillableHintException alreadyBoundVariableHint(String prettifiedHint, String variable) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
                .withParam(GqlParams.ListParam.hintList, List.of(prettifiedHint))
                .build();
        return new UnfulfillableHintException(
                gql,
                "Unable to plan hint `%s`. The variable `%s` is already bound.".formatted(prettifiedHint, variable)) {};
    }
}
