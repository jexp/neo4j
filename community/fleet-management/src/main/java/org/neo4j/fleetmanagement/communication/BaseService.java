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
package org.neo4j.fleetmanagement.communication;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.neo4j.fleetmanagement.communication.model.ErrorCode;
import org.neo4j.fleetmanagement.communication.model.ErrorResponse;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;

public class BaseService {
    protected final ObjectMapper objectMapper;
    protected final ITransactor transactor;
    protected final Log userLog;
    protected final Logger fleetManagerLog;
    protected final Configuration configuration;

    protected final State state;

    public BaseService(ITransactor transactor, State state, Configuration configuration) {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
        this.transactor = transactor;
        this.userLog = Logger.getNeo4jLogger();
        this.fleetManagerLog = Logger.getFleetManagerLogger();
        this.state = state;
        this.configuration = configuration;
    }

    void handleErrorResponse(String errMsgPrefix, int responseCode, byte[] responseBody) {
        if (responseBody == null || responseBody.length == 0) {
            var errorMsg = errMsgPrefix + " - Error response without body, code: " + responseCode;
            this.state.setDisconnected(errorMsg);
            this.userLog.error(errorMsg);
            return;
        }
        try {
            ErrorResponse errorResponse = objectMapper.readValue(responseBody, ErrorResponse.class);
            if (errorResponse != null && errorResponse.code != 0) {
                handleValidErrorResponse(errMsgPrefix, errorResponse);
            } else {
                handleInvalidErrorResponse(errMsgPrefix, responseCode, responseBody, errorResponse);
            }
        } catch (Exception e) {
            this.userLog.debug("Failed to parse error response", e);
            handleInvalidErrorResponse(errMsgPrefix, responseCode, responseBody, null);
        }
    }

    private void handleInvalidErrorResponse(
            String errMsgPrefix, int responseCode, byte[] responseBody, ErrorResponse errorResponse) {
        var errorMsg = "Unknown error response received: '" + new String(responseBody) + "' with code: " + responseCode;
        if (errorResponse != null && errorResponse.message != null) {
            errorMsg = "Error response: '" + errorResponse.message + "' with code: " + responseCode;
        }
        this.userLog.error(errMsgPrefix + " - " + errorMsg);
        this.state.setDisconnected(errorMsg);
    }

    private void handleValidErrorResponse(String errMsgPrefix, ErrorResponse errorResponse) {
        ErrorCode code = errorResponse.code();
        if (code == ErrorCode.TOKEN_ROTATION_DUE) {
            if (!this.state.isRotatingToken()) {
                this.userLog.info(errMsgPrefix + " - " + errorResponse);
                this.state.setDisconnected(code.getStatusMessage());
                this.transactor.rotateToken();
                this.state.setRotatingToken(true);
            }
        } else if (code == ErrorCode.TOKEN_EXPIRED) {
            this.userLog.error(errMsgPrefix + " - " + errorResponse);
            this.transactor.deleteToken();
            this.state.setDisconnected(code.getStatusMessage());
        } else if (code == ErrorCode.TOKEN_REVOKED) {
            this.userLog.error(errMsgPrefix + " - " + errorResponse);
            this.transactor.deleteToken();
            this.state.setDisconnected(code.getStatusMessage());
        } else if (code == ErrorCode.ACCESS_DENIED) {
            this.userLog.error(errMsgPrefix + " - " + errorResponse);
            this.transactor.deleteToken();
            this.state.setDisconnected(code.getStatusMessage());
        } else {
            this.userLog.error(errMsgPrefix + " - Unknown error code in error response: " + errorResponse);
            this.state.setDisconnected(errMsgPrefix + " - Unknown error code: " + errorResponse.code());
        }
    }
}
