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
package org.neo4j.genai.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.test.ports.PortAuthority;

class HttpServiceTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 5, 8, 10, 14, 15, 16})
    @Timeout(value = 45, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void pipeShouldNotBlock(int size) throws IOException {
        var port = PortAuthority.allocatePort();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        try {
            var maxSleepInMillis = 500;
            server.createContext("/test", exchange -> {
                var response = (String) JsonUtils.getObjectMapper()
                        .readValue(exchange.getRequestBody(), JsonUtils.TYPE_REF_MAP_STRING_OBJECT)
                        .get("key");
                var bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bytes.length);
                var outputStream = exchange.getResponseBody();
                outputStream.write(bytes);
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(maxSleepInMillis));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                outputStream.flush();
                outputStream.close();
            });
            server.setExecutor(null);
            server.start();

            var secureRandom = new SecureRandom();
            var buffer = new byte[(int) Math.pow(2, size)];
            secureRandom.nextBytes(buffer);
            var body = Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);

            var service = new HttpService(new URLAccessChecker() {
                @Override
                public URL checkURL(URL url) {
                    return url;
                }
            });
            var result = service.request(
                    URI.create("http://localhost:%d/test".formatted(port)),
                    (builder -> {
                        HttpRequest.BodyPublisher pipe = HttpService.pipe(outputStream -> {
                            try {
                                JsonUtils.getObjectMapper().writeValue(outputStream, Map.of("key", body));
                                Thread.sleep(ThreadLocalRandom.current().nextInt(maxSleepInMillis));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
                        return builder.POST(pipe).build();
                    }),
                    inputStream -> {
                        try (var in = new BufferedInputStream(inputStream)) {
                            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
            assertThat(result).isEqualTo(body);
        } finally {
            server.stop(0);
        }
    }
}
