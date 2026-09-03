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
package org.neo4j.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.PreferHeapByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import org.neo4j.configuration.ssl.ClientAuth;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class SslPolicy {
    private static final List<String> DISABLED_CIPHERS = List.of("_CBC_", "_DH_", "_ECDH_");

    /* cryptographic objects */
    private final PrivateKey privateKey;
    private final X509Certificate[] keyCertChain;

    /* cryptographic parameters */
    private final List<String> ciphers;
    private final String[] tlsVersions;
    private final ClientAuth clientAuth;

    /* Temporary, to be removed when we have a proper way to configure the driver */
    private final Path privateKeyFile;
    private final Path certificateFile;

    private final TrustManagerFactory trustManagerFactory;
    private final SslProvider sslProvider;

    private final boolean verifyHostname;
    private final boolean verifyExpiration;
    private final InternalLog log;

    public SslPolicy(
            PrivateKey privateKey,
            Path privateKeyFile,
            X509Certificate[] keyCertChain,
            Path certificateFile,
            List<String> tlsVersions,
            List<String> ciphers,
            ClientAuth clientAuth,
            TrustManagerFactory trustManagerFactory,
            SslProvider sslProvider,
            boolean verifyHostname,
            boolean verifyExpiration,
            InternalLogProvider logProvider) {
        this.privateKey = privateKey;
        this.keyCertChain = keyCertChain;
        this.tlsVersions = tlsVersions == null ? null : tlsVersions.toArray(new String[0]);
        this.clientAuth = clientAuth;
        this.trustManagerFactory = trustManagerFactory;
        this.sslProvider = sslProvider;
        this.verifyHostname = verifyHostname;
        this.verifyExpiration = verifyExpiration;
        this.log = logProvider.getLog(SslPolicy.class);
        this.privateKeyFile = privateKeyFile;
        this.certificateFile = certificateFile;

        var filteredCiphers = removeInsecureCiphersFromDefaults(ciphers);
        this.ciphers = filteredCiphers;
    }

    private List<String> removeInsecureCiphersFromDefaults(List<String> ciphers) {
        if (ciphers == null) // default ciphers
        {
            try {
                var builder = SslContextBuilder.forClient()
                        .sslProvider(sslProvider)
                        .keyManager(privateKey, keyCertChain)
                        .protocols(tlsVersions)
                        .ciphers(ciphers)
                        .trustManager(trustManagerFactory);
                var context = builder.build();
                var alloc = PreferHeapByteBufAllocator.DEFAULT;
                var engine = context.newEngine(alloc);
                var enabledCiphers = engine.getEnabledCipherSuites();

                ciphers = Arrays.stream(enabledCiphers)
                        .filter(cipher -> !DISABLED_CIPHERS.contains(cipher))
                        .toList();
            } catch (SSLException e) {
                log.warn("Exception interrogating default enabled cipher suites", e);
            }
        }
        return ciphers;
    }

    public SslContext nettyServerContext() throws SSLException {
        return SslContextBuilder.forServer(privateKey, keyCertChain)
                .sslProvider(sslProvider)
                .clientAuth(forNetty(clientAuth))
                .protocols(tlsVersions)
                .ciphers(ciphers)
                .trustManager(trustManagerFactory)
                .build();
    }

    public SslContext nettyClientContext() throws SSLException {
        var builder = SslContextBuilder.forClient()
                .sslProvider(sslProvider)
                .keyManager(privateKey, keyCertChain)
                .protocols(tlsVersions)
                .ciphers(ciphers)
                .trustManager(trustManagerFactory);
        if (!verifyHostname) {
            builder.endpointIdentificationAlgorithm(null);
        }
        return builder.build();
    }

    private static io.netty.handler.ssl.ClientAuth forNetty(ClientAuth clientAuth) {
        return switch (clientAuth) {
            case NONE -> io.netty.handler.ssl.ClientAuth.NONE;
            case OPTIONAL -> io.netty.handler.ssl.ClientAuth.OPTIONAL;
            case REQUIRE -> io.netty.handler.ssl.ClientAuth.REQUIRE;
        };
    }

    public ChannelHandler nettyServerHandler(Channel channel) throws SSLException {
        return nettyServerHandler(channel, nettyServerContext());
    }

    private static ChannelHandler nettyServerHandler(Channel channel, SslContext sslContext) {
        SSLEngine sslEngine = sslContext.newEngine(channel.alloc());
        return new SslHandler(sslEngine);
    }

    public ChannelHandler nettyClientHandler(Channel channel) throws SSLException {
        return nettyClientHandler(channel, nettyClientContext());
    }

    public ChannelHandler nettyClientHandler(Channel channel, SslContext sslContext) {
        return new ClientSideOnConnectSslHandler(channel, sslContext, verifyHostname, tlsVersions, new String[] {});
    }

    public ChannelHandler nettyClientHandler(Channel channel, SslContext sslContext, String[] namedGroups) {
        return new ClientSideOnConnectSslHandler(channel, sslContext, verifyHostname, tlsVersions, namedGroups);
    }

    public PrivateKey privateKey() {
        return privateKey;
    }

    public X509Certificate[] certificateChain() {
        return keyCertChain;
    }

    public Path certificateFile() {
        return certificateFile;
    }

    public Path privateKeyFile() {
        return privateKeyFile;
    }

    public KeyStore getKeyStore(char[] keyStorePass, char[] privateKeyPass) {
        KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            log.debug("Keystore loaded is of type " + keyStore.getClass().getName());
            keyStore.load(null, keyStorePass);
            keyStore.setKeyEntry("key", privateKey, privateKeyPass, keyCertChain);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return keyStore;
    }

    public TrustManagerFactory getTrustManagerFactory() {
        return trustManagerFactory;
    }

    public List<String> getCipherSuites() {
        return ciphers;
    }

    public String[] getTlsVersions() {
        return tlsVersions;
    }

    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    public boolean isVerifyHostname() {
        return verifyHostname;
    }

    public boolean shouldVerifyExpiration() {
        return verifyExpiration;
    }

    @Override
    public String toString() {
        return "SslPolicy{" + "keyCertChain="
                + describeCertChain() + ", ciphers="
                + ciphers + ", tlsVersions="
                + Arrays.toString(tlsVersions) + ", clientAuth="
                + clientAuth + '}';
    }

    private static String describeCertificate(X509Certificate certificate) {
        return "Subject: " + certificate.getSubjectDN() + ", Issuer: " + certificate.getIssuerDN();
    }

    private String describeCertChain() {
        List<String> certificates =
                Arrays.stream(keyCertChain).map(SslPolicy::describeCertificate).collect(Collectors.toList());
        return String.join(", ", certificates);
    }
}
