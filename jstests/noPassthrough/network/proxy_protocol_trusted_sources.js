/**
 * Verifies that `proxyProtocolTrustedSources` restricts which TCP peers may send a proxy protocol
 * header to the proxy port, so a client cannot spoof its source address by connecting directly.
 * The ingress code is the same for both `mongos` and `mongod`, so we only test against a `mongod`.
 * @tags: [
 *    requires_fcv_83,
 *    grpc_incompatible,
 * ]
 */

if (_isWindows()) {
    quit();
}

import {ProxyProtocolServer} from "jstests/sharding/libs/proxy_protocol.js";

const kUntrustedProxySourceLogId = 10979301;
const kNoTrustedSourcesWarningLogId = 10979300;

function rejectedUntrustedProxySources(conn) {
    return assert.commandWorked(conn.adminCommand({serverStatus: 1}))
        .network.connsRejectedDueToUntrustedProxyProtocolSource;
}

function startMongod(proxyPort, trustedSources) {
    const setParameter = {logComponentVerbosity: {network: {verbosity: 2}}};
    if (trustedSources !== undefined) {
        setParameter.proxyProtocolTrustedSources = trustedSources;
    }
    return MongoRunner.runMongod({proxyPort: proxyPort, setParameter: setParameter});
}

function sendSpoofedHeader(proxyPort) {
    // A client connecting straight to the proxy port claims to be an internal address.
    assert.eq(0,
              runProgram("bash",
                         "-c",
                         `exec 3<>/dev/tcp/127.0.0.1/${proxyPort}; ` +
                             `printf 'PROXY TCP4 10.0.0.5 10.0.0.1 1000 3000\\r\\n' >&3; ` +
                             `timeout 10 cat <&3 >/dev/null; exit 0`));
}

const ingressPort = allocatePort();
const proxyPort = allocatePort();

// The proxy port warns when no trusted sources are configured, and keeps accepting all peers.
{
    const conn = startMongod(proxyPort);
    checkLog.containsJson(conn, kNoTrustedSourcesWarningLogId);
    assert.eq(0, rejectedUntrustedProxySources(conn));

    const proxyServer = new ProxyProtocolServer(ingressPort, proxyPort, 1);
    proxyServer.start();
    assert.commandWorked(new Mongo(`mongodb://127.0.0.1:${ingressPort}`).adminCommand({hello: 1}));
    proxyServer.stop();

    MongoRunner.stopMongod(conn);
}

// Loopback is not trusted: both a spoofed header sent directly and a connection through a local
// proxy are rejected before the header is parsed, while the main port keeps working.
{
    const conn = startMongod(proxyPort, "192.0.2.0/24,2001:db8::/32");
    assert(!checkLog.checkContainsOnceJson(conn, kNoTrustedSourcesWarningLogId, {}));

    sendSpoofedHeader(proxyPort);
    assert.soon(() => rejectedUntrustedProxySources(conn) == 1);
    checkLog.containsJson(conn, kUntrustedProxySourceLogId);

    for (const version of [1, 2]) {
        const proxyServer = new ProxyProtocolServer(ingressPort, proxyPort, version);
        proxyServer.start();
        assert.throws(() => new Mongo(`mongodb://127.0.0.1:${ingressPort}`));
        proxyServer.stop();
    }
    assert.soon(() => rejectedUntrustedProxySources(conn) >= 3);

    assert.commandWorked(new Mongo(`mongodb://127.0.0.1:${conn.port}`).adminCommand({hello: 1}));

    MongoRunner.stopMongod(conn);
}

// Loopback is trusted: the local proxy can connect and its header is honored.
{
    const conn = startMongod(proxyPort, "192.0.2.0/24,127.0.0.0/8");

    for (const version of [1, 2]) {
        const proxyServer = new ProxyProtocolServer(ingressPort, proxyPort, version);
        proxyServer.start();
        assert.commandWorked(
            new Mongo(`mongodb://127.0.0.1:${ingressPort}`).adminCommand({hello: 1}));
        proxyServer.stop();
    }
    assert.eq(0, rejectedUntrustedProxySources(conn));

    MongoRunner.stopMongod(conn);
}

// Invalid entries are rejected at startup.
assert.throws(() => startMongod(proxyPort, "127.0.0.1/8,not-a-cidr"));
