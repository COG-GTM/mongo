/**
 * Verifies that connections accepted on the proxy port that are still waiting for the proxy
 * protocol header count against the session limit (derived from maxConns and the open file limit),
 * even when proxyProtocolMaximumPendingConnections is left at its default, and that the server
 * keeps accepting connections on the main port.
 *
 * @tags: [
 *    requires_fcv_82,
 *    grpc_incompatible,
 * ]
 */

if (_isWindows()) {
    quit();
}

function getNumberOfRejectedProxyConnections(conn) {
    return assert.commandWorked(conn.getDB("admin").runCommand({serverStatus: 1}))
        .network.connsRejectedDueToMaxPendingProxyProtocolHeader;
}

const kMaxConns = 10;
const listenPort = allocatePort();
const proxyPort = allocatePort();

const conn = MongoRunner.runMongod({
    port: listenPort,
    proxyPort: proxyPort,
    maxConns: kMaxConns,
    setParameter: {proxyProtocolTimeoutSecs: 120},
});

const admin = conn.getDB("admin");
assert.eq(0, getNumberOfRejectedProxyConnections(conn));
const openSessions = assert.commandWorked(admin.runCommand({serverStatus: 1})).connections.current;
assert.lt(openSessions, kMaxConns);

// None of these connections send a proxy protocol header, so they stay pending until the timeout.
const kAttempts = 2 * kMaxConns;
for (let i = 0; i < kAttempts; i++) {
    assert.eq(0, runProgram("bash", "-c", `exec <>/dev/tcp/127.0.0.1/${proxyPort}`));
}

// One pending slot (a tenth of maxConns, at least one) is always reserved for exempt clients.
const expectedRejected = kAttempts - Math.max(kMaxConns - openSessions, 1);
let observed = 0;
assert.soon(() => {
    observed = getNumberOfRejectedProxyConnections(conn);
    return observed == expectedRejected;
}, () => `observed = ${observed}, expected = ${expectedRejected}`);

// The main port must still accept and serve new connections.
const direct = new Mongo(conn.host);
assert.commandWorked(direct.getDB("admin").runCommand({ping: 1}));

MongoRunner.stopMongod(conn);
