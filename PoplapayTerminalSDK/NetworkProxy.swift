//
//  NetworkProxy.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 05/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import Foundation
import PromiseKit

/*
 *  JSONPOS network proxy implementation, provides NetworkConnect etc for
 *  a given JsonRpcConnection.  Connections are automatically cleaned up
 *  when the underlying JsonRpcConnection closes.
 *
 *  The network proxy maintains a connectionId -> NetworkConnection mapping.
 *  The proxy handles connect, disconnected, disconnected notify, and
 *  reasonably fair rate limited data writing.
 *
 *  There's a three-level rate limiting strategy:
 *
 *  1. JsonRpcConnection uses a rate limiter and tries to keep withing link
 *     speed limits.  Due to variability of the link speed, this is not
 *     always successful.
 *
 *  2. NetworkProxy rate limits Data notifys so that the notifys, with
 *     base-64 expansion and other overhead, are within a fraction of the
 *     link speed (e.g. 80%).  This leaves some link capacity available
 *     for keepalives and application methods.
 *
 *  3. NetworkProxy monitors the JsonRpcConnection output queue size in
 *     bytes.  If the estimated transfer time of the already queued messages
 *     is too long (several seconds), the proxy stops writing Data notifys.
 *     This may happen if the link is slower than anticipated, and backing
 *     off allows keepalives and other methods to work reasonably.
 *
 *  Data rate limiting tracks the background vs. interactive status of each
 *  connection using a simple heuristic, and prefers interactive connections
 *  when deciding which connections get write attention.  When the terminal
 *  is in non-idle state (processing a transaction) background data is further
 *  reduced to minimize latency for interactive use cases.
 */

public class NetworkProxy: NSObject {
    
    private static let logTag = "NetworkProxy"
    
    private static let WRITE_TRIGGER_TIMEOUT_IDLE: TimeInterval = 5
    private static let WRITE_TRIGGER_TIMEOUT_NONIDLE: TimeInterval = 0.5
    private static let WRITE_THROTTLE_SLEEP: TimeInterval = 2.5
    private static let BACKGROUND_WRITE_INTERVAL: TimeInterval = 0.5
    
    private static let PREFER_INTERACTIVE_LIMIT = 3
    
    private let conn: JsonRpcConnection
    private let dataWriteLimiter: RateLimiter?
    private let linkSpeed: Int
    
    private var connections: [Int64: NetworkProxyConnection] = [:]
    
    private var writeTriggerPromise: Promise<Void>
    private var writeTriggerResolver: Resolver<Void>
    
    private var started = false
    private var stopped = false
    private var allowConnections = false
    private var selectPreferInteractiveCount = 0
    private var terminalIsIdle = true
    private var lastNonIdleBgWriteTime: Date!
    
    public init(conn: JsonRpcConnection, dataWriteLimiter: RateLimiter?, linkSpeed: Int) {
        self.conn = conn
        self.dataWriteLimiter = dataWriteLimiter
        self.linkSpeed = linkSpeed
        
        (writeTriggerPromise, writeTriggerResolver) = Promise<Void>.pending()
    }
    
    public func registerNetworkMethods(_ dispatcher: JsonRpcDispatcher) {
        let finalProxy = self
        
        dispatcher.registerMethod("NetworkConnect", JsonRpcThreadMethodHandler() { [weak self] params, extras in
            guard let self = self else {
                return nil
            }
            
            guard let params = params,
                  let host = params.string(for: "host"),
                  let port = params.int(for: "port"),
                  let id = params.int64(for: "connection_id") else {
                    
                    throw IllegalStateException("NetworkConnect with non-existent params")
            }
            
            Log.i(NetworkProxy.logTag, String(format: "NetworkConnect: %d -> %@:%d", id, host, port))
            
            guard self.allowConnections else {
                throw IllegalStateException("reject connection, connections not allowed by proxy at this point")
            }
            guard self.connections[id] == nil else {
                throw IllegalArgumentException(String(format: "NetworkConnect for an already active connection id %d", id))
            }
            
            let c = NetworkProxyConnection(host: host, port: port, id: id, proxy: finalProxy, linkSpeed: self.linkSpeed)
            self.connections[id] = c
            c.start()
            _ = try c.connectedPromise.wait()  // block until connected/failed
            
            // Connection closure is handled by the network proxy write
            // loop.  It detects a closed connection with no queued data
            // and sends the necessary NetworkDisconnected.
            
            return nil
        })
        
        dispatcher.registerMethod("NetworkDisconnect", JsonRpcThreadMethodHandler() { [weak self] params, extras in
            guard let params = params,
                  let id = params.int64(for: "connection_id") else {
                    throw IllegalStateException(String(format: "NetworkDisconnect with non-existent params"))
            }
            
            let reason = params.string(for: "reason")
            
            Log.i(NetworkProxy.logTag, String(format: "NetworkDisconnect: %d", id))
            
            guard let c = self?.connections[id] else {
                throw IllegalArgumentException(String(format: "NetworkDisconnect for non-existent connection %d", id))
            }
            c.close(reason != nil ? RuntimeException(reason) : RuntimeException("peer requested closed"))
            _ = try c.closedPromise.wait()  // XXX: unclean wrapped exception when ExecutionException
            
            return nil
        })
        
        dispatcher.registerMethod("Data", JsonRpcInlineMethodHandler() { [weak self] params, extras in
            // Must be handled efficiently, use inline handler.
            guard let params = params,
                  let id = params.int64(for: "id"),
                  let data64 = params.string(for: "data") else {
                    throw IllegalStateException(String(format: "received Data with non-existent params"))
            }
            
            guard let c = self?.connections[id] else {
                throw IllegalStateException(String(format: "received Data for non-existent connection id %d", id))
            }
            
            if let data = Data(base64Encoded: data64) {
                do {
                    try c.write(data)
                } catch {
                    Log.i(NetworkProxy.logTag, "failed to write incoming Data, closing tcp connection", error)
                    c.close(error)
                    throw error
                }
            } else {
                let message = "failed to decode incoming Data, closing tcp connection"
                Log.i(NetworkProxy.logTag, message)
                let error = InvalidDataException(message)
                c.close(error)
                throw error
            }
            
            return nil
        })
    }

    public func setTerminalIdleState(_ isIdle: Bool) {
        let trigger = (isIdle != terminalIsIdle)
        terminalIsIdle = isIdle
        if trigger {
            writeTriggerResolver.fulfill(())
        }
    }

    public func startNetworkProxySync() throws {
        if started {
            throw IllegalStateException("already started")
        }
        
        _ = try conn.sendRequestSync("NetworkStart", nil, nil)
        started = true
        allowConnections = true
        
        let writerThread = Thread() { [weak self] in
            var cause: Error? = nil
            do {
                try self?.runWriteLoop()
                Log.d(NetworkProxy.logTag, "write loop exited")
            } catch {
                Log.d(NetworkProxy.logTag, "write loop failed", error)
                cause = error
            }
            
            self?.forceCloseConnections(cause)
            do {
                try self?.stopNetworkProxySync()
            } catch {
                Log.d(NetworkProxy.logTag, "failed to close proxy", error)
            }
        }
        writerThread.start()
    }

    public func stopNetworkProxySync() throws {
        if !started {
            return
        }
        if stopped {
            return
        }
        stopped = true
        allowConnections = false
        _ = try conn.sendRequestSync("NetworkStop", nil, nil)
        forceCloseConnections(RuntimeException("proxy stopping"))
    }

    private func forceCloseConnections(_ cause: Error?) {
        allowConnections = false
        // Afunc concurrent modification error by getting a snapshot of
        // the key set (which should no longer grow).
        for c in connections.values {
            Log.i(NetworkProxy.logTag, "closing proxy connection ID \(c.id)", cause)
            c.close(RuntimeException("proxy exiting", cause))
        }
    }

    // True if connection needs to write to JSONPOS, either data or a
    // NetworkDisconnected message.
    private func connectionNeedsWrite(_ c: NetworkProxyConnection) -> Bool {
        return c.hasPendingData || c.isClosed
    }

    // Select a network connection next serviced for a write.  This selection
    // must ensure rough fairness (= all connections get data transfer), and
    // should ideally favor connections that seem interactive.  Also closed
    // connections must be selected so that NetworkDisconnected gets sent.
    //
    // Current approach: on most writes prefer interactive-looking connections
    // over non-interactive ones.  Use a 'last selected for write' timestamp
    // to round robin over connections, i.e. we select the connection which
    // has least recently received attention.
    private func selectConnectionHelper(_ interactiveOnly: Bool) -> NetworkProxyConnection? {
        var best: NetworkProxyConnection! = nil
        var oldest: TimeInterval = -1
        let connIds = connections.keys

        for connId in connIds {
            let c: NetworkProxyConnection! = connections[connId]
            if c != nil && connectionNeedsWrite(c) &&
                (!interactiveOnly || c.seemsInteractive) {
                if oldest < 0 || c.lastWriteAttention < oldest {
                    best = c
                    oldest = c.lastWriteAttention
                }
            }
        }

        if best != nil {
            best.lastWriteAttention = Date.timeIntervalSinceReferenceDate
        }
        return best
    }

    private func selectConnectionForWrite() -> NetworkProxyConnection? {
        // To ensure rough fairness run a looping index over the connection
        // ID set.  The set may change so we may skip or process a certain
        // ID twice, but this happens very rarely in practice so it doesn't
        // matter.
        var res: NetworkProxyConnection! = nil
        if selectPreferInteractiveCount < NetworkProxy.PREFER_INTERACTIVE_LIMIT {
            res = selectConnectionHelper(true)
            if res != nil {
                Log.d(NetworkProxy.logTag, String(format: "select interactive connection %d for data write", res.id))
                selectPreferInteractiveCount += 1
                return res
            }
        }
        selectPreferInteractiveCount = 0

        // When terminal is not idle, send data more slowly but still keep
        // sending it e.g. once or twice second to afunc HTTP activity
        // timeouts.
        if !terminalIsIdle {
            if (lastNonIdleBgWriteTime ?? Date.distantPast).timeIntervalUntilNow < NetworkProxy.BACKGROUND_WRITE_INTERVAL {
                Log.d(NetworkProxy.logTag, "terminal is not idle, don't send background data too often")
                return nil
            }
            lastNonIdleBgWriteTime = Date()
        }

        res = selectConnectionHelper(false)
        if res != nil {
            Log.d(NetworkProxy.logTag, String(format: "select connection %d for data write", res.id))
            return res
        }

        //Log.v(NetworkProxy.logTag, "no connection in need of writing, skip write")
        return nil
    }

    private func runWriteLoop() throws {
        while (true) {
            //Log.v(NetworkProxy.logTag, "network proxy write loop")
            if conn.isClosed {
                Log.d(NetworkProxy.logTag, "network proxy write loop exiting, jsonrpc connection is closed")
                break
            }
            if stopped {
                Log.d(NetworkProxy.logTag, "network proxy write loop exiting, stopped==true")
                break
            }

            // If underlying JsonRpcConnection has too much queued data,
            // stop writing for a while because we don't want the queue
            // to become too large.  This is a backstop which tries to
            // ensure that the connection remains minimally responsible
            // even if Data rate limiting doesn't correctly match assumed
            // connection speed.
            if throttleJsonRpcData() {
                Log.d(NetworkProxy.logTag, "connection queue too long, throttle proxy writes")
                Thread.sleep(forTimeInterval: NetworkProxy.WRITE_THROTTLE_SLEEP)
                continue
            }
            
            var wrote = false
            
            if let c = selectConnectionForWrite() {
                if let data = c.getQueuedReadData() {
                    // Queue data to be written to JSONRPC.  Here we assume the caller is
                    // only providing us with reasonably small chunks (see read buffer size
                    // in NetworkProxyConnection) so that they can be written out as individual
                    // Data notifys without merging or splitting.  Consume rate limit after
                    // sending the notify to minimize latency.
                    
                    //Log.v(NetworkProxy.logTag, String(format: "connection id %d has data (chunk is %d bytes)", id, data.length))
                    
                    var params = JSONObject()
                    params["id"] = c.id
                    params["data"] = data.base64EncodedString()
                    try conn.sendNotifySync("Data", params)
                    
                    if let dataWriteLimiter = dataWriteLimiter {
                        try dataWriteLimiter.consumeSync(data.count)  // unencoded size
                    }
                    
                    wrote = true
                } else if c.isClosed {
                    // Connection has no data and is closed, issue NetworkDisconnected
                    // and stop tracking.
                    
                    //Log.v(NetworkProxy.logTag, String(format: "connection id %d has no data and is closed -> send NetworkDisconnected", id))
                    
                    var reason: String! = nil
                    let closedPromise = c.closedPromise
                    do {
                        let error = try closedPromise.wait()
                        reason = error?.localizedDescription
                    } catch {
                        reason = "failed to get reason: " + error.localizedDescription
                    }
                    
                    connections.removeValue(forKey: c.id)
                    
                    var params = JSONObject()
                    params["connection_id"] = c.id
                    if reason != nil {
                        params["reason"] = reason
                    }
                    
                    // Result is ignored for now.  We could maybe retry on error
                    // but there's no known reason for this to fail.
                    _ = try conn.sendRequestAsync("NetworkDisconnected", params)
                    
                    wrote = true
                }
            }

            // XXX: Support for non-idle mode should be improved using a proper
            // blocking rate limiter.  For now, when terminal is non-idle, sleep
            // only a short interval and recheck background data writing between
            // sleeps.

            // If we didn't write data, wait for a trigger future or
            // sanity timeout.
            if !wrote {
                if writeTriggerPromise.isResolved {
                    //Log.v(NetworkProxy.logTag, "refresh network proxy write trigger future")
                    (writeTriggerPromise, writeTriggerResolver) = Promise<Void>.pending()
                }
                //Log.v(NetworkProxy.logTag, "proxy did not write data, wait for trigger")
                do {
                    let timeoutInterval = terminalIsIdle ? NetworkProxy.WRITE_TRIGGER_TIMEOUT_IDLE : NetworkProxy.WRITE_TRIGGER_TIMEOUT_NONIDLE
                    let timeout = after(seconds: timeoutInterval)
                    try race(writeTriggerPromise.asVoid(), timeout.asVoid()).wait()
                } catch {
                    /* No trigger, sanity poll. */
                }
            }
        }
    }
    
    public func throttleJsonRpcData() -> Bool {
        // If underlying JsonRpcConnection write queue is too long (estimated
        // to be several seconds long, making it likely a _Keepalive would
        // fail), throttle writing Data to the JsonRpcConnection.
        let throttleLimit = linkSpeed * 2  // 2 seconds of data (at estimated link speed)
        let queuedBytes = conn.getWriteQueueBytes()
        return queuedBytes >= throttleLimit
    }
    
    public func triggerWriteCheck() {
        writeTriggerResolver.fulfill(())
    }
}
