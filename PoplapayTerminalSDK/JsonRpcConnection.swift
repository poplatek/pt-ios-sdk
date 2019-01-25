//
//  JsonRpcConnection.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 01/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

/*
 *  Run a Poplatek JSONRPC connection on a given, pre-connected InputStream
 *  and OutputStream pair.  Each instance tracks one connection from start to
 *  finish and is not reused.
 *
 *  Connection lifecycle:
 *
 *    - Construct a JsonRpcConnection instance for an already connected
 *      InputStream/OutputStream pair.  Connecting the stream must be done
 *      by the caller.
 *
 *    - Call setXxx() methods to configure keepalive, method dispatcher, rate
 *      limits, etc.  Some parameters, like rate limiter, can also be changed
 *      later on-the-fly.
 *
 *    - Call start() to launch read, write, and keepalive threads, all managed
 *      by this class internally.
 *
 *    - Call waitReady() before sending any requests.  The call will block and
 *      return when the connection is ready for application requests, or throw
 *      an error if e.g. _Sync'ing the connection times out.  Calling self
 *      method is especially important if _Sync is used.  Equivalent method
 *      returning a waitable Promise: getReadyPromise().
 *
 *    - Call sendRequestSync() and sendRequestAsync() to send JSONRPC requests
 *      synchronously (blocks) or asynchronously (returns Promise).  Call
 *      sendNotifySync() to send JSONRPC notifies; the call never waits.
 *
 *    - If necessary, call waitClosed() to wait for the connection to be
 *      closed.  The call blocks and eventually returns or throws, regardless
 *      of what causes the connection to close (including _Sync errors,
 *      _Keepalive timeouts, peer closing the connection, local calls to
 *      close(), etc).  Promise equivalent: getClosedPromise().
 *
 *    - Call close() to initiate a connection close.  The call never blocks,
 *      and eventually the closed future is set (causing waitClosed() to
 *      return).  The connection may also be closed by a keepalive timeout,
 *      remote peer closing the connection, or other external reason.  If
 *      already closed, the close() call is a safe no-op.
 *
 *    - When the connection closes for any reason, all pending futures are
 *      set to the close reason, and all blocking wait calls will exit or
 *      throw.  This closure behavior includes all lifecycle futures (i.e.
 *      started, ready, closing, closed, and their synchronous counterparts
 *      like waitClosed()), and all pending outbound request futures and
 *      their synchronous counterparts like sendRequestSync() calls.  Pending
 *      inbound method calls remain running, but their results will be ignored.
 *
 *   -  The basic idea for connection closure handling is that a caller can
 *      simply wait for e.g. an outbound request to finish (sendRequestSync()),
 *      and be guaranteed that the wait will throw if the underlying connection
 *      closes before the request completes.  There's no need to wait for the
 *      connection closure explicitly in many cases.
 *
 *  Promises and lifecycle:
 *
 *     started  -->  ready  -->  closing  -->  closed
 *
 *  Inbound requests and notifys are dispatched using a JsonRpcDispatcher.
 *  Transport layer methods (_Keepalive, _CloseReason, etc) are handled
 *  internally.
 */

import Foundation
import PromiseKit

public typealias JSONObject = [String: Any]

class JsonRpcTrialParseResult {
    let msg: JSONObject
    let skip: Int
    
    init(msg: JSONObject, skip: Int) {
        self.msg = msg
        self.skip = skip
    }
}

enum Log {
    
    enum Level: Int {
        case verbose = 0
        case debug = 1
        case info = 2
        case warning = 3
        
        var key: String {
            switch self {
            case .verbose:
                return "VERBOSE"
            case .debug:
                return "DEBUG"
            case .info:
                return "INFO"
            case .warning:
                return "WARNING"
            }
        }
    }
    
    static var minimumLevel: Level = .debug
    
    private static func log(_ level: Level, _ tag: String, string: String, value: Any?) {
        guard level.rawValue >= minimumLevel.rawValue else {
            return
        }
        NSLog("\(level.key) \(tag): \(string) \(value ?? "")")
    }
    
    static func v(_ tag: String, _ string: String, _ value: Any? = nil) {
        log(.verbose, tag, string: string, value: value)
    }
    
    static func d(_ tag: String, _ string: String, _ value: Any? = nil) {
        log(.debug, tag, string: string, value: value)
    }
    
    static func i(_ tag: String, _ string: String, _ value: Any? = nil) {
        log(.info, tag, string: string, value: value)
    }
    
    static func w(_ tag: String, _ string: String, _ value: Any? = nil) {
        log(.warning, tag, string: string, value: value)
    }
}

func synchronized(_ lock: Any, closure: () -> ()) {
    objc_sync_enter(lock)
    closure()
    objc_sync_exit(lock)
}

extension Notification.Name {
    
    static let jsonRpcConnectionDidSendMessage = Notification.Name("jsonRpcConnectionDidSendMessage")
    static let jsonRpcConnectionDidReceiveMessage = Notification.Name("jsonRpcConnectionDidReceiveMessage")
}

public protocol RateLimiter {
    func consumeSync(_ count: Int) throws
    func consumeAsync(_ count: Int) throws -> Promise<Void>
}

public class JsonRpcConnection: NSObject {
    
    private static let DEFAULT_KEEPALIVE_IDLE: TimeInterval = 30
    private static let DEFAULT_KEEPALIVE_BUSY: TimeInterval = 5
    private static let KEEPALIVE_REQUEST_TIMEOUT: TimeInterval = 20
    private static let DISCARD_LOOP_DELAY: TimeInterval = 1
    private static let WRITE_LOOP_SANITY_TIMEOUT: TimeInterval = 10
    private static let THREAD_EXIT_TIMEOUT: TimeInterval = 5
    private static let FINAL_THREAD_WAIT_TIMEOUT: TimeInterval = 60
    
    private static let WRITE_CHUNK_SIZE_LIMIT: Int = 256  // with rate limiting
    private static let WRITE_CHUNK_SIZE_NOLIMIT: Int = 65536  // without rate limiting
    
    private static var globalConnectionIdCounter: Int64 = 0
    
    private let logTag: String
    private let connectionId: Int64
    private var requestIdCounter: Int64 = 0
    private let readBufferSize = 262144
    private let maxFrameLength = 262144 - 10  // 10 bytes transport overhead (HHHHHHHH: and newline)
    
    private var readThread: Thread!
    private var writeThread: Thread!
    private var keepaliveThread: Thread!
    
    private let inputStream: InputStream
    private let outputStream: OutputStream
    
    private var writeQueue = [String]()
    
    private var keepaliveEnabled = false
    private var keepaliveIdleInterval: TimeInterval = JsonRpcConnection.DEFAULT_KEEPALIVE_IDLE
    private var keepaliveBusyInterval: TimeInterval = JsonRpcConnection.DEFAULT_KEEPALIVE_BUSY
    
    private var discardEnabled = false
    private var discardTime: TimeInterval = 0
    
    private var syncEnabled = false
    private var syncTimeout: TimeInterval = 0
    
    // Note: close reason may be set before isClosed() is true.
    private(set) var closeReason: Error?
    
    private let startedPromise: Promise<Void>
    private let startedResolver: Resolver<Void>
    
    let readyPromise: Promise<Void>
    private let readyResolver: Resolver<Void>
    
    private let closingPromise: Promise<Error?>
    private let closingResolver: Resolver<Error?>
    
    let closedPromise: Promise<Error?>
    private let closedResolver: Resolver<Error?>
    
    private var writeTriggerPromise: Promise<Void>
    private var writeTriggerResolver: Resolver<Void>
    
    private var keepaliveTriggerPromise: Promise<Void>
    private var keepaliveTriggerResolver: Resolver<Void>
    
    private var pendingKeepaliveResolver: Resolver<JSONObject>?
    private var pendingOutboundResolvers = [String: Resolver<JSONObject>]()
    
    private var pendingInboundRequests = [String: Bool]()
    
    public var dispatcher: JsonRpcDispatcher?
    private let internalDispatcher = JsonRpcDispatcher()
    
    public var writeRateLimiter: RateLimiter? = nil
    
    private var statsStartedTime: TimeInterval = 0
    private var statsReadyTime: TimeInterval = 0  // only set on success
    private var statsClosingTime: TimeInterval = 0
    private var statsClosedTime: TimeInterval = 0
    
    private var statsBytesSent: Int64 = 0
    private var statsBytesReceived: Int64 = 0  // pre-sync discarded bytes not included
    private var statsBoxesSent: Int64 = 0
    private var statsBoxesReceived: Int64 = 0  // _Sync included
    
    private var statsLastTime: TimeInterval = 0
    private let statsLogInterval: TimeInterval = 300
    
    private let statsOutboundRequests = StatsMap()
    private let statsOutboundNotifys = StatsMap()
    private let statsInboundRequests = StatsMap()
    private let statsInboundNotifys = StatsMap()
    
    /*
     *  Public API
     */

    public init(inputStream: InputStream, outputStream: OutputStream) {
        JsonRpcConnection.globalConnectionIdCounter += 1
        self.connectionId = JsonRpcConnection.globalConnectionIdCounter
        self.logTag = String(format: "JsonRpcConnection-%d", self.connectionId)
        
        self.inputStream = inputStream
        self.outputStream = outputStream
        
        (startedPromise, startedResolver) = Promise<Void>.pending()
        (readyPromise, readyResolver) = Promise<Void>.pending()
        (closingPromise, closingResolver) = Promise<Error?>.pending()
        (closedPromise, closedResolver) = Promise<Error?>.pending()
        (writeTriggerPromise, writeTriggerResolver) = Promise<Void>.pending()
        (keepaliveTriggerPromise, keepaliveTriggerResolver) = Promise<Void>.pending()
        
        super.init()
        
        initInternalDispatcher()
    }

    public func setKeepalive() {
        setKeepalive(JsonRpcConnection.DEFAULT_KEEPALIVE_IDLE, JsonRpcConnection.DEFAULT_KEEPALIVE_BUSY)
    }

    public func setKeepalive(_ idleInterval: TimeInterval, _ busyInterval: TimeInterval) {
        Log.d(logTag, String(format: "enable automatic keepalives: idle %f s, busy %f s", idleInterval, busyInterval))
        self.keepaliveEnabled = true
        self.keepaliveIdleInterval = idleInterval
        self.keepaliveBusyInterval = busyInterval
    }
    
    public func setDiscard(_ discardTime: TimeInterval) {
        Log.d(logTag, String(format: "enable automatic data discard: %.3f s", discardTime))
        self.discardEnabled = true
        self.discardTime = discardTime
    }

    public func setSync(_ timeout: TimeInterval) {
        self.syncEnabled = true
        self.syncTimeout = timeout
        Log.d(logTag, String(format: "enable connection _Sync: timeout %.3f s", syncTimeout))
    }
    
    public var isStarted: Bool {
        return startedPromise.isResolved
    }
    
    public func start() {
        startRaw()
    }
    
    public var isReady: Bool {
        return readyPromise.isResolved
    }
    
    public func waitReady() throws {
        try readyPromise.wait()
    }
    
    public var isClosing: Bool {
        return closingPromise.isResolved
    }
    
    public func waitClosing() throws {
        _ = try closingPromise.wait()
    }
    
    public var isClosed: Bool {
        return closedPromise.isResolved
    }
    
    public func waitClosed() throws -> Error? {
        // Close reason is returned as an Exception object.  An exception
        // may be thrown in fatal internal errors only.
        return try closedPromise.wait()
    }
    
    public func close(_ reason: Error? = nil) {
        closeRaw(reason)
    }
    
    public func close(_ reason: String) {
        close(JsonRpcException("CONNECTION_CLOSED", reason, nil, nil))
    }
    
    public func sendRequestAsync(_ method: String, _ params: JSONObject?, _ args: JSONObject? = nil) throws -> Promise<JSONObject> {
        let (_ , promise) = try _sendRequestAsync(method, params, args)
        return promise
    }
    
    private func _sendRequestAsync(_ method: String, _ params: JSONObject?, _ args: JSONObject? = nil) throws -> (String?, Promise<JSONObject>) {
        // nil 'params' is treated the same as an empty JSONObject.

        // If connection is closing or closed, return the error as a Promise
        // rather than an immediate throw, because the caller cannot avoid
        // a race where connection .isClosed() == false, but on a subsequent
        // sendRequestAsync() call the connection is already closed.

        if isClosing || isClosed {
            Log.i(logTag, String(format: "attempted to send request %@ when connection closing/closed, returning error future", method))
            return (nil, Promise<JSONObject>(error: closeReason!))
        }

        let id = getJsonrpcId()
        var msg = JSONObject()
        msg["jsonrpc"] = "2.0"
        msg["method"] = method
        msg["id"] = id
        msg["params"] = params != nil ? params : JSONObject()
        
        let (promise, resolver) = Promise<JSONObject>.pending()
        synchronized (self) {
            pendingOutboundResolvers[id] = resolver
        }
        try writeBox(msg)
        statsOutboundRequests.bump(method)
        
        // If pending requests goes from 0->1, trigger an immediate
        // keepalive check to trigger a new keepalive quickly.
        keepaliveTriggerResolver.fulfill(())
        
        return (id, promise)
    }
    
    public func sendRequestSync(_ method: String, _ params: JSONObject?, _ args: JSONObject? = nil) throws -> JSONObject? {
        let fut = try sendRequestAsync(method, params, args)
        return try fut.wait()
    }
    
    public func sendNotifySync(_ method: String, _ params: JSONObject?, _ args: JSONObject?) throws {
        // nil 'params' is treated the same as an empty JSONObject.
        
        if isClosing || isClosed {
            Log.i(logTag, String(format: "attempted to send notify %@ when connection closing/closed, ignoring", method))
            return
        }
        
        var msg = JSONObject()
        msg["jsonrpc"] = "2.0"
        msg["method"] = method
        msg["params"] = params != nil ? params : JSONObject()
        
        try writeBox(msg)
        statsOutboundNotifys.bump(method)
    }
    
    public func sendNotifySync(_ method: String, _ params: JSONObject?) throws {
        try sendNotifySync(method, params, nil)
    }

    // Get write queue length in bytes.  This is a useful rough estimate
    // for throttling; e.g. network proxy code can stop reading data from
    // internet if the queue is too long.
    public func getWriteQueueBytes() -> Int64 {
        var result: Int64 = 0
        synchronized (self) {
            for s in writeQueue {
                result += Int64(s.count)
            }
        }
        return result
    }

    /*
     *  Misc helpers
     */

    private func setCloseReason(_ exc: Error?) {
        if closeReason != nil {
            //Log.v(logTag, "wanted to set close reason, but already set; ignoring", exc)
        } else if exc == nil {
            Log.i(logTag, "wanted to set close reason, but argument was nil; ignoring")
        } else {
            Log.d(logTag, "setting close reason", exc)
            closeReason = exc
        }
    }

    private func logStats() {
        let now = Date.timeIntervalSinceReferenceDate
        var sb = ""
        sb += "stats:"
        sb += String(format: " startTime=%d, readyTime=%d, closingTime=%d, closeTime=%d", statsStartedTime, statsReadyTime, statsClosingTime, statsClosedTime)
        if statsStartedTime > 0 && statsClosedTime > 0 {
            sb += String(format: " (closed, duration %d seconds)", (statsClosedTime - statsStartedTime) / 1000)
        } else if statsStartedTime > 0 {
            sb += String(format: " (open, duration %d seconds)", (now - statsStartedTime) / 1000)
        }
        sb += String(format: ", bytesOut=%d, boxesOut=%d, bytesIn=%d, boxesIn=%d", statsBytesSent, statsBoxesSent, statsBytesReceived, statsBoxesReceived)
        sb += ", outbound requests: "
        sb += statsOutboundRequests.toString()
        sb += ", outbound notifys: "
        sb += statsOutboundNotifys.toString()
        sb += ", inbound requests: "
        sb += statsInboundRequests.toString()
        sb += ", inbound notifys: "
        sb += statsInboundNotifys.toString()
        
        Log.i(logTag, sb)
    }

    private func checkLogStats() {
        let now = Date.timeIntervalSinceReferenceDate
        if now - statsLastTime >= statsLogInterval {
            statsLastTime = now
            logStats()
        }
    }
    
    private let numberCharRange   = "0".char ... "9".char
    private let lowercaseHexRange = "a".char ... "f".char
    private let uppercaseHexRange = "A".char ... "F".char
    
    private func isHexDigit(_ b: UInt8) -> Bool {
        return numberCharRange.contains(b) || lowercaseHexRange.contains(b) || uppercaseHexRange.contains(b)
    }
    
    private func getJsonrpcId() -> String {
        var id: String = ""
        synchronized (self) {
            requestIdCounter += 1
            id = String(format: "pos-%d-%d", connectionId, requestIdCounter)
        }
        return id
    }
    
    // JSONRPC Transport requires that all messages are pure ASCII, so replace
    // any non-ASCII characters with escapes.  Such characters can only appear
    // inside key or value strings, so string quoting is always appropriate.
    // Use a custom algorithm rather than an external library to minimize
    // dependencies.  Optimize for skipping large sections of ASCII data because
    // non-ASCII is rare in practice.
    private func ensureJsonAscii(_ x: String) -> String {
        let x = x as NSString
        var sb = ""
        var start = 0
        var end = start
        let len = x.length
        
        while (true) {
            // Find maximal safe ASCII range [start,end[.
            end = start
            while end < len {
                let cp = x.character(at: end)
                if cp >= 0x80 {
                    break
                }
                end += 1
            }
            
            // Append pure ASCII [start,end[ (may be zero length).
            sb += x.substring(with: NSMakeRange(start, end - start))
            
            // Deal with a possible non-ASCII character, or finish.
            if end < len {
                sb.append(String(format: "\\u%04x", Int(x.character(at: end)) & 0xffff))
                start = end + 1
            } else {
                break
            }
        }
        
        return sb
    }
    
    private func writeJsonrpcRequest(_ method: String, _ id: String, _ params: JSONObject?) throws {
        var msg = JSONObject()
        msg["jsonrpc"] = "2.0"
        msg["method"] = method
        msg["id"] = id
        msg["params"] = params ?? JSONObject()
        
        try writeBox(msg)
        statsOutboundRequests.bump(method)
    }
    
    private func writeJsonrpcResult(_ method: String, _ id: String, _ result: JSONObject?, prioritized: Bool = false) throws {
        var msg = JSONObject()
        msg["jsonrpc"] = "2.0"
        msg["response_to"] = method
        msg["id"] = id
        msg["result"] = result ?? JSONObject()
        
        try writeBox(msg, prioritized: prioritized)
    }
    
    private func writeJsonrpcError(_ method: String, _ id: String, _ exc: Error?) throws {
        var exc: Error! = exc
        if exc == nil {
            exc = JsonRpcException("UNKNOWN", "unknown error (exc == nil)", nil, nil)
        }
        var msg = JSONObject()
        msg["jsonrpc"] = "2.0"
        msg["response_to"] = method
        msg["id"] = id
        msg["error"] = try JsonRpcException.exceptionToErrorBox(exc)
        
        try writeBox(msg)
    }
    
    private func writeRaw(_ string: String, queued: Bool, prioritized: Bool = false) throws {
        if queued {
            if isClosing || isClosed {
                Log.i(logTag, "tried to writeBox() when connection closing/closed, dropping")
                return
            }
            synchronized (self) {
                if prioritized {
                    writeQueue.insert(string, at: 0)
                } else {
                    writeQueue.append(string)
                }
                writeTriggerResolver.fulfill(())
            }
        } else {
            // Non-queued writes are allowed in closing state.
            if isClosed {
                Log.i(logTag, "tried to writeBox() when connection closed, dropping")
                return
            }
            statsBytesSent += Int64(string.count)
            statsBoxesSent += 1
            Log.i(logTag, String(format: "SEND (non-queued): %@", string))
            
            let encodedDataArray = [UInt8](string.utf8)
            outputStream.write(encodedDataArray, maxLength: encodedDataArray.count)
        }
    }
    
    private func writeBox(_ msg: JSONObject, queued: Bool, prioritized: Bool = false) throws {
        let msgString = ensureJsonAscii(msg.toString())
        let msgLength = msgString.count
        let framed = String(format: "%08x:%@\n", msgLength, msgString)
        try writeRaw(framed, queued: queued, prioritized: prioritized)
        DispatchQueue.main.async {
            NotificationCenter.default.post(name: .jsonRpcConnectionDidSendMessage, object: msg)
        }
    }
    
    private func writeBox(_ msg: JSONObject, prioritized: Bool = false) throws {
        try writeBox(msg, queued: true, prioritized: prioritized)
    }
    
    // Wait promise to complete (with error or success), with timeout.
    // Returns: true=promise was resolved, false=timeout.
    @discardableResult
    private func waitPromiseWithTimeout(_ promise: Promise<Void>, _ timeout: TimeInterval) throws -> Bool {
        let start = Date.timeIntervalSinceReferenceDate
        //Log.v(logTag, String(format: "wait for future with timeout %.3f s", timeout))
        let timeout = after(seconds: timeout)
        try? race(promise.asVoid(), timeout.asVoid()).wait()
        if promise.isResolved {
            if promise.isFulfilled {
                Log.v(logTag, String(format: "promise completed with success after %.3f s", Date.timeIntervalSinceReferenceDate - start))
            } else {
                Log.v(logTag, String(format: "promise completed with error after %.3f s", Date.timeIntervalSinceReferenceDate - start), promise.error)
            }
            return true
        } else {
            Log.v(logTag, String(format: "timeout after %.3f s", Date.timeIntervalSinceReferenceDate - start))
            return false
        }
    }

    // Wait thread to terminate with timeout.  Returns: true if thread
    // terminated within limit, false if not.
    private func waitThreadWithTimeout(_ t: Thread?, _ timeout: TimeInterval) -> Bool {
        guard let t = t else {
            return true
        }
        let deadline = Date(timeIntervalSinceNow: timeout)
        while (true) {
            let remain = deadline.timeIntervalSinceNow
            Log.d(logTag, "waiting for thread to finish, remain: \(remain)")
            if t.isFinished {
                return true
            }
            if remain <= 0 {
                break
            }
            // t.join(remain)
            Thread.sleep(forTimeInterval: 0.25)
        }
        return false
    }

    /*
     *  Transport level methods
     */

    private func initInternalDispatcher() {
        let logTag = self.logTag
        internalDispatcher.registerMethod("_Keepalive", JsonRpcInlineMethodHandler() { params, extras in
            return nil
        })
        internalDispatcher.registerMethod("_Error", JsonRpcInlineMethodHandler() { params, extras in
            Log.w(logTag, "peer sent an _Error notify: " + (params?.toString() ?? ""))
            return nil
        })
        internalDispatcher.registerMethod("_Info", JsonRpcInlineMethodHandler() { params, extras in
            Log.i(logTag, "peer sent an _Info notify: " + (params?.toString() ?? ""))
            return nil
        })
        internalDispatcher.registerMethod("_CloseReason", JsonRpcInlineMethodHandler() { [weak self] params, extras in
            Log.i(logTag, "peer sent a _CloseReason: " + (params?.toString() ?? ""))
            if let reason = params?.dict(for: "error") {
                self?.setCloseReason(JsonRpcException.errorBoxToException(reason))
            } else {
                Log.i(logTag, "received _CloseReason, but 'error' is missing or unacceptable, ignoring")
            }
            return nil
        })
    }

    /*
     *  Close handling
     */
    
    // For Suspend test use, not fully functional.
    public func closeStreamsRaw() {
        Log.i(logTag, "hard closing input and output streams")
        outputStream.close()
        inputStream.close()
    }
    
    private func closeRaw(_ reason: Error?) {
        // Internal 'closeReason' sticks to first local *or* remote close
        // reason.  If close reason comes from peer, it is echoed back in
        // our own close reason.
        var reason: Error! = reason
        if reason == nil {
            reason = JsonRpcException("CONNECTION_CLOSED", "closed by application request", nil, nil)
        }
        setCloseReason(reason)
        
        if isClosing {
            //Log.v(logTag, "trying to close, already closed or closing")
            return
        }
        
        // The close sequence happens in a separate thread so that the caller
        // never blocks.  The caller can wait for closure using waitClosed().
        let t = Thread() { [weak self] in
            //Log.v(logTag, "close thread started")
            self?.runCloseThread(reason)
            //Log.v(logTag, "close thread finished")
        }
        t.start()
    }
    
    private func runCloseThread(_ closeReason: Error) {
        // If connection is not .start()ed, start it now so that we can
        // close the threads always the same way.  The threads will wait
        // for startedPromise and bail out once we set it.
        if !isStarted {
            Log.d(logTag, "close thread: start() not called, call it first")
            start()
        }
        
        startedResolver.reject(closeReason)
        readyResolver.reject(closeReason)
        closingResolver.resolve(closeReason, nil)
        // closedPromise set when close sequence is (mostly) done.
        statsClosingTime = Date.timeIntervalSinceReferenceDate
        Log.i(logTag, "connection closing: " + closeReason.localizedDescription)
        
        // The read thread may be blocked on a stream read without timeout.
        // Close the input stream to force it to exit quickly.
        Log.d(logTag, "closing input stream")
        inputStream.close()
        
        // The keepalive thread is potentially waiting on a trigger future or
        // the last _Keepalive sent which is tracked explicitly.  Set both
        // futures to force the thread to detect 'closing' and finish quickly.
        if keepaliveThread != nil {
            keepaliveTriggerResolver.fulfill(())
        }
        if let pendingKeepaliveResolver = pendingKeepaliveResolver {
            pendingKeepaliveResolver.fulfill(JSONObject())
        }
        
        // The write thread is potentially waiting on a trigger future, set it
        // to force the write thread to detect 'closing' and finish quickly.
        // The write thread writes out the close reason.  Ideally nothing comes
        // after the _CloseReason in the output stream data.  Forcibly close the
        // output stream and drain the write queue if the writer doesn't close
        // cleanly on its own.
        writeTriggerResolver.fulfill(())
        if !waitThreadWithTimeout(writeThread, JsonRpcConnection.THREAD_EXIT_TIMEOUT) {
            Log.w(logTag, "write thread didn't exit within timeout")
        }
        synchronized (self) {
            writeQueue.removeAll()
        }
        Log.d(logTag, "closing output stream")
        outputStream.close()
        
        // Wait for the read loop to complete cleanly.
        if !waitThreadWithTimeout(readThread, JsonRpcConnection.THREAD_EXIT_TIMEOUT) {
            Log.w(logTag, "read thread didn't exit within timeout")
        }
        
        // Wait for the _Keepalive thread to complete cleanly.
        if keepaliveThread != nil {
            if !waitThreadWithTimeout(keepaliveThread, JsonRpcConnection.THREAD_EXIT_TIMEOUT) {
                Log.w(logTag, "keepalive thread didn't exit within timeout")
            }
        }
        
        // Finally, set close reason to closedPromise (all other lifecycle
        // futures were completed when closing started) and pending
        // requests.  Pending inbound requests will remain running, and
        // their results will be ignored.
        //Log.v(logTag, "set pending outbound request futures and closed future to close reason")
        synchronized (self) {
            for (_, resolver) in pendingOutboundResolvers {
                resolver.reject(closeReason)
            }
            pendingOutboundResolvers.removeAll()
            closedResolver.reject(closeReason)

            statsClosedTime = Date.timeIntervalSinceReferenceDate
            logStats()
        }
        Log.i(logTag, "connection closed: " + closeReason.localizedDescription)
        
        // Normally threads are finished now, but if they aren't, track
        // them for a while and log about their status.  This happens
        // after isClosed() is already set so it won't affect the caller.
        let startThreadPollAt = Date()
        while (true) {
            let t = -startThreadPollAt.timeIntervalSinceNow
            let readOk = (readThread == nil || readThread.isFinished)
            let writeOk = (writeThread == nil || writeThread.isFinished)
            let keepaliveOk = (keepaliveThread == nil || keepaliveThread.isFinished)
            if readOk && writeOk && keepaliveOk {
                //Log.v(logTag, "all threads finished")
                break
            } else {
                if t >= JsonRpcConnection.FINAL_THREAD_WAIT_TIMEOUT {
                    Log.w(logTag, String(format: "all threads not yet finished, waited %.3f s, giving up", t))
                    break
                } else {
                    Log.w(logTag, String(format: "all threads not yet finished, waited %.3f s, still waiting", t))
                }
            }
            Thread.sleep(forTimeInterval: 1)
        }
    }
    
    /*
     *  Start handling
     */

    private func startRaw() {
        if isStarted {
            fatalError("already started")
        }
        if readThread != nil || writeThread != nil || keepaliveThread != nil {
            fatalError("readThread, writeThread, or keepaliveThread != nil")
        }
        
        for stream in [inputStream, outputStream] {
            stream.delegate = self
            stream.schedule(in: RunLoop.current, forMode: .default)
            stream.open()
        }
        
        readThread = Thread() { [weak self] in
            guard let self = self else { return }
            //Log.v(logTag, "read thread starting")
            do {
                try self.runReadLoop()
                Log.v(self.logTag, "read thread exited with success")
                self.close(JsonRpcException("CONNECTION_CLOSED", "read loop exited cleanly", nil, nil))
            } catch {
                Log.i(self.logTag, "read thread failed", error)
                self.close(error)
            }
            //Log.v(logTag, "read thread ending")
        }
        readThread.start()
        
        writeThread = Thread() { [weak self] in
            guard let self = self else { return }
            //Log.v(logTag, "write thread starting")
            do {
                try self.runWriteLoop()
                self.close(JsonRpcException("CONNECTION_CLOSED", "write loop exited cleanly", nil, nil))
            } catch {
                Log.i(self.logTag, "write thread failed", error)
                self.close(error)
            }
            
            // Once the write queue has been dealt with, send
            // a _CloseReason and close the socket.
            do {
                var reason: Error! = self.closeReason
                if reason == nil {
                    // Should not happen.
                    Log.w(self.logTag, "close reason is nil when writer thread writing _CloseReason, should not happen")
                    reason = JsonRpcException("CONNECTION_CLOSED", "nil close reason", nil, nil)
                }
                Log.d(self.logTag, "sending _CloseReason", reason)
                
                var params = JSONObject()
                let error = try JsonRpcException.exceptionToErrorBox(reason)
                params["error"] = error
                
                var msg = JSONObject()
                msg["jsonrpc"] = "2.0"
                msg["method"] = "_CloseReason"
                msg["params"] = params
                
                self.statsOutboundNotifys.bump("_CloseReason")
                
                try self.writeBox(msg, queued: false) // direct write, skip queue; allowed also when in closing state
            } catch {
                Log.i(self.logTag, "failed to send _CloseReason", error)
            }
            
            //Log.v(logTag, "write thread ending")
        }
        writeThread.start()

        if keepaliveEnabled {
            keepaliveThread = Thread() { [weak self] in
                guard let self = self else { return }
                //Log.v(logTag, "keepalive thread starting")
                do {
                    try self.runKeepaliveLoop()
                    self.close(JsonRpcException("CONNECTION_CLOSED", "keepalive loop exited cleanly", nil, nil))
                } catch {
                    Log.i(self.logTag, "keepalive thread failed", error)
                    self.close(error)
                }
                //Log.v(logTag, "keepalive thread ending")
            }
            keepaliveThread.start()
        }
    }
    
    /*
     *  Keepalive thread
     */
    
    private func runKeepaliveLoop() throws {
        // Wait for actual start.
        try startedPromise.wait()
        
        // Wait for _Sync completion before starting keepalives.
        try readyPromise.wait()
        
        while (true) {
            if isClosing {
                Log.d(logTag, "connection closing/closed, clean exit for keepalive thread")
                break
            }
            
            // Send a _Keepalive with a fixed, sane timeout.  Track the
            // request so that close() can force a quick exit.  Downcast
            // for Promise is safe because we know the internal type.
            let (id, promise) = try _sendRequestAsync("_Keepalive", nil)
            pendingKeepaliveResolver = pendingOutboundResolvers[id ?? ""]
            let timeout = after(seconds: JsonRpcConnection.KEEPALIVE_REQUEST_TIMEOUT)
            try? race(promise.asVoid(), timeout.asVoid()).wait()
            if !promise.isResolved {
                throw JsonRpcException("KEEPALIVE", "keepalive timeout", nil, nil)
            }
            
            // Wait before sending a new _Keepalive, with interval depending on
            // whether there are pending requests or not.  We can be woken up
            // by a trigger future, which is used when pending requests go from
            // 0->1 and we want to recheck the connection keepalive immediately.
            // It's also used to force a quick, clean exit.
            let interval = pendingOutboundResolvers.isEmpty ? keepaliveIdleInterval : keepaliveBusyInterval
            Log.d(logTag, String(format: "keepalive wait: %d", interval))
            if keepaliveTriggerPromise.isResolved {
                Log.d(logTag, "refresh keepalive trigger future")
                (keepaliveTriggerPromise, keepaliveTriggerResolver) = Promise<Void>.pending()
            }
            if try waitPromiseWithTimeout(keepaliveTriggerPromise, interval) {
                Log.i(logTag, "keepalive triggered explicitly")
            }
        }
    }
    
    /*
     *  Read thread
     */
    
    private func readAndDiscard(_ duration: TimeInterval) throws {
        var tmp = Data()
        let startTime = Date.timeIntervalSinceReferenceDate
        var discardedBytes = 0
        while (true) {
            let now = Date.timeIntervalSinceReferenceDate
            //Log.v(logTag, String(format: "readAndDiscard, %d of %d millis done", now - startTime, durationMillis))
            if isClosing {
                Log.d(logTag, "connection closing/closed, stop readAndDiscard")
                break
            }
            if now - startTime >= duration {
                //Log.v(logTag, "readAndDiscard done")
                break
            }
            let available = inputStream.hasBytesAvailable
            //Log.v(logTag, String(format: "readAndDiscard, available=%d", available))
            if available {
                // If InputStream's .available() returns > 0, we assume
                // that self.read() will never block.  If the stream does
                // not implement .available(), it should return 0.
                let got = inputStream.read(&tmp)
                if got < 0 {
                    throw JsonRpcException("UNKNOWN", "input stream EOF while discarding", nil, nil)
                }
                discardedBytes += got  // not included in stats
            }
            Thread.sleep(forTimeInterval: JsonRpcConnection.DISCARD_LOOP_DELAY)
        }
        Log.i(logTag, String(format: "discarded %d initial bytes in %.3f s", discardedBytes, duration))
    }
    
    // Trial parse a length prefixed (HHHHHHHH:<...>\n) JSONRPC frame from
    // a given buffer with 'buflen' available bytes, starting at offset 'base'.
    // There are three possible outcomes:
    //   1. JsonRpcTrialParseResult: valid, complete frame.
    //   2. nil: possibly valid frame, but the frame is not complete.
    //   3. throw: invalid frame.
    private func trialParseJsonRpcFrame(_ buf: String, _ base: Int, _ buflen: Int) throws -> JsonRpcTrialParseResult? {
        let avail = buflen - base
        
        if avail < 9 {
            return nil
        }
        
        var len: Int
        let lenString = buf.substring(from: base, length: 8)
        if let lenTmp = Int(lenString, radix: 16) {
            if lenTmp < 0 {
                throw JsonRpcParseErrorException(String(format: "framing error: length is negative: %d", lenTmp))
            } else if lenTmp > maxFrameLength {
                throw JsonRpcParseErrorException(String(format: "framing error: frame too long: %d", lenTmp))
            }
            len = lenTmp
        } else {
            throw JsonRpcParseErrorException("framing error: cannot parse length")
        }
        
        if buf.char(at: base + 8) != Character(":") {
            throw JsonRpcParseErrorException("framing error: expected colon after length")
        }
        
        if avail < len + 10 {
            // No full frame received yet, continue later.  We could have a timeout for
            // incomplete frames, but there's no need because _Keepalive monitoring will
            // catch a never-completing frame automatically.
            //Log.v(logTag, "frame incomplete, continue later")
            return nil
        }
        
        if buf.char(at: base + len + 9) != Character("\n") {
            throw JsonRpcParseErrorException("framing error: expected newline at end of message")
        }
        
        // Full frame exists in the buffer, try to parse it.
        //Log.v(logTag, String(format: "parsing complete frame of %d bytes", len))
        let jsonStr = buf.substring(from: base + 9, length: len)
        let logStr = buf.substring(from: base, length: len + 9)  // omit newline
        Log.i(logTag, String(format: "RECV: %@", logStr))
        guard let jsonData = jsonStr.data(using: .utf8),
              let jsonObject = try? JSONSerialization.jsonObject(with: jsonData, options: []),
              let msg = jsonObject as? JSONObject else {
                throw JsonRpcParseErrorException("framing error: failed to parse JSON")
        }
        DispatchQueue.main.async {
            NotificationCenter.default.post(name: .jsonRpcConnectionDidReceiveMessage, object: msg)
        }
        return JsonRpcTrialParseResult(msg: msg, skip: len + 10)
    }
    
    private func runReadLoop() throws {
        var scanningSync = false
        var syncStartTime: TimeInterval = -1
        
        // Wait for actual start.
        try startedPromise.wait()
        
        // Automatic discarding of input data is useful for RFCOMM.
        if discardEnabled {
            Log.i(logTag, String(format: "read and discard inbound data for %.3f s", discardTime))
            try readAndDiscard(discardTime)
        }
        
        // For Bluetooth RFCOMM, send a unique _Sync and hunt for a response
        // for a limited amount of time.  _Sync filler is for improving SPm20
        // RFCOMM resume behavior; SPm20 hardware loses a few bytes and may
        // corrupt a few more after a resume.
        var syncId: String! = nil
        if syncEnabled {
            Log.i(logTag, String(format: "send _Sync, hunt for response within %.3f s", syncTimeout))
            let syncFiller = "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
            try writeRaw(syncFiller, queued: true)
            Thread.sleep(forTimeInterval: 0.1)
            syncId = getJsonrpcId()
            try writeJsonrpcRequest("_Sync", syncId, JSONObject())
            scanningSync = true
            syncStartTime = Date.timeIntervalSinceReferenceDate
        } else {
            statsReadyTime = Date.timeIntervalSinceReferenceDate
            readyResolver.fulfill(())
        }
        
        var buf = Data()
        var off: Int = 0
        
        // Main read loop.
        while (true) {
            // If closing, exit cleanly.
            if isClosing {
                Log.d(logTag, "connection closing/closed, clean exit for read thread")
                break
            }
            
            // Check for sync timeout.
            if scanningSync {
                let t = Date.timeIntervalSinceReferenceDate - syncStartTime
                Log.d(logTag, String(format: "waiting for _Sync response, waited %.3f s so far", t))
                if t >= syncTimeout {
                    throw JsonRpcException("SYNC_FAILED", "timeout scanning for _Sync response", nil, nil)
                }
            }
            
            // Check for buffer space: we don't want to be stuck waiting for
            // a frame with no space to complete it.
            if off < 0 || off > readBufferSize {
                throw JsonRpcInternalErrorException("internal error, invalid offset")
            }
            let space = readBufferSize - off
            if space <= 0 {
                // Frame not complete and no space to complete it.
                throw JsonRpcParseErrorException("framing error: read buffer full, cannot parse message")
            }
            if space > readBufferSize {
                throw JsonRpcInternalErrorException("internal error, invalid space")
            }
            
            // Blocking read() for more data.  If stream is broken, this will
            // (eventually) throw.  In scan mode don't block so we can time out
            // during scan.
            if scanningSync {
                let available = inputStream.hasBytesAvailable  // See comments in readAndDiscard()
                Log.v(logTag, String(format: "reading in sync mode, off=%d, space=%d, available=%d", off, space, available))
                if available {
                    let got = inputStream.read(&buf, space)
                    Log.v(logTag, String(format: "read returned %d", got))
                    if got < 0 {
                        throw JsonRpcException("SYNC_FAILED", "input stream EOF while waiting for _Sync response", nil, nil)
                    }
                    if got > space {
                        throw JsonRpcInternalErrorException("internal error, read() return value invalid")
                    }
                    statsBytesReceived += Int64(got)
                    off += got
                } else {
                    Log.v(logTag, "no available data, sleep and retry")
                    Thread.sleep(forTimeInterval: 0.25)
                }
            } else {
                //Log.v(logTag, String(format: "reading, off=%d, space=%d", off, space))
                let got = inputStream.read(&buf, space)
                //Log.v(logTag, String(format: "read returned %d", got))
                if got < 0 {
                    Log.d(logTag, "input stream EOF")
                    return
                }
                if got > space {
                    throw JsonRpcInternalErrorException("internal error, read() return value invalid")
                }
                statsBytesReceived += Int64(got)
                off += got
                if got == 0 {
                    Thread.sleep(forTimeInterval: 0.25)
                }
            }
            
            // For _Sync mode, trial parse at every offset until we find a _Sync
            // reply.  Ignore any errors.  Note that we must scan every available
            // position because there may be a huge pending frame before a _Sync
            // reply (e.g. "12345678:..." which never completes) and we must not
            // get stuck in that pending frame.
            if scanningSync {
                for base in 0 ..< off {
                    // Quick pre-check: skip silently unless at least length appears valid.
                    // This is not strictly necessary except to reduce log output from
                    // trial parse attempts.
                    let avail = off - base
                    if avail < 9 {
                        break
                    }
                    let prefixValid = isHexDigit(buf[base + 0]) && isHexDigit(buf[base + 1]) &&
                                      isHexDigit(buf[base + 2]) && isHexDigit(buf[base + 3]) &&
                                      isHexDigit(buf[base + 4]) && isHexDigit(buf[base + 5]) &&
                                      isHexDigit(buf[base + 6]) && isHexDigit(buf[base + 7]) &&
                                      buf[base + 8] == ":".char
                    if !prefixValid {
                        continue
                    }
                    do {
                        Log.i(logTag, String(format: "trial parsing at base offset %d/%d", base, off))
                        
                        if let bufString = String(data: buf, encoding: .utf8),
                           let trialParseResult = try trialParseJsonRpcFrame(bufString, base, off) {
                            let msg = trialParseResult.msg
                            if (msg.string(for: "jsonrpc") == "2.0" &&
                                msg.string(for: "id") == syncId &&
                                msg.dict(for: "result") != nil) {
                                Log.i(logTag, String(format: "got _Sync response at offset %d, moving to non-sync mode", base))
                                scanningSync = false
                                statsReadyTime = Date.timeIntervalSinceReferenceDate
                                readyResolver.fulfill(())
                                
                                buf = buf.slice(from: base + trialParseResult.skip, to: off)!
                                off = off - (base + trialParseResult.skip)
                                break
                            }
                        }
                    } catch {
                        Log.i(logTag, String(format: "error parsing _Sync reply at offset %d, ignoring)", base), error)
                    }
                }
            }
            
            // For non-_Sync mode, trial parse all completed frames.  Note that a
            // single read() may complete more than one frame and we must handle
            // them all before issuing another read() which may block indefinitely.
            if !scanningSync {
                while (true) {
                    //Log.v(logTag, String(format: "read loop, off=%d", off))
                    
                    let base = 0
                    guard let bufString = String(data: buf, encoding: .utf8),
                          let trialParseResult = try trialParseJsonRpcFrame(bufString, base, off) else {
                        // Partial frame, keep reading.
                        break
                    }
                    buf = buf.slice(from: base + trialParseResult.skip, to: off)!
                    off = off - (base + trialParseResult.skip)
                    let msg = trialParseResult.msg
                    
                    // Successfully parsed a framed message, handle it.  If the handler
                    // throws, assume it's an internal error and drop the transport connection.
                    // Message processing must catch any expected errors (such as a user
                    // callback throwing).
                    //Log.v(logTag, "processing parsed message")
                    do {
                        statsBoxesReceived += 1
                        try processBox(msg)
                    } catch {
                        Log.d(logTag, "failed to process incoming frame", error)
                        throw error
                    }
                }
            }
        }
    }
    
    /*
     *  Inbound message handling and method dispatch
     */
    
    private func processBox(_ msg: JSONObject) throws {
        var tmp: Any!
        var method: String! = nil
        var id: String! = nil
        var params: JSONObject! = nil
        var result: JSONObject! = nil
        var error: JSONObject! = nil
        
        // Message field type and presence check.  Some of the constraints
        // in the Poplatek JSONRPC transport are stricter than in JSONRPC 2.0
        // for example, params/result/error values are required to be objects
        // (not e.g. arrays) and 'id' fields are required to be strings.
        
        tmp = msg["jsonrpc"]
        if tmp == nil {
            throw JsonRpcInvalidRequestException("inbound message missing 'jsonrpc'")
        }
        if !(tmp is String) {
            throw JsonRpcInvalidRequestException("inbound message 'jsonrpc' is not a string")
        }
        if (tmp as? String) != "2.0" {
            throw JsonRpcInvalidRequestException("inbound message 'jsonrpc' is not '2.0'")
        }
        
        tmp = msg["method"]
        if tmp != nil {
            if let string = tmp as? String {
                method = string
            } else {
                throw JsonRpcInvalidRequestException("inbound message 'method' is not a string")
            }
        }
        
        tmp = msg["id"]
        if tmp != nil {
            if let string = tmp as? String {
                id = string
            } else {
                throw JsonRpcInvalidRequestException("inbound message 'id' is not a string")
            }
        }
        
        tmp = msg["params"]
        if tmp != nil {
            if let object = tmp as? JSONObject {
                params = object
            } else {
                throw JsonRpcInvalidRequestException("inbound message 'params' is not an object")
            }
        }
        
        tmp = msg["result"]
        if tmp != nil {
            if let object = tmp as? JSONObject {
                result = object
            } else {
                throw JsonRpcInvalidRequestException("inbound message 'result' is not an object")
            }
        }
        
        tmp = msg["error"]
        if tmp != nil {
            if let object = tmp as? JSONObject {
                error = object
            } else {
                throw JsonRpcInvalidRequestException("inbound message 'error' is not an object")
            }
        }
        
        if params != nil {
            if method == nil {
                throw JsonRpcInvalidRequestException("inbound message has 'params' but no 'method'")
            }
            if result != nil {
                throw JsonRpcInvalidRequestException("inbound message has both 'params' and 'result'")
            }
            if error != nil {
                throw JsonRpcInvalidRequestException("inbound message has both 'params' and 'error'")
            }
            
            // If an inbound method is already running with the requested ID,
            // drop transport because request/reply guarantees can no longer
            // be met.
            if id != nil && pendingInboundRequests[id] != nil {
                Log.w(logTag, "inbound request 'id' matches an already running inbound request, fatal transport error")
                throw JsonRpcInvalidRequestException("inbound request 'id' matches an already running inbound request")
            }
            
            if id != nil {
                statsInboundRequests.bump(method)
            } else {
                statsInboundNotifys.bump(method)
            }
            
            // Inbound method or notify dispatch.  Use internal dispatcher for
            // transport level methods, otherwise refer to external dispatcher.
            // Inline _Keepalive handling to make keepalives as prompt as
            // possible.
            if method == "_Keepalive" {
                if let id = id {
                    try writeJsonrpcResult(method, id, JSONObject(), prioritized: true)
                }
                return
            }
            let disp: JsonRpcDispatcher! = internalDispatcher.hasMethod(method) ? internalDispatcher : dispatcher
            if disp == nil || !disp.hasMethod(method) {
                if let id = id {
                    Log.i(logTag, String(format: "unhandled method %@, sending error", method))
                    try writeJsonrpcError(method, id, JsonRpcMethodNotFoundException(String(format: "method %@ not supported", method)))
                } else {
                    Log.i(logTag, String(format: "unhandled notify %@, ignoring", method))
                }
                return
            }
            
            let extras = JsonRpcMethodExtras()
            extras.method = method
            extras.id = id
            extras.message = msg
            extras.connection = self
            let handler = disp.getHandler(method)
            try dispatchMethodWithHandler(method, id, params, extras, handler)
        } else if result != nil {
            if params != nil {
                // Cannot actually happen, as 'params' was checked above.
                throw JsonRpcInvalidRequestException("inbound message has both 'result' and 'params'")
            }
            if error != nil {
                throw JsonRpcInvalidRequestException("inbound message has both 'result' and 'error'")
            }
            if id == nil {
                throw JsonRpcInvalidRequestException("inbound message has 'result' but no 'id'")
            }
            
            // Inbound success result dispatch.
            synchronized (self) {
                if let resolver = pendingOutboundResolvers[id] {
                    pendingOutboundResolvers.removeValue(forKey: id)
                    resolver.fulfill(result)
                } else {
                    Log.w(logTag, String(format: "unexpected jsonrpc result message, id %@, ignoring", id))
                }
            }
        } else if error != nil {
            if params != nil {
                // Cannot actually happen, as 'params' was checked above.
                throw JsonRpcInvalidRequestException("inbound message has both 'error' and 'params'")
            }
            if result != nil {
                // Cannot actually happen, as 'result' was checked above.
                throw JsonRpcInvalidRequestException("inbound message has both 'error' and 'result'")
            }
            if id == nil {
                throw JsonRpcInvalidRequestException("inbound message has 'error' but no 'id'")
            }
            
            // Inbound error result dispatch.
            synchronized (self) {
                if let resolver = pendingOutboundResolvers[id] {
                    let exc = JsonRpcException.errorBoxToException(error)
                    pendingOutboundResolvers.removeValue(forKey: id)
                    resolver.reject(exc)
                } else {
                    Log.w(logTag, String(format: "unexpected jsonrpc error message, id %@, ignoring", id))
                }
            }
        } else {
            throw JsonRpcInvalidRequestException("inbound message does not have 'params', 'result', or 'error'")
        }
    }
    
    // Dispatch inbound request based on the specific handler subtype.
    private func dispatchMethodWithHandler(_ method: String, _ id: String?, _ params: JSONObject?, _ extras: JsonRpcMethodExtras?, _ handler: JsonRpcMethodHandler?) throws {
        if handler == nil {
            let e = JsonRpcMethodNotFoundException(String(format: "no handler for method %@", method))
            Log.i(logTag, e.message, e)
            if let id = id {
                do {
                    try writeJsonrpcError(method, id, e)
                } catch {
                    close(error)
                }
            }
        } else if let handler = handler as? JsonRpcInlineMethodHandler {
            do {
                let res = try handler.handle(params, extras)
                if let id = id {
                    do {
                        try writeJsonrpcResult(method, id, res)
                    } catch {
                        close(error)
                    }
                }
            } catch {
                Log.i(logTag, String(format: "inline handler for method %@ failed", method), error)
                if let id = id {
                    do {
                        try writeJsonrpcError(method, id, error)
                    } catch {
                        close(error)
                    }
                }
            }
        } else if let handler = handler as? JsonRpcThreadMethodHandler {
            let t = Thread() { [weak self] in
                guard let self = self else { return }
                do {
                    if let id = id {
                        self.pendingInboundRequests[id] = true
                    }
                    let res = try handler.handle(params, extras)
                    if let id = id {
                        self.pendingInboundRequests.removeValue(forKey: id)
                        do {
                            try self.writeJsonrpcResult(method, id, res)
                        } catch {
                            self.close(error)
                        }
                    }
                } catch {
                    Log.i(self.logTag, String(format: "thread handler for method %@ failed", method), error)
                    if let id = id {
                        self.pendingInboundRequests.removeValue(forKey: id)
                        do {
                            try self.writeJsonrpcError(method, id, error)
                        } catch {
                            self.close(error)
                        }
                    }
                }
            }
            t.start()
        } else if let handler = handler as? JsonRpcPromiseMethodHandler {
            // Asynchronous result, need to wait for completion.
            // For now just launch a Thread to wait for each Promise
            // individually.  For specific Promise subtypes (like
            // CompletablePromise) we could afunc a Thread launch.
            let tmpFut: Promise<JSONObject>
            do {
                tmpFut = try handler.handle(params, extras)
            } catch {
                Log.i(logTag, String(format: "future handler for method %@ failed", method), error)
                if let id = id {
                    do {
                        try writeJsonrpcError(method, id, error)
                    } catch {
                        close(error)
                    }
                }
                return
            }
            guard let id = id else {
                return
            }
            pendingInboundRequests[id] = true
            
            let resFut = tmpFut
            let t = Thread() { [weak self] in
                guard let self = self else { return }
                do {
                    let res = try resFut.wait()
                    do {
                        self.pendingInboundRequests.removeValue(forKey: id)
                        try self.writeJsonrpcResult(method, id, res)
                    } catch {
                        self.close(error)
                    }
                } catch {
                    Log.i(self.logTag, String(format: "future handler for method %@ failed", method), error)
                    do {
                        self.pendingInboundRequests.removeValue(forKey: id)
                        try self.writeJsonrpcError(method, id, error)
                    } catch {
                        self.close(error)
                    }
                }
            }
            t.start()
        } else {
            throw JsonRpcInternalErrorException("invalid method handler subtype")
        }
    }
    
    /*
     *  Write thread
     */
    
    private func runWriteLoop() throws {
        // Wait for actual start.
        try startedPromise.wait()
        
        while (true) {
            var string: String! = nil
            
            checkLogStats()
            
            var shouldBreak = false
            synchronized (self) {
                if isClosing {
                    Log.d(logTag, "connection closing/closed and no more pending writes, write loop exiting cleanly")
                    shouldBreak = true
                }
                
                string = writeQueue.count > 0 ? writeQueue.removeFirst() : nil
                if writeTriggerPromise.isResolved {
                    //Log.v(logTag, "refresh write trigger future")
                    (writeTriggerPromise, writeTriggerResolver) = Promise<Void>.pending()
                }
            }
            
            if shouldBreak {
                return
            }
            
            if string == nil {
                // No frame in queue, wait until trigger or sanity poll.
                try waitPromiseWithTimeout(writeTriggerPromise, JsonRpcConnection.WRITE_LOOP_SANITY_TIMEOUT)
            } else {
                // When rate limiting enabled, write and consume in small
                // pieces to handle large messages reasonably for RFCOMM.
                Log.i(logTag, String(format: "SEND: %@", string ?? ""))
                let writeChunkSize = writeRateLimiter != nil ? JsonRpcConnection.WRITE_CHUNK_SIZE_LIMIT : JsonRpcConnection.WRITE_CHUNK_SIZE_NOLIMIT
                var off: Int = 0
                let data = [UInt8](string.utf8)
                while off < data.count {
                    let left = data.count - off
                    let now = min(left, writeChunkSize)
                    Log.v(logTag, String(format: "writing %d (range [%d,%d[) of %d bytes", now, off, off + now, data.count))
                    if let writeRateLimiter = writeRateLimiter {
                        try writeRateLimiter.consumeSync(now)
                    }
                    statsBytesSent += Int64(now)
                    while !outputStream.hasSpaceAvailable {
                        Thread.sleep(forTimeInterval: 0.1)
                    }
                    outputStream.write([UInt8](data[off ..< (off + now)]), maxLength: now)
                    off += now
                }
                statsBoxesSent += 1
            }
        }
    }
}

extension JsonRpcConnection: StreamDelegate {
    
    private func checkIfStarted() {
        if inputStream.isOpen, outputStream.isOpen, outputStream.hasSpaceAvailable, startedPromise.isPending {
            Log.i(logTag, "connection starting")
            startedResolver.fulfill(())
            statsStartedTime = Date.timeIntervalSinceReferenceDate
        }
    }
    
    public func stream(_ stream: Stream, handle eventCode: Stream.Event) {
        
        Log.v(logTag, "\(type(of: stream)) (status: \(stream.streamStatus.identifier)) got event: \(eventCode.identifier)")
        
        switch eventCode {
        case .openCompleted:
            checkIfStarted()
        case .hasBytesAvailable:
            checkIfStarted()
            // Could replace read loop with reads triggered from here
        case .hasSpaceAvailable:
            checkIfStarted()
            // Could replace write loop with writes triggered from here (and when adding to queue, if space is available)
        default:
            break
        }
    }
}

/*
 *  Additional information for inbound method handling.
 *
 *  Collected into a holder object to keep interface signatures clean, and to
 *  allow easier addition and removal of extra fields without breaking call
 *  sites.
 */

public class JsonRpcMethodExtras {
    var method: String!
    var id: String!
    var message: JSONObject!
    var connection: JsonRpcConnection!
}

/*
 *  String-to-counter map for stats collection.
 */

public class StatsMap {
    private var map = [String: Int64]()
    
    public func bump(_ key: String) {
        map[key] = (map[key] ?? 0) + 1
    }
    
    public func toString() -> String {
        var sb = "{"
        let keys = map.keys.sorted()
        var first = true
        for key in keys {
            if first {
                first = false
                sb += " "
            } else {
                sb += ", "
            }
            sb += String(format: "%@:%d", key, map[key] ?? 0)
        }
        if !first {
            sb += " "
        }
        sb += "}"
        return sb
    }
}

extension Data {
    
    func slice(from: Int, to: Int) -> Data? {
        guard let string = String(data: self, encoding: .utf8) as NSString?, string.length >= to else {
            return nil
        }
        return string.substring(with: NSMakeRange(from, to - from)).data(using: .utf8)
    }
}

extension Date {
    
    var timeIntervalUntilNow: TimeInterval {
        return -timeIntervalSinceNow
    }
}

extension Dictionary where Key == String, Value == Any {
    
    func int(for key: String) -> Int? {
        return self[key] as? Int
    }
    
    func int64(for key: String) -> Int64? {
        return self[key] as? Int64
    }
    
    func string(for key: String) -> String? {
        return self[key] as? String
    }
    
    func dict(for key: String) -> JSONObject? {
        return self[key] as? JSONObject
    }
    
    func toString() -> String {
        if let data = try? JSONSerialization.data(withJSONObject: self, options: []),
           let string = String(data: data, encoding: .utf8) {
            return string
        } else {
            return String(describing: self)
        }
    }
}

extension InputStream {
    
    func read(_ data: inout Data, _ bufferSize: Int = 1024) -> Int {
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        let readCount = read(buffer, maxLength: bufferSize)
        if readCount > 0 {
            data.append(buffer, count: readCount)
        }
        buffer.deallocate()
        return readCount
    }
}

extension String {
    
    var char: UInt8 {
        return UInt8(unicodeScalars.first!.value)
    }
    
    func replacingCharacters(from characterSet: CharacterSet, with replacementString: String = "") -> String {
        return components(separatedBy: characterSet).joined(separator: replacementString)
    }
    
    func trimmed() -> String {
        return trimmingCharacters(in: .whitespaces)
    }
    
    func char(at index: Int) -> Character? {
        guard index >= 0, index < count else { return nil }
        return self[self.index(self.startIndex, offsetBy: index)]
    }
    
    func substring(from: Int, length: Int) -> String {
        return (self as NSString).substring(with: NSMakeRange(from, length))
    }
}

extension Stream {
    
    var isOpen: Bool {
        switch streamStatus {
        case .open, .reading, .writing:
            return true
        case .notOpen, .opening, .atEnd, .closed, .error:
            return false
        }
    }
}

extension Stream.Status {
    
    var identifier: String {
        switch self {
        case .notOpen:
            return "notOpen"
        case .opening:
            return "opening"
        case .open:
            return "open"
        case .reading:
            return "reading"
        case .writing:
            return "writing"
        case .atEnd:
            return "atEnd"
        case .closed:
            return "closed"
        case .error:
            return "error"
        }
    }
}

extension Stream.Event {
    
    var identifier: String {
        switch self {
        case .openCompleted:
            return "openCompleted"
        case .hasBytesAvailable:
            return "hasBytesAvailable"
        case .hasSpaceAvailable:
            return "hasSpaceAvailable"
        case .errorOccurred:
            return "errorOccurred"
        case .endEncountered:
            return "endEncountered"
        default:
            return "\(rawValue)"
        }
    }
}
