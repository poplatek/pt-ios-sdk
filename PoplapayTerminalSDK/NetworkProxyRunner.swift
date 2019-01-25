//
//  NetworkProxyRunner.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 05/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import Foundation
import ExternalAccessory

/*
 *  Run a persistent JSONPOS network proxy over Bluetooth RFCOMM to a given,
 *  already paired MAC address or automatic detection of available device.
 */

public typealias DebugStatusCallback = (_ text: String) throws -> Void
public typealias TerminalInfoCallback = (_ conn: JsonRpcConnection?, _ terminalInfo: JSONObject?) throws -> Void
public typealias StatusCallback = (_ conn: JsonRpcConnection?, _ status: JSONObject?) throws -> Void
public typealias ConnectionStateCallback = (_ conn: JsonRpcConnection?, _ state: NetworkProxyRunner.ConnectionState) throws -> Void

public class NetworkProxyRunner: NSObject {
    
    let sdkVersion = "0.1"
    
    public enum ConnectionState {
        case DISCONNECTED, CONNECTING, CONNECTED
    }
    
    private let logTag = "BluetoothProxy"
    
    // Connection retry schedule is important for reliability.  For small
    // connection drops the retry can be quick, but there must be a backoff
    // to at least 10 seconds to allow SPm20 Bluetooth init to succeed when
    // the terminal restarts.
    private static let RETRY_SLEEP_MIN: TimeInterval = 1
    private static let RETRY_SLEEP_MAX: TimeInterval = 10
    private static let RETRY_SLEEP_STEP: TimeInterval = 2
    private static let RFCOMM_DISCARD_TIME: TimeInterval = 2
    private static let SYNC_TIMEOUT: TimeInterval = 5
    
    private static let SPM20_LINK_SPEED: Int = 10 * 1024  // Default without .link_speed
    private static let MIN_LINK_SPEED: Int = 10 * 1024
    
    let protocolString: String
    
    var debugStatusCallback: DebugStatusCallback?
    var terminalInfoCallback: TerminalInfoCallback?
    var statusCallback: StatusCallback?
    var connectionStateCallback: ConnectionStateCallback?
    
    var currentTerminalIdle = true
    
    var currentConnection: JsonRpcConnection!
    var currentProxy: NetworkProxy!
    
    var stopReason: Error? = nil
    
    private var failCount = 0
    
    @objc public convenience override init() {
        self.init(protocolString: "com.thyron")
    }
    
    @objc public init(protocolString: String) {
        self.protocolString = protocolString
    }
    
    private func updateDebugStatus(_ text: String) {
        do {
            try debugStatusCallback?(text)
        } catch {
            Log.d(logTag, "failed to update debug status, ignoring", error)
        }
    }
    
    private func updateTerminalInfo(_ conn: JsonRpcConnection, _ terminalInfo: JSONObject) {
        do {
            try terminalInfoCallback?(conn, terminalInfo)
        } catch {
            Log.d(logTag, "failed to update terminal info, ignoring", error)
        }
    }
    
    private func updateStatus(_ conn: JsonRpcConnection?, _ status: JSONObject?) {
        do {
            try statusCallback?(conn, status)
        } catch {
            Log.d(logTag, "failed to update status, ignoring", error)
        }
    }
    
    private func updateConnectionState(_ conn: JsonRpcConnection?, _ state: ConnectionState) {
        do {
            try connectionStateCallback?(conn, state)
        } catch {
            Log.d(logTag, "failed to update connection state, ignoring", error)
        }
    }
    
    private func closeCurrentJsonRpcConnection(_ reason: Error?) {
        currentConnection?.close(reason)
    }
    
    @objc public func start() {
        Thread() {
            self.runProxyLoop()
        }.start()
    }
    
    @objc public func stop() {
        stopProxyLoop(nil)
    }
    
    private func runProxyLoop() {
        while (true) {
            closeCurrentJsonRpcConnection(RuntimeException ("closing just in case"))
            
            if stopReason != nil {
                break
            }
            
            currentTerminalIdle = true
            currentConnection = nil
            currentProxy = nil
            
            updateConnectionState(nil, ConnectionState.DISCONNECTED)
            
            var inputStream: InputStream!
            var outputStream: OutputStream!
            
            let manager = EAAccessoryManager.shared()
            for accessory in manager.connectedAccessories {
                if accessory.protocolStrings.contains(protocolString) {
                    if let session = EASession(accessory: accessory,
                                               forProtocol: protocolString) {
                        inputStream = session.inputStream
                        outputStream = session.outputStream
                        if inputStream != nil, outputStream != nil {
                            break
                        }
                    }
                }
            }
            
            if inputStream != nil, outputStream != nil {
                do {
                    updateConnectionState(nil, ConnectionState.CONNECTING)
                    try runProxy(inputStream, outputStream)
                } catch {
                    updateDebugStatus("FAILED: \(error.localizedDescription)")
                    Log.i(logTag, "bluetooth connect failed, sleep and retry", error)
                    failCount += 1
                }
            } else {
                Log.i(logTag, "Failed to set up streams")
                failCount += 1
            }
            
            let retrySleep = min(NetworkProxyRunner.RETRY_SLEEP_MIN + NetworkProxyRunner.RETRY_SLEEP_STEP * Double(failCount), NetworkProxyRunner.RETRY_SLEEP_MAX)
            Log.i(logTag, String(format: "sleep %.3f s", retrySleep))
            Thread.sleep(forTimeInterval: retrySleep)
        }
        
        closeCurrentJsonRpcConnection(stopReason)
        
        Log.i(logTag, "proxy loop stop reason set, stopping:", stopReason)
    }
    
    // Request proxy loop to stop.  Returns without waiting for the stop
    // to complete at present.
    private func stopProxyLoop(_ reason: Error?) {
        var reason = reason
        if stopReason != nil {
            return
        }
        if reason == nil {
            reason = RuntimeException("proxy stop requested by caller")
        }
        Log.i(logTag, "setting proxy stop reason:", reason)
        stopReason = reason
        closeCurrentJsonRpcConnection(reason)
    }
    
    // A simple, soft idle vs. non-idle heuristic based on the
    // .transaction_status field of Status.  This allows data rate
    // limiting to reduce background traffic during Purchase
    // processing.
    //
    // This is not ideal as only operations involving .transaction_status
    // are considered non-idle.  For example, DisplayScreen does not cause
    // a non-idle status to be detected.  Promise terminal versions are
    // likely to indicate idle vs. non-idle state as an explicit field so
    // that this can be made more accurate.
    private func checkIdleStateChange(_ status: JSONObject?) {
        let isIdle = status?.string(for: "transaction_status") == nil
        if currentTerminalIdle {
            if !isIdle {
                Log.i(logTag, "terminal went from idle to non-idle")
                if currentProxy != nil {
                    currentProxy.setTerminalIdleState(isIdle)
                }
            }
        } else {
            if isIdle {
                Log.i(logTag, "terminal went from non-idle to idle")
                if currentProxy != nil {
                    currentProxy.setTerminalIdleState(isIdle)
                }
            }
        }
        currentTerminalIdle = isIdle
    }
    
    private func handleJsonposStatusUpdate(_ status: JSONObject?) {
        Log.i(logTag, "STATUS: " + (status?.toString() ?? ""))
        updateDebugStatus("Status: " + (status?.toString() ?? ""))
        checkIdleStateChange(status)
    }
    
    private func handleJsonposStatusUpdate(_ conn: JsonRpcConnection?, _ status: JSONObject?) {
        Log.i(logTag, "STATUS: " + (status?.toString() ?? ""))
        updateDebugStatus("Status: " + (status?.toString() ?? ""))
        updateStatus(conn, status)
    }
    
    // Status poller for older terminals with no StatusEvent support.
    private func runStatusPoller(_ conn: JsonRpcConnection) throws {
        let logTag = self.logTag
        Thread() {
            while (true) {
                if conn.isClosed {
                    break
                }
                do {
                    let params = JSONObject()
                    let status = try conn.sendRequestSync("Status", params)
                    self.handleJsonposStatusUpdate(conn, status)
                } catch {
                    Log.i(logTag, "Status failed, ignoring", error)
                }
                Thread.sleep(forTimeInterval: 5)
            }
            Log.i(logTag, "status poller exiting")
        }.start()
    }
    
    private func runProxy(_ inputStream: InputStream, _ outputStream: OutputStream) throws {
        Log.i(logTag, "launch jsonrpc connection")
        
        // Method dispatcher for connection.
        let dispatcher = JsonRpcDispatcher()
        dispatcher.registerMethod("StatusEvent", JsonRpcInlineMethodHandler() { [weak self] params, extras in
            self?.handleJsonposStatusUpdate(extras?.connection, params)
            return JSONObject()
        })
        
        // Create a JSONRPC connection for the input/output stream, configure
        // it, and start read/write loops.
        let conn = JsonRpcConnection(inputStream: inputStream, outputStream: outputStream)
        currentConnection = conn
        conn.setKeepalive()
        // conn.setDiscard(RFCOMM_DISCARD_TIME)  // unnecessary if _Sync reply scanning is reliable
        conn.setSync(NetworkProxyRunner.SYNC_TIMEOUT)
        conn.dispatcher = dispatcher
        DispatchQueue.main.async {
            conn.start()
        }
        
        // Wait for _Sync to complete before sending anything.
        Log.i(logTag, "wait for connection to become ready")
        try conn.waitReady()
        
        // Reset backoff.
        failCount = 0
        
        updateConnectionState(conn, ConnectionState.CONNECTING)
        
        // Check TerminalInfo before enabling network proxy.  TerminalInfo
        // can provide useful information for choosing rate limits. Fall back
        // to VersionInfo (older terminals).  The 'ios_sdk_version' is not
        // a required field, but is given informatively.
        var terminalInfo: JSONObject! = nil
        let versionMethods = ["TerminalInfo", "VersionInfo"]
        for method in versionMethods {
            var terminalInfoParams = JSONObject()
            terminalInfoParams["ios_sdk_version"] = sdkVersion
            let terminalInfoPromise = try conn.sendRequestAsync(method, terminalInfoParams)
            do {
                terminalInfo = try terminalInfoPromise.wait()
                Log.i(logTag, String(format: "%@: %@", method, terminalInfo.toString()))
                break
            } catch {
                Log.w(logTag, String(format: "%@: failed, ignoring", method), error)
            }
        }
        if terminalInfo == nil {
            Log.w(logTag, "failed to get TerminalInfo")
            terminalInfo = JSONObject()
        }
        updateTerminalInfo(conn, terminalInfo)
        
        // Feature detection based on version comparison.  Terminal versions
        // numbers have the format MAJOR.MINOR.PATCH where each component is
        // a number.  Use version comparison helper class for comparisons.
        if let terminalVersion = terminalInfo.string(for: "version") {
            Log.i(logTag, "terminal software version is: " + terminalVersion)
            if terminalVersion == "0.0.0" {
                Log.w(logTag, "terminal is running an unversioned development build (0.0.0)")
            }
            do {
                if TerminalVersion.supportsStatusEvent(terminalVersion) {
                    Log.i(logTag, "terminal supports StatusEvent, no need for polling")
                } else {
                    Log.i(logTag, "terminal does not support StatusEvent, poll using Status request")
                    try runStatusPoller(conn)
                }
            } catch {
                Log.w(logTag, "failed to parse terminal version", error)
            }
        } else {
            Log.w(logTag, "terminal software version is unknown")
        }
        
        // Rate limits are based on the known or estimated base link speed.
        // Use .link_speed from TerminalInfo response (with a sanity minimum)
        // so that SPm20 link speed differences are taken into account
        // automatically.  Assume a hardcoded default if no .link_speed is
        // available.
        var linkSpeed = terminalInfo.int(for: "link_speed") ?? NetworkProxyRunner.SPM20_LINK_SPEED
        linkSpeed = max(linkSpeed, NetworkProxyRunner.MIN_LINK_SPEED) / 3
        Log.i(logTag, String(format: "use base link speed %d bytes/second", linkSpeed))
        
        // Compute other rate limits from the base link speed.
        let jsonrpcWriteTokenRate = linkSpeed
        let jsonrpcWriteMaxTokens = Int(Double(jsonrpcWriteTokenRate) * 0.25)  // ~250ms buffered data maximum
        let dataWriteTokenRate = Int(Double(jsonrpcWriteTokenRate) * 0.4)  // 0.5 expands by base64 to about 70-80% of link, plus overhead and headroom for other requests
        let dataWriteMaxTokens = Int(Double(jsonrpcWriteTokenRate) * 0.4 * 0.25)  // ~250ms buffered data maximum
        
        Log.i(logTag, String(format: "using jsonrpc transport rate limits: maxTokens=%d, rate=%d", jsonrpcWriteMaxTokens, jsonrpcWriteTokenRate))
        Log.i(logTag, String(format: "using Data notify rate limits: maxTokens=%d, rate=%d", dataWriteMaxTokens, dataWriteTokenRate))
        
        let connLimiter = try TokenBucketRateLimiter(name: "jsonrpcWrite", maxTokens: jsonrpcWriteMaxTokens, tokensPerSecond: jsonrpcWriteTokenRate)
        let dataLimiter = try TokenBucketRateLimiter(name: "dataWrite", maxTokens: dataWriteMaxTokens, tokensPerSecond: dataWriteTokenRate)
        conn.writeRateLimiter = connLimiter
        
        // _Sync and other handshake steps completed, register Network*
        // methods and start networking.  This could be made faster by
        // starting the network proxy right after _Sync, and then updating
        // the rate limits once we have a TerminalInfo response.
        Log.i(logTag, "starting network proxy")
        let proxy = NetworkProxy(conn: conn, dataWriteLimiter: dataLimiter, linkSpeed: jsonrpcWriteTokenRate)
        currentProxy = proxy
        proxy.registerNetworkMethods(dispatcher)
        try proxy.startNetworkProxySync()
        proxy.setTerminalIdleState(currentTerminalIdle)
        
        updateConnectionState(conn, ConnectionState.CONNECTED)
        
        // Check for proxy stop.
        if stopReason != nil {
            throw RuntimeException("closing because proxy stop requested")
        }
        
        // Wait for JSONPOS connection to finish, due to any cause.
        Log.i(logTag, "wait for jsonrpc connection to finish")
        try _ = conn.closedPromise.wait()
        Log.i(logTag, "jsonrpc connection finished")
    }
}

/*
 *  Helpers to compare payment terminal version numbers (e.g. "18.7.0") and
 *  for specific feature checks.
 */

public class TerminalVersion: NSObject {
    
    private static let logTag = "TerminalVersion"
    
    public static func breakIntoComponents(_ version: String) throws -> [Int] {
        let parts = version.components(separatedBy: ".").compactMap({ Int($0) })
        guard parts.count == 3 else {
            throw IllegalArgumentException("version number format invalid")
        }
        return parts
    }
    
    public static func validate(_ version: String) -> Bool {
        do {
            let _ = try breakIntoComponents(version)
            return true
        } catch {
            return false
        }
    }
    
    public static func greaterOrEqual(_ versionA: String, _ versionB: String) -> Bool {
        guard let a = try? breakIntoComponents(versionA),
              let b = try? breakIntoComponents(versionB) else {
            return false
        }
        for i in 0 ..< a.count {
            if a[i] > b[i] {
                return true
            }
            if a[i] < b[i] {
                return false
            }
        }
        return true  // equal
    }
    
    public static func equal(_ versionA: String, _ versionB: String) -> Bool {
        return greaterOrEqual(versionA, versionB) && greaterOrEqual(versionB, versionA)
    }
    
    // Some manual development builds may have version "0.0.0".  These should
    // never appear in integrator system testing or production environments
    // however.  For feature tests, assume all new features are supported in
    // unversioned dev builds.
    public static func isUnversionedDevBuild(_ version: String) -> Bool {
        return version == "0.0.0"
    }
    
    public static func supportsStatusEvent(_ version: String) -> Bool {
        return greaterOrEqual(version, "18.2.0") || isUnversionedDevBuild(version)
    }
}
