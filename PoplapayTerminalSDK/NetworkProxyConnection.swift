//
//  NetworkProxyConnection.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 05/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import Foundation
import PromiseKit

/*
 *  Handle one TCP connection for NetworkProxy.
 */

class NetworkProxyConnection: NSObject, StreamDelegate {
    
    private static let READ_BUFFER_SIZE = 512
    
    private static let CONNECT_TIMEOUT: TimeInterval = 10
    private static let READ_THROTTLE_SLEEP: TimeInterval = 2
    private static let READ_THROTTLE_NONINTERACTIVE: TimeInterval = 5
    
    let host: String
    let port: Int
    let id: Int64
    let proxy: NetworkProxy
    let linkSpeed: Int  // bytes/second
    
    private let logTag: String
    
    private var readThread: Thread!
    private var socketInputStream: InputStream!
    private var socketOutputStream: OutputStream!
    
    let connectedPromise: Promise<Void>
    private let connectedResolver: Resolver<Void>
    
    private var connectedRejectTimer: Timer?
    
    let closedPromise: Promise<Error?>
    private let closedResolver: Resolver<Error?>
    
    private var readQueue = [Data]()
    private let readDispatchQueue = DispatchQueue(label: "readQueue")
    
    private var lastInternetReadThrottle: Date!
    
    var lastWriteAttention: TimeInterval = 0
    
    public init(host: String, port: Int, id: Int64, proxy: NetworkProxy, linkSpeed: Int) {
        self.host = host
        self.port = port
        self.id = id
        self.proxy = proxy
        self.linkSpeed = linkSpeed
        
        logTag = String(format: "NetworkProxyConnection(%@:%d/%d)", host, port, id)
        
        (connectedPromise, connectedResolver) = Promise<Void>.pending()
        (closedPromise, closedResolver) = Promise<Error?>.pending()
    }
    
    public var isClosed: Bool {
        return closedPromise.isResolved
    }
    
    public func start() {
        startReadThread()
    }
    
    public func close(_ reason: Error?) {
        // For a clean close, the input stream has been read until EOF and all
        // data has been queued to the JsonRpcConnection write queue as Data
        // notifies.  NetworkDisconnected is sent out by NetworkProxy based on
        // the connection having no queued data and the connection being closed.
        // (This relies on queued Data and NetworkDisconnected not being
        // reordered before being written out, which is true now when there's
        // no queue prioritization.)
        
        let reason = (reason != nil) ? reason : RuntimeException("closed without reason")
        connectedResolver.fulfill(())
        closedResolver.fulfill(reason)
        socketInputStream?.close()
        socketOutputStream?.close()
        
        // At this point closedPromise is set and the connection is finished.
        // There may still be undelivered data, which is delivered by network
        // proxy.  Once the data is delivered, the proxy notifies there's no
        // data and the connection is closed, and issues a NetworkDisconnected.
        proxy.triggerWriteCheck()
    }
    
    private func startReadThread() {
        let logTag = self.logTag
        readThread = Thread() { [weak self] in
            do {
                try self?.runReadThread()
                self?.close(RuntimeException("clean close by remote peer"))
            } catch {
                Log.i(logTag, "read thread failed", error)
                self?.close(error)
            }
        }
        readThread.start()
    }
    
    public func write(_ data: Data) throws {
        // We could also throttle data sent towards to internet.
        // This is in practice unnecessary with RFCOMM but maybe
        // necessary later when proxying is used with e.g. Wi-Fi
        // terminals.
        Log.d(logTag, String(format: "TERMINAL -> INTERNET: %d bytes of data", data.count))
        socketOutputStream.write([UInt8](data), maxLength: data.count)
    }
    
    private func runReadThread() throws {
        Log.i(logTag, String(format: "start read thread; connecting to %@:%d", host, port))
        
        var readStream: Unmanaged<CFReadStream>?
        var writeStream: Unmanaged<CFWriteStream>?
        
        CFStreamCreatePairWithSocketToHost(kCFAllocatorDefault, host as CFString, UInt32(port), &readStream, &writeStream)
        
        socketInputStream = readStream?.takeRetainedValue()
        socketOutputStream = writeStream?.takeRetainedValue()
        
        socketInputStream.schedule(in: .current, forMode: .common)
        socketOutputStream.schedule(in: .current, forMode: .common)
        
        socketInputStream.delegate = self
        socketOutputStream.delegate = self
        
        socketInputStream.open()
        socketOutputStream.open()
        
        let timeout = NetworkProxyConnection.CONNECT_TIMEOUT
        connectedRejectTimer?.invalidate()
        connectedRejectTimer = Timer.scheduledTimer(withTimeInterval: timeout, repeats: false, block: { [weak self] _ in
            self?.connectedResolver.reject(RuntimeException("Socket connection timed out"))
        })
        
        while (true) {
            if isClosed {
                break
            }
            if throttleInternetRead() {
                Log.d(logTag, "too much queued read data, throttle internet reads")
                lastInternetReadThrottle = Date()
                Thread.sleep(forTimeInterval: NetworkProxyConnection.READ_THROTTLE_SLEEP)
                continue
            }
            
            var buf = Data()
            let got = socketInputStream.read(&buf)
            if got < 0 {
                Log.i(logTag, "input stream EOF")
                break
            }
            if got > buf.count {
                throw RuntimeException(String(format: "internal error, unexpected read() result %d", got))
            }
            if got > 0 {
                // Data towards terminal is always queued because throttling it
                // fairly is critical with RFCOMM connections.  NetworkProxy pulls
                // data from the queue.
                Log.d(logTag, String(format: "INTERNET -> TERMINAL: %d bytes of data", got))
                readDispatchQueue.sync {
                    readQueue.append(buf)
                }
                proxy.triggerWriteCheck()
            }
        }
    }
    
    public func getQueuedReadData() -> Data? {
        var data: Data? = nil
        readDispatchQueue.sync {
            data = (readQueue.count > 0) ? readQueue.removeFirst() : nil
        }
        return data
    }
    
    private func throttleInternetRead() -> Bool {
        // Throttling internet reads is not critical, we just don't want
        // to keep excessive data waiting for transmission.
        let throttleLimit = (linkSpeed * 2 / 3) * 5  // 5 seconds of unexpanded data
        let queuedBytes = getReadQueueBytes()
        if queuedBytes > 0 {
            Log.d(logTag, "queuedBytes: \(queuedBytes), throttleLimit: \(throttleLimit)")
        }
        return queuedBytes >= throttleLimit
    }
    
    private func getReadQueueBytes() -> Int64 {
        var res: Int64 = 0
        for data in readQueue {
            res += Int64(data.count)
        }
        return res
    }
    
    public var hasPendingData: Bool {
        return !readQueue.isEmpty
    }
    
    // Heuristic estimate whether the connection seems interactive or a
    // background data transfer connection.  This doesn't need to be right
    // 100% of the time, as it only affects rate limiting.
    public var seemsInteractive: Bool {
        // Minimally functional: if read throttle limit was hit, consider
        // connection non-interactive for a certain window of time.  Also
        // consider interactive if connection is pending a close and no
        // data is queued so that NetworkDisconnected is sent quickly.
        //
        // XXX: Could be improved by considering queued data amount also.
        // For now checking only Internet throttling works well enough
        // because the Internet reads are quite strictly throttled.
        if isClosed && !hasPendingData {
            return true
        } else if let lastInternetReadThrottle = lastInternetReadThrottle {
            return lastInternetReadThrottle.timeIntervalSinceNow > NetworkProxyConnection.READ_THROTTLE_NONINTERACTIVE
        } else {
            return true
        }
    }
    
    // MARK: - StreamDelegate
    
    func stream(_ stream: Stream,
                handle eventCode: Stream.Event) {
        
        Log.d(logTag, "\(type(of: stream)) (status: \(stream.streamStatus.identifier)) got event: \(eventCode.identifier)")
        
        switch eventCode {
        case .openCompleted:
            if socketInputStream.isOpen, connectedPromise.isPending {
                Log.i(logTag, "connection starting")
                Log.i(logTag, String(format: "connected to %@:%d, start read loop and write thread", host, port))
                connectedRejectTimer?.invalidate()
                connectedResolver.fulfill(())
            }
        default:
            break
        }
    }
}
