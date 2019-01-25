//
//  JsonRpcDispatcher.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 05/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import Foundation
import PromiseKit

/*
 *  JSONRPC method dispatcher.  Provides inbound method/notify registration
 *  based on method name.  Handlers are sub-interfaces of JsonRpcMethodHandler,
 *  providing different call styles (inline, thread-based, etc).
 */

public class JsonRpcDispatcher {
    private static let logTag = "JsonRpcDispatcher"
    
    private var methods = [String: JsonRpcMethodHandler]()
    
    public func registerMethod(_ method: String, _ handler: JsonRpcMethodHandler) {
        synchronized (self) {
            if methods[method] != nil {
                Log.i(JsonRpcDispatcher.logTag, String(format: "method %@ already registered, replacing with new handler", method))
            }
            Log.d(JsonRpcDispatcher.logTag, String(format: "registered method %@", method))
            methods[method] = handler
        }
    }
    
    public func unregisterMethod(_ method: String) {
        synchronized (self) {
            if methods[method] != nil {
                Log.d(JsonRpcDispatcher.logTag, String(format: "unregistered method %@", method))
                methods.removeValue(forKey: method)
            } else {
                Log.i(JsonRpcDispatcher.logTag, String(format: "trying to unregister non-existent method %@, ignoring", method))
            }
        }
    }
    
    public func hasMethod(_ method: String) -> Bool {
        return methods[method] != nil
    }
    
    public func getHandler(_ method: String) -> JsonRpcMethodHandler? {
        return methods[method]
    }
}

/*
 *   Marker interface for different JSONRPC method handler call styles:
 *
 *   - JsonRpcThreadMethodHandler: calling code (JsonRpcConnection) launches a
 *     new Thread per request, so the method handler can block.
 *
 *   - JsonRpcInlineMethodHandler: calling code calls handler directly, and the
 *     handler must not block.
 *
 *   - JsonRpcPromiseMethodHandler: calling code calls handler directly, and the
 *     handler must return a Promise which eventually completes.
 */

public protocol JsonRpcMethodHandler {
}

/*
 *  Method handler which returns a Promise<JSONObject> which must eventually
 *  complete.  A direct null return value is allowed and represents an empty
 *  object, {}.  Similarly, if the Promise<JSONObject> completes with null,
 *  it represents and empty object.  The method MUST NOT block, and must
 *  return the Promise (or null) promptly.
 */

public class JsonRpcPromiseMethodHandler: JsonRpcMethodHandler {
    
    typealias Handler = (_ params: JSONObject?, _ extras: JsonRpcMethodExtras?) throws -> Promise<JSONObject>
    
    private let handler: Handler
    
    init(_ handler: @escaping Handler) {
        self.handler = handler
    }
    
    func handle(_ params: JSONObject?, _ extras: JsonRpcMethodExtras?) throws -> Promise<JSONObject> {
        return try handler(params, extras)
    }
}

/*
 *  Method handler executing in the calling code's Thread.  Handler MUST NOT
 *  block.  A null return value is allowed and represents an empty object, {}.
 */

public class JsonRpcInlineMethodHandler: JsonRpcMethodHandler {
    
    typealias Handler = (_ params: JSONObject?, _ extras: JsonRpcMethodExtras?) throws -> JSONObject?
    
    private let handler: Handler
    
    init(_ handler: @escaping Handler) {
        self.handler = handler
    }
    
    func handle(_ params: JSONObject?, _ extras: JsonRpcMethodExtras?) throws -> JSONObject? {
        return try handler(params, extras)
    }
}

/*
 *  Method handler executing in its own Thread so may block as necessary.
 *  Caller (JsonRpcConnection) launches a new Thread per request.  A null
 *  return value is allowed and represents an empty object, {}.
 *
 *  This is the recommended handler variant unless spawning a Thread for
 *  each request has too much of a performance impact.
 */

public class JsonRpcThreadMethodHandler: JsonRpcMethodHandler {
    
    typealias Handler = (_ params: JSONObject?, _ extras: JsonRpcMethodExtras?) throws -> JSONObject?
    
    private let handler: Handler
    
    init(_ handler: @escaping Handler) {
        self.handler = handler
    }
    
    func handle(_ params: JSONObject?, _ extras: JsonRpcMethodExtras?) throws -> JSONObject? {
        return try handler(params, extras)
    }
}
