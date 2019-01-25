//
//  JsonRpcException.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 05/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import Foundation

/*
 *  Representation of JSONRPC-originated exceptions and mapping between
 *  Exceptions and JSONRPC error boxes.
 */

public class Exception: Error, CustomStringConvertible {
    
    let code: String
    let message: String
    let cause: Error?
    let callStackSymbols: [String]
    
    public init(code: String, message: String, cause: Error? = nil) {
        self.code = code
        self.message = message
        self.cause = cause
        self.callStackSymbols = Thread.callStackSymbols
    }
    
    public var localizedDescription: String {
        return "\(code): \(message)"
    }
    
    public var description: String {
        return localizedDescription
    }
}

public class JsonRpcException: Exception {
    
    private static let logTag = "JsonRpcException"
    private static let MAX_CODE_LENGTH = 256
    private static let MAX_MESSAGE_LENGTH = 1024
    private static let MAX_DETAILS_LENGTH = 8192
    
    var details: String!
    
    public init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        // Fill in (sanitized) message using super constructor.
        
        // Fill in (sanitized) code and message separately.  Note that for default
        // 'details' code and message are intentionally defaulted but *not* sanitized,
        // because details will go through a sanitization step for the final result.
        
        let code = code ?? "UNKNOWN"
        let message = message ?? ""
        
        super.init(code: JsonRpcException.sanitizeCode(code), message: message, cause: cause)
        
        if let details = details {
            self.details = JsonRpcException.sanitizeDetails(details)
        } else {
            self.details = JsonRpcException.formatJsonRpcDefaultDetailsShort(self, code, message)
        }
    }
    
    private static func clipString(_ x: String, _ limit: Int) -> String {
        if x.count > limit {
            return String(x.prefix(limit))
        }
        return x
    }
    
    private static func sanitizeCode(_ code: String) -> String {
        return clipString(code, MAX_CODE_LENGTH)
    }
    
    private static func sanitizeMessage(_ message: String) -> String {
        return clipString(message, MAX_MESSAGE_LENGTH)
    }
    
    private static func sanitizeDetails(_ details: String) -> String {
        return clipString(details, MAX_DETAILS_LENGTH)
    }
    
    // Get a formatted traceback with unsuppressed cause chain.
    private static func formatJsonRpcDefaultDetailsFull(_ error: Error, _ code: String, _ message: String) -> String {
        var sb = ""
        
        var error: Error! = error
        var first = true
        while error != nil {
            
            sb += error.localizedDescription
            
            if first {
                first = false
            } else {
                sb += "\nCaused by: "
            }
            
            if let exception = error as? Exception {
                sb += exception.callStackSymbols.joined(separator: "\n")
                error = exception.cause
            } else {
                error = nil
            }
        }
        
        // No trailing newline for details.
        return String(format: "%@: %@\n%@", code, message, sb)
    }
    
    private static func formatJsonRpcDefaultDetailsShort(_ error: Error, _ code: String, _ message: String) -> String {
        let trace = (error as? Exception)?.callStackSymbols.joined(separator: "\n") ?? ""
        return String(format: "%@: %@\n%@", code, message, trace)
    }
    
    // Map a JSONRPC numeric error code to a string code.
    public static func jsonCodeToStringCode(_ jsonCode: Int64) -> String {
        switch jsonCode {
        case -32700:
            return "JSONRPC_PARSE_ERROR"
        case -32600:
            return "JSONRPC_INVALID_REQUEST"
        case -32601:
            return "JSONRPC_METHOD_NOT_FOUND"
        case -32602:
            return "JSONRPC_INVALID_PARAMS"
        case -32603:
            return "INTERNAL_ERROR"
        case -32000:
            return "KEEPALIVE"
        default:
            return "UNKNOWN"
        }
    }

    // Map a string code to JSONRPC numeric error code.
    public static func stringCodeToJsonCode(_ stringCode: String) -> Int64 {
        switch stringCode {
        case "JSONRPC_PARSE_ERROR":
            return -32700
        case "JSONRPC_INVALID_REQUEST":
            return -32600
        case "JSONRPC_METHOD_NOT_FOUND":
            return -32601
        case "JSONRPC_INVALID_PARAMS":
            return -32602
        case "INTERNAL_ERROR":
            return -32603
        case "KEEPALIVE":
            return -32000
        default:
            return 1
        }
    }
    
    // Convert a JSONRPC 'error' box to an Exception object.
    public static func errorBoxToException(_ error: JSONObject) -> Error {
        var jsonCode: Int64 = 1
        var stringCode: String! = nil
        var details: String! = nil
        var message: String! = nil
        
        message = error.string(for: "message") ?? ""
        if let data = error.dict(for: "data") {
            stringCode = data.string(for: "string_code")
            details = data.string(for: "details")
        }
        jsonCode = error.int64(for: "code") ?? 1
        
        if stringCode == nil {
            stringCode = jsonCodeToStringCode(jsonCode)
        }
        if details == nil {
            details = String(format: "%@: %@", stringCode, message)
        }
        
        let exc: JsonRpcException
        if stringCode == "JSONRPC_PARSE_ERROR" {
            exc = JsonRpcParseErrorException(stringCode, message, details, nil)
        } else if stringCode == "JSONRPC_INVALID_REQUEST" {
            exc = JsonRpcInvalidRequestException(stringCode, message, details, nil)
        } else if stringCode == "JSONRPC_METHOD_NOT_FOUND" {
            exc = JsonRpcMethodNotFoundException(stringCode, message, details, nil)
        } else if stringCode == "JSONRPC_INVALID_PARAMS" {
            exc = JsonRpcInvalidParamsException(stringCode, message, details, nil)
        } else if stringCode == "INTERNAL_ERROR" {
            exc = JsonRpcInternalErrorException(stringCode, message, details, nil)
        } else if stringCode == "KEEPALIVE" {
            exc = JsonRpcKeepaliveException(stringCode, message, details, nil)
        } else {
            exc = JsonRpcException(stringCode, message, details, nil)
        }
        
        return exc
    }

    // Convert an arbitrary Exception to a JSONRPC 'error' box.
    public static func exceptionToErrorBox(_ exc: Error) throws -> JSONObject {
        var jsonCode: Int64 = 1
        var stringCode: String = "UNKNOWN"
        var message: String = exc.localizedDescription
        var details: String!

        // exc = ExceptionUtil.unwrapExecutionExceptionsToThrowable(exc)

        // Round trip exceptions coming from JSONRPC cleanly.  For other exceptions
        // use JsonRpcException() constructor which guarantees sanitized and clipped
        // results that won't exceed message size limits.
        var jsonExc: JsonRpcException
        if let exc = exc as? JsonRpcException {
            jsonExc = exc
        } else {
            let tmpCode = "UNKNOWN"
            let tmpMessage = exc.localizedDescription
            jsonExc = JsonRpcException(tmpCode,
                                       tmpMessage,
                                       JsonRpcException.formatJsonRpcDefaultDetailsShort(exc, tmpCode, tmpMessage),
                                       nil)
        }
        stringCode = jsonExc.code
        message = jsonExc.message
        details = jsonExc.details
        jsonCode = stringCodeToJsonCode(stringCode)
        
        var data = JSONObject()
        data["string_code"] = stringCode
        data["details"] = details
        
        var error = JSONObject()
        error["code"] = jsonCode
        error["message"] = message
        error["data"] = data
        return error
    }
}

public class JsonRpcInternalErrorException: JsonRpcException {
    public override init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        super.init(code, message, details, cause)
    }

    public init(_ message: String) {
        super.init("INTERNAL_ERROR", message, nil, nil)
    }

    public init(_ message: String, _ cause: Error?) {
        super.init("INTERNAL_ERROR", message, nil, cause)
    }
}

public class JsonRpcInvalidParamsException: JsonRpcException {
    public override init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        super.init(code, message, details, cause)
    }

    public init(_ message: String) {
        super.init("JSONRPC_INVALID_PARAMS", message, nil, nil)
    }

    public init(_ message: String, _ cause: Error?) {
        super.init("JSONRPC_INVALID_PARAMS", message, nil, cause)
    }
}

public class JsonRpcInvalidRequestException: JsonRpcException {
    public override init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        super.init(code, message, details, cause)
    }

    public init(_ message: String) {
        super.init("JSONRPC_INVALID_REQUEST", message, nil, nil)
    }

    public init(_ message: String, _ cause: Error?) {
        super.init("JSONRPC_INVALID_REQUEST", message, nil, cause)
    }
}

public class JsonRpcKeepaliveException: JsonRpcException {
    public override init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        super.init(code, message, details, cause)
    }

    public init(_ message: String) {
        super.init("KEEPALIVE", message, nil, nil)
    }

    public init(_ message: String, _ cause: Error?) {
        super.init("KEEPALIVE", message, nil, cause)
    }
}

public class JsonRpcMethodNotFoundException: JsonRpcException {
    public override init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        super.init(code, message, details, cause)
    }

    public init(_ message: String) {
        super.init("JSONRPC_METHOD_NOT_FOUND", message, nil, nil)
    }

    public init(_ message: String, _ cause: Error?) {
        super.init("JSONRPC_METHOD_NOT_FOUND", message, nil, cause)
    }
}

public class JsonRpcParseErrorException: JsonRpcException {
    public override init(_ code: String?, _ message: String?, _ details: String?, _ cause: Error?) {
        super.init(code, message, details, cause)
    }

    public init(_ message: String) {
        super.init("JSONRPC_PARSE_ERROR", message, nil, nil)
    }

    public init(_ message: String, _ cause: Error?) {
        super.init("JSONRPC_PARSE_ERROR", message, nil, cause)
    }
}

public class RuntimeException: Exception {
    
    init(_ message: String?, _ cause: Error? = nil) {
        super.init(code: String(describing: type(of: self)), message: message ?? "", cause: cause)
    }
}

public class IllegalArgumentException: RuntimeException {
}

public class IllegalStateException: RuntimeException {
}

public class InvalidDataException: RuntimeException {
}
