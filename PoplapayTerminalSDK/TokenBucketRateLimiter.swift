//
//  TokenBucketRateLimiter.swift
//  PoplapayTerminalSDK
//
//  Created by Janne Käki on 06/02/2019.
//  Copyright © 2019 Poplapay. Licensed under CC0-1.0.
//

import Foundation
import PromiseKit

/*
 *  Token bucket rate limiter.
 */

public class TokenBucketRateLimiter: NSObject, RateLimiter {
    
    private let statsInterval: TimeInterval = 300
    
    private let logTag: String
    
    private var lastStats: TimeInterval
    private var lastUptime: TimeInterval
    
    private var lastCount: Double
    private let maxTokens: Double        // tokens
    private let tokensPerSecond: Double  // tokens/second
    private var statsConsumed: Double    // tokens consumed since last stats log write
    
    public init(name: String, maxTokens: Int, tokensPerSecond: Int) throws {
        if maxTokens <= 0 {
            throw IllegalArgumentException(String(format: "invalid maxTokens %d", maxTokens))
        }
        if tokensPerSecond <= 0 {
            throw IllegalArgumentException(String(format: "invalid tokensPerSecond %d", tokensPerSecond))
        }
        let now = Date().timeIntervalSinceReferenceDate
        self.logTag = String(format: "TokenBucketRateLimiter(%@)", name)
        self.lastStats = 0  // force immediate stats logging on first attempt
        self.lastUptime = now
        self.lastCount = Double(maxTokens)  // initialize to maximum capacity
        self.maxTokens = Double(maxTokens)
        self.tokensPerSecond = Double(tokensPerSecond)
        self.statsConsumed = 0
        Log.i(logTag, String(format: "created rate limiter, maxTokens=%d, tokensPerSecond=%d", maxTokens, tokensPerSecond))
    }
    
    // Update current token state based on the difference between last update
    // time and current time.
    private func updateCurrent() {
        let now = Date().timeIntervalSinceReferenceDate
        let diff = now - lastUptime
        let tokenAdd = diff * tokensPerSecond
        let timeAdd = tokenAdd / tokensPerSecond
        let newTokens = lastCount + tokenAdd
        if newTokens > maxTokens {
            lastUptime = now
            lastCount = maxTokens
        } else {
            lastUptime += timeAdd
            lastCount = newTokens
        }
    }
    
    private func checkStatsLog() {
        let now = Date().timeIntervalSinceReferenceDate
        if now - lastStats >= statsInterval {
            Log.i(logTag, String(format: "stats: %d tokens consumed in last interval, %d / second (tokensPerSecond %d / second)",
                                        statsConsumed,
                                        Int64(Double(statsConsumed) / statsInterval),
                                        tokensPerSecond))
            lastStats = now
            statsConsumed = 0
        }
    }
    
    private func tryConsumeRaw(_ count: Double) -> Bool {
        // 'synchronized' is not strictly necessary because the only current
        // call site is itself synchronized.
        var result: Bool = false
        synchronized (self) {
            checkStatsLog()
            updateCurrent()
            if lastCount >= count {
                lastCount -= count
                statsConsumed += count
                result = true
            }
        }
        return result
    }
    
    // Consume 'count' tokens, blocking until complete.  Token count may be
    // higher than maxTokens; the requested number of tokens is consumed in
    // smaller pieces if necessary.  There are no explicit fairness guarantees.
    public func consumeSync(_ count: Int) throws {
        let count = Double(count)
        if count < 0 {
            throw RuntimeException(String(format: "invalid requested token count: %.0f, count", count))
        }
        let chunk = max(maxTokens / 2, 1)
        var left = count
        while left > 0 {
            let now = min(left, chunk)
            var sleepTime: TimeInterval = 0  // dummy default, overwritten if needed
            synchronized (self) {
                if tryConsumeRaw(now) {
                    left -= now
                } else {
                    sleepTime = Double(now - lastCount) / Double(tokensPerSecond) + 0.001
                    Log.d(logTag, String(format: "need to wait %.3f s, requested %.0f, current %.0f (total left %.0f, original requested %.0f)",
                                         sleepTime, now, lastCount, left, count))
                }
            }
            if sleepTime > 0 {
                Thread.sleep(forTimeInterval: sleepTime)
            }
        }
    }
    
    // Promise variant of consumeSync().
    public func consumeAsync(_ count: Int) throws -> Promise<Void> {
        return Promise<Void>(resolver: { (resolver) in
            try self.consumeSync(count)
            resolver.fulfill(())
        })
    }
}
