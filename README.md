# Poplapay Terminal iOS SDK

This repo provides the Swift source files and a sample application for Poplapay
payment terminal iOS SDK:

* **Source files** can be integrated directly into your application, using
  CocoaPods.  The components use [PromiseKit](https://github.com/PromiseKit)
  to implement promise-based asynchronous and synchronous interactions, but
  other than that, external dependencies have been kept as minimal as possible. 
  The SDK connects to the payment terminal via the External Accessory framework
  (protocol com.thyron) over Bluetooth.

* **The sample application** can be built and installed on an iOS device. 
  It provides examples and guidance in using the SDK APIs.  It can also be used
  as a starting point for making custom applications.

## Sample application

The SDK contains a sample application, TestNetworkProxy. It provides an
automatically reconnecting, persistent External Accessory connection via
Bluetooth with JSONPOS network proxying, as well as a visual listing of all
messages being passed back and forth.

To build the sample app, open the workspace in Xcode, install PromiseKit via
CocoaPods, and build/run normally.

## Using library components in an app project

Library sources can be copied into an Xcode project using CocoaPods:

```
pod 'PoplapayTerminalSDK', :git => 'https://github.com/poplatek/pt-ios-sdk.git', :tag => 'release/0.1'
```

Required declarations in Info.plist:

```xml
<key>UIBackgroundModes</key>
<array>
    <string>external-accessory</string>
</array>
<key>UISupportedExternalAccessoryProtocols</key>
<array>
    <string>com.thyron</string>
</array>
```

Starting and stopping the network proxy:

```swift
private var runner: NetworkProxyRunner!

func restartNetworkProxy() {
    runner?.stop()
    runner = NetworkProxyRunner()
    runner.start()
}
```
