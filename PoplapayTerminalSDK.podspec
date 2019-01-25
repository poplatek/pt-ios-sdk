Pod::Spec.new do |s|

  s.name         = "PoplapayTerminalSDK"
  s.version      = "0.1"
  s.summary      = "Components and sample application for Poplapay payment terminal iOS SDK."

  s.description  = <<-DESC
                   Provides bluetooth connectivity to Poplapay terminals for iOS applications.
                   DESC

  s.homepage     = "https://github.com/poplatek/pt-ios-sdk"

  s.license      = "CC0-1.0"

  s.author       = { "Janne Käki" => "janne@awesomeness.pro" }

  s.platform     = :ios, "10.0"

  s.source       = { :git => "https://github.com/poplatek/pt-ios-sdk.git", :tag => "release/#{s.version}" }

  s.source_files = "PoplapayTerminalSDK", "PoplapayTerminalSDK/*.swift"

  s.pod_target_xcconfig = { 'SWIFT_VERSION' => '4.2' }

  s.dependency 'PromiseKit/CorePromise', '= 6.8.2'

end
