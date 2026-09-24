// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "FlowRun",
    platforms: [.macOS(.v13), .iOS(.v16), .tvOS(.v16), .watchOS(.v9), .visionOS(.v1)],
    products: [
        .library(name: "FlowRun", targets: ["FlowRun"])
    ],
    targets: [
        .target(name: "FlowRun", linkerSettings: [.linkedLibrary("sqlite3")]),
        .testTarget(name: "FlowRunTests", dependencies: ["FlowRun"])
    ]
)
