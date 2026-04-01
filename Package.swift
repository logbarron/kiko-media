// swift-tools-version:6.2
import PackageDescription

let package = Package(
    name: "KikoMedia",
    platforms: [
        .macOS(.v26)
    ],
    products: [
        .executable(
            name: "benchmarks",
            targets: ["benchmarks"]
        ),
        .executable(
            name: "benchmark",
            targets: ["benchmarks"]
        ),
        .executable(
            name: "orchestrator",
            targets: ["Orchestrator"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/hummingbird.git", from: "2.20.0"),
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.10.0"),
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.32.0"),
    ],
    targets: [
        .target(
            name: "KikoMediaCore",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        .target(
            name: "KikoMediaApp",
            dependencies: [
                "KikoMediaCore",
                .product(name: "Hummingbird", package: "hummingbird"),
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
            ],
            path: "Sources/KikoMedia",
            swiftSettings: [
                .unsafeFlags(["-cross-module-optimization"], .when(configuration: .release))
            ]
        ),
        .executableTarget(
            name: "KikoMedia",
            dependencies: [
                "KikoMediaApp",
            ],
            path: "Sources/KikoMediaExecutable",
            swiftSettings: [
                .unsafeFlags(["-cross-module-optimization"], .when(configuration: .release))
            ]
        ),
        .testTarget(
            name: "KikoMediaTests",
            dependencies: [
                "KikoMediaCore",
                "KikoMediaApp",
                "benchmarks",
                "Orchestrator",
                .product(name: "HummingbirdTesting", package: "hummingbird"),
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        .executableTarget(
            name: "benchmarks",
            dependencies: [
                "KikoMediaCore",
                .product(name: "GRDB", package: "GRDB.swift"),
            ],
            path: "Sources/Benchmarks",
            swiftSettings: [
                .unsafeFlags(["-cross-module-optimization"], .when(configuration: .release))
            ]
        ),
        .executableTarget(
            name: "Orchestrator",
            dependencies: [
                "KikoMediaCore",
            ],
            path: "Sources/Orchestrator"
        ),
    ]
)
