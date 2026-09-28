// swift-tools-version:6.2.3
//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2022 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Foundation
import PackageDescription

// The Swift Static Linux SDK (musl) bundles BoringSSL but not libsasl2, and its `openssl`
// pkg-config module doesn't resolve for the cross-target. Set `SWIFT_KAFKA_MUSL=1` when building
// against a musl SDK to drop Cyrus SASL and link the SDK's ssl/crypto directly. SwiftPM can't
// detect the target libc during manifest evaluation, so this is driven by an environment variable.
let buildingForMusl = ProcessInfo.processInfo.environment["SWIFT_KAFKA_MUSL"] != nil

var rdkafkaExclude = [
    "./librdkafka/src/CMakeLists.txt",
    "./librdkafka/src/Makefile",
    "./librdkafka/src/README.lz4.md",
    "./librdkafka/src/generate_proto.sh",
    "./librdkafka/src/librdkafka_cgrp_synch.png",
    "./librdkafka/src/opentelemetry/metrics.options",
    "./librdkafka/src/rdkafka_sasl_win32.c",
    "./librdkafka/src/rdwin32.h",
    "./librdkafka/src/statistics_schema.json",
    "./librdkafka/src/win32_config.h",
    // Remove dependency on cURL. Disabling `ENABLE_CURL` and `WITH_CURL` does
    // not appear to prevent processing of the below files, so we have to exclude
    // them explicitly.
    "./librdkafka/src/rdkafka_sasl_oauthbearer.c",
    "./librdkafka/src/rdkafka_sasl_oauthbearer_oidc.c",
    "./librdkafka/src/rdhttp.c",
]

var crdkafkaLinkerSettings: [LinkerSetting] = [
    .linkedLibrary("sasl2"),
    .linkedLibrary("z"),  // zlib
]

if buildingForMusl {
    // rdkafka_sasl_cyrus.c includes <sasl/sasl.h> unconditionally; exclude it since the musl SDK
    // has no Cyrus SASL. Link the SDK-bundled BoringSSL (ssl/crypto) explicitly and drop sasl2.
    rdkafkaExclude.append("./librdkafka/src/rdkafka_sasl_cyrus.c")
    crdkafkaLinkerSettings = [
        .linkedLibrary("z"),  // zlib
        .linkedLibrary("ssl"),  // BoringSSL, bundled in the Static Linux SDK
        .linkedLibrary("crypto"),
    ]
}

let package = Package(
    name: "swift-kafka-client",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .watchOS(.v11),
        .tvOS(.v18),
    ],
    products: [
        .library(
            name: "Kafka",
            targets: ["Kafka"]
        ),
        .library(
            name: "KafkaFoundationCompat",
            targets: ["KafkaFoundationCompat"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.55.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.1.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.14.0"),
        .package(url: "https://github.com/apple/swift-metrics", from: "2.4.1"),
        // The zstd Swift package produces warnings that we cannot resolve:
        // https://github.com/facebook/zstd/issues/3328
        .package(url: "https://github.com/facebook/zstd.git", from: "1.5.0"),
    ],
    targets: [
        .target(
            name: "Crdkafka",
            dependencies: [
                "COpenSSL",
                .product(name: "libzstd", package: "zstd"),
            ],
            exclude: rdkafkaExclude,
            sources: [
                "./librdkafka/src/",
                "./custom/musl_compat",  // BoringSSL shims (inert on glibc/macOS)
            ],
            publicHeadersPath: "./include",
            cSettings: [
                // dummy folder, because config.h is included as "../config.h" in librdkafka
                .headerSearchPath("./custom/config/dummy"),
                .headerSearchPath("./librdkafka/src"),
            ],
            linkerSettings: crdkafkaLinkerSettings
        ),
        .target(
            name: "Kafka",
            dependencies: [
                "Crdkafka",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
            ]
        ),
        .target(
            name: "KafkaFoundationCompat",
            dependencies: [
                "Kafka"
            ]
        ),
        .systemLibrary(
            name: "COpenSSL",
            pkgConfig: "openssl",
            providers: [
                .brew(["openssl@3"]),
                .apt(["libssl-dev"]),
            ]
        ),
        .testTarget(
            name: "KafkaTests",
            dependencies: [
                "Kafka",
                .product(name: "MetricsTestKit", package: "swift-metrics"),
            ]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: ["Kafka"]
        ),
    ]
)

// A tiny executable that links the library, built only for musl. A library `swift build` doesn't
// perform a final executable link, so this gives the Static SDK CI real link coverage — proving
// the BoringSSL shims resolve and no undefined symbols remain. Absent from normal builds.
if buildingForMusl {
    package.targets.append(
        .executableTarget(
            name: "MuslLinkCheck",
            dependencies: ["Kafka"]
        )
    )
}

for target in package.targets {
    switch target.type {
    case .regular, .test, .executable:
        var settings = target.swiftSettings ?? []
        settings.append(.enableExperimentalFeature("StrictConcurrency=complete"))
        target.swiftSettings = settings
    case .macro, .plugin, .system, .binary:
        break  // These targets do not support settings
    @unknown default:
        fatalError("Update to handle new target type \(target.type)")
    }
}

// ---    STANDARD CROSS-REPO SETTINGS DO NOT EDIT   --- //
for target in package.targets {
    switch target.type {
    case .regular, .test, .executable:
        var settings = target.swiftSettings ?? []
        // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
        settings.append(.enableUpcomingFeature("MemberImportVisibility"))
        target.swiftSettings = settings
    case .macro, .plugin, .system, .binary:
        ()  // not applicable
    @unknown default:
        ()  // we don't know what to do here, do nothing
    }
}
// --- END: STANDARD CROSS-REPO SETTINGS DO NOT EDIT --- //
