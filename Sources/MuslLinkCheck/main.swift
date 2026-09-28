//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-kafka-client open source project
//
// Copyright (c) 2026 Apple Inc. and the swift-kafka-client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of swift-kafka-client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

// Static-SDK link check (musl only). References a code path that pulls librdkafka + its crypto
// symbols into the link, so building this executable proves a fully-static musl binary links.
import Kafka

var config = KafkaProducerConfig()
config.bootstrapServers = ["localhost:9092"]
_ = try? KafkaProducer.makeProducer(config: config)
print("musl link check: ok")
