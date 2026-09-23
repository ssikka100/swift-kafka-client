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

import Metrics
import NIOConcurrencyHelpers

/// Holds all producer metric instruments and provides thread-safe recording methods.
final class KafkaProducerMetrics: Sendable {
    private let sendErrors: Counter
    private let sendDuration: Timer
    private let deliverySuccess: Counter
    private let deliveryFailure: Counter
    private let errors: Counter
    private let messagesSent: Counter
    private let bytesSent: Counter
    private let queueMessages: Gauge
    private let queueBytes: Gauge
    private let batchSizeAvg: Gauge
    // Base labels (already prefixed) for per-broker latency windows.
    private let brokerRTTLabel: String
    private let brokerThrottleLabel: String
    private let brokerQueueLatencyLabel: String
    private let brokerRequestLatencyLabel: String

    // librdkafka reports cumulative totals; we emit deltas as Counter increments.
    private let previousMessagesSent: NIOLockedValueBox<Int>
    private let previousBytesSent: NIOLockedValueBox<Int>

    init(prefix: String) {
        self.sendErrors = Counter(label: "\(prefix).\(KafkaMetricLabels.producerSendErrors)")
        self.sendDuration = Timer(label: "\(prefix).\(KafkaMetricLabels.producerSendDuration)")
        self.deliverySuccess = Counter(label: "\(prefix).\(KafkaMetricLabels.producerDeliverySuccess)")
        self.deliveryFailure = Counter(label: "\(prefix).\(KafkaMetricLabels.producerDeliveryFailure)")
        self.errors = Counter(label: "\(prefix).\(KafkaMetricLabels.producerErrors)")
        self.messagesSent = Counter(label: "\(prefix).\(KafkaMetricLabels.producerMessagesSent)")
        self.bytesSent = Counter(label: "\(prefix).\(KafkaMetricLabels.producerBytesSent)")
        self.queueMessages = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerQueueMessages)")
        self.queueBytes = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerQueueBytes)")
        self.batchSizeAvg = Gauge(label: "\(prefix).\(KafkaMetricLabels.producerBatchSizeAvg)")
        self.brokerRTTLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerRTT)"
        self.brokerThrottleLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerThrottle)"
        self.brokerQueueLatencyLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerQueueLatency)"
        self.brokerRequestLatencyLabel = "\(prefix).\(KafkaMetricLabels.producerBrokerRequestLatency)"
        self.previousMessagesSent = NIOLockedValueBox(0)
        self.previousBytesSent = NIOLockedValueBox(0)
    }

    func recordSendError() {
        self.sendErrors.increment()
    }

    /// Records the end-to-end latency of an acknowledged `sendAndAwait(_:)` (enqueue → delivery report).
    func recordSend(duration: Duration) {
        self.sendDuration.record(duration: duration)
    }

    func recordDeliverySuccess() {
        self.deliverySuccess.increment()
    }

    func recordDeliveryFailure() {
        self.deliveryFailure.increment()
    }

    func recordError() {
        self.errors.increment()
    }

    /// Records a delta against `previous`, updating it, and returns the increment (>= 0).
    private static func delta(_ current: Int, _ previous: NIOLockedValueBox<Int>) -> Int {
        previous.withLockedValue { prev in
            let delta = current - prev
            prev = current
            return delta > 0 ? delta : 0
        }
    }

    func updateFromStatistics(_ stats: RDKafkaStatistics) {
        self.queueMessages.record(stats.queueMessages)
        self.queueBytes.record(stats.queueMessagesSize)

        let messagesDelta = Self.delta(stats.messagesSentTotal, self.previousMessagesSent)
        if messagesDelta > 0 { self.messagesSent.increment(by: messagesDelta) }

        let bytesDelta = Self.delta(stats.bytesSentTotal, self.previousBytesSent)
        if bytesDelta > 0 { self.bytesSent.increment(by: bytesDelta) }

        var totalBatchSize = 0
        var topicCount = 0
        for topic in stats.topics.values {
            if let avg = topic.batchBytes?.avg {
                totalBatchSize += avg
                topicCount += 1
            }
        }
        if topicCount > 0 {
            self.batchSizeAvg.record(totalBatchSize / topicCount)
        }

        for broker in stats.brokers.values {
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerRTTLabel,
                broker: broker.name,
                divisorToMilliseconds: 1000,
                broker.roundTripTime
            )
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerThrottleLabel,
                broker: broker.name,
                divisorToMilliseconds: 1,
                broker.throttleTime
            )
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerQueueLatencyLabel,
                broker: broker.name,
                divisorToMilliseconds: 1000,
                broker.internalLatency
            )
            KafkaMetricSupport.recordWindow(
                baseLabel: self.brokerRequestLatencyLabel,
                broker: broker.name,
                divisorToMilliseconds: 1000,
                broker.outbufLatency
            )
        }
    }
}
