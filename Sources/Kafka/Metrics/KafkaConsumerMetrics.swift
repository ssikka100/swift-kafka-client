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

/// Holds all consumer metric instruments and provides thread-safe recording methods.
final class KafkaConsumerMetrics: Sendable {
    private let lagMax: Gauge
    private let lagLabel: String
    private let errors: Counter
    private let rebalances: Counter
    private let commits: Counter
    private let commitsFailed: Counter
    private let commitDuration: Timer
    private let messagesReceived: Counter
    private let bytesReceived: Counter
    private let queueOperations: Gauge
    // Base labels (already prefixed) for per-broker latency windows.
    private let brokerRTTLabel: String
    private let brokerThrottleLabel: String

    // librdkafka reports cumulative totals; we emit deltas as Counter increments.
    private let previousMessagesReceived: NIOLockedValueBox<Int>
    private let previousBytesReceived: NIOLockedValueBox<Int>
    private let previousRebalances: NIOLockedValueBox<Int>

    init(prefix: String) {
        self.lagMax = Gauge(label: "\(prefix).\(KafkaMetricLabels.consumerLagMax)")
        self.lagLabel = "\(prefix).\(KafkaMetricLabels.consumerLag)"
        self.errors = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerErrors)")
        self.rebalances = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerRebalances)")
        self.commits = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerCommits)")
        self.commitsFailed = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerCommitsFailed)")
        self.commitDuration = Timer(label: "\(prefix).\(KafkaMetricLabels.consumerCommitDuration)")
        self.messagesReceived = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerMessagesReceived)")
        self.bytesReceived = Counter(label: "\(prefix).\(KafkaMetricLabels.consumerBytesReceived)")
        self.queueOperations = Gauge(label: "\(prefix).\(KafkaMetricLabels.consumerQueueOperations)")
        self.brokerRTTLabel = "\(prefix).\(KafkaMetricLabels.consumerBrokerRTT)"
        self.brokerThrottleLabel = "\(prefix).\(KafkaMetricLabels.consumerBrokerThrottle)"
        self.previousMessagesReceived = NIOLockedValueBox(0)
        self.previousBytesReceived = NIOLockedValueBox(0)
        self.previousRebalances = NIOLockedValueBox(0)
    }

    func recordError() {
        self.errors.increment()
    }

    func recordCommit(duration: Duration) {
        self.commits.increment()
        self.commitDuration.record(duration: duration)
    }

    func recordCommitFailure() {
        self.commitsFailed.increment()
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
        self.queueOperations.record(stats.queueOperations)

        let messagesDelta = Self.delta(stats.messagesReceivedTotal, self.previousMessagesReceived)
        if messagesDelta > 0 { self.messagesReceived.increment(by: messagesDelta) }

        let bytesDelta = Self.delta(stats.bytesReceivedTotal, self.previousBytesReceived)
        if bytesDelta > 0 { self.bytesReceived.increment(by: bytesDelta) }

        // Authoritative cgrp counter; counting assign/revoke events would double-count.
        if let rebalancesTotal = stats.consumerGroup?.rebalancesTotal {
            let rebalancesDelta = Self.delta(rebalancesTotal, self.previousRebalances)
            if rebalancesDelta > 0 { self.rebalances.increment(by: rebalancesDelta) }
        }

        var maxLag = 0
        for (topicName, topic) in stats.topics {
            for partition in (topic.partitions ?? [:]).values {
                let lag = partition.consumerLag
                guard lag >= 0 else { continue }
                Gauge(
                    label: self.lagLabel,
                    dimensions: [
                        (KafkaMetricLabels.topicDimension, topicName),
                        (KafkaMetricLabels.partitionDimension, "\(partition.partition)"),
                    ]
                ).record(lag)
                maxLag = max(maxLag, lag)
            }
        }
        self.lagMax.record(maxLag)

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
        }
    }
}
