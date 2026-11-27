package dev.ocpplab.outbox

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaConfig {

    /** Created by KafkaAdmin on startup if missing. Key = transactionId, so one session stays in one partition. */
    @Bean
    fun chargingSessionEventsTopic(@Value("\${outbox.topic}") topic: String) =
        TopicBuilder.name(topic).partitions(3).replicas(1).build()

    /**
     * Listener errors (e.g. the database is down) are retried every second, forever. The default
     * handler gives up after 10 tries and skips the record, which would lose a bill.
     * Known-bad input (unreadable JSON, duplicates) is handled in BillingListener and BillingService and never gets here,
     * so it cannot block the partition.
     */
    @Bean
    fun kafkaErrorHandler(): CommonErrorHandler =
        DefaultErrorHandler(FixedBackOff(1000L, FixedBackOff.UNLIMITED_ATTEMPTS))
}

/** Runs the relay periodically. Tests set outbox.relay.enabled=false and call publishPending() themselves. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = ["outbox.relay.enabled"], havingValue = "true", matchIfMissing = true)
class OutboxRelayScheduler(private val relay: OutboxRelay) {

    @Scheduled(fixedDelayString = "\${outbox.relay.interval-ms}")
    fun publish() {
        relay.publishPending()
    }
}
