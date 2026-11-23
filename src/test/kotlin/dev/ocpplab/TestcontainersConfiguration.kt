package dev.ocpplab

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * Postgres 16 and a single KRaft Kafka broker in Docker for tests. @ServiceConnection points the
 * datasource and spring.kafka.bootstrap-servers at them, and Flyway runs the migrations on startup.
 * Import it with @Import(TestcontainersConfiguration::class).
 */
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    fun postgres(): PostgreSQLContainer<*> = PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"))

    @Bean
    @ServiceConnection
    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
}
