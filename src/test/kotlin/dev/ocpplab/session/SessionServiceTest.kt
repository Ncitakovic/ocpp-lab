package dev.ocpplab.session

import dev.ocpplab.TestcontainersConfiguration
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.math.BigDecimal
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Runs against a real Postgres (Testcontainers). Every test creates its own session,
 * so tests do not interfere with each other and no cleanup is needed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(TestcontainersConfiguration::class)
class SessionServiceTest @Autowired constructor(
    private val service: SessionService,
    private val sessions: ChargingSessionRepository,
    private val meterValues: MeterValueRepository,
) {
    private val startedAt = Instant.parse("2026-11-20T09:00:00Z")
    private val stoppedAt = Instant.parse("2026-11-20T10:00:00Z")

    private fun openSession(): Int =
        service.startSession("CP001", 1, "TAG-1", meterStart = 1000, startedAt = startedAt).transactionId!!

    private fun load(transactionId: Int): ChargingSession = sessions.findById(transactionId).orElseThrow()

    /** Already implemented; must pass. */
    @Nested
    inner class StartAndMeterValues {

        @Test
        fun `startSession stores an OPEN session with a generated transactionId`() {
            val session = service.startSession("CP001", 2, "TAG-9", meterStart = 1234, startedAt = startedAt)

            val id = assertNotNull(session.transactionId)
            val stored = load(id)
            assertEquals(SessionStatus.OPEN, stored.status)
            assertEquals("CP001", stored.chargePointId)
            assertEquals(2, stored.connectorId)
            assertEquals("TAG-9", stored.idTag)
            assertEquals(1234, stored.meterStart)
            assertEquals(startedAt, stored.startedAt)
            assertNull(stored.meterStop)
            assertNull(stored.stoppedAt)
            assertNull(stored.stopReason)
        }

        @Test
        fun `transactionIds are unique`() {
            assertTrue(openSession() != openSession())
        }

        @Test
        fun `recordMeterValue stores readings for an existing transaction`() {
            val id = openSession()

            assertTrue(service.recordMeterValue(id, startedAt.plusSeconds(60), BigDecimal("1500")))
            assertTrue(service.recordMeterValue(id, startedAt.plusSeconds(120), BigDecimal("2000.5")))

            val stored = meterValues.findByTransactionIdOrderByTimestamp(id)
            assertEquals(listOf(BigDecimal("1500"), BigDecimal("2000.5")), stored.map { it.valueWh })
        }

        @Test
        fun `recordMeterValue for an unknown transaction stores nothing`() {
            assertFalse(service.recordMeterValue(-1, startedAt, BigDecimal.ONE))
        }
    }

    /** Fail until stopSession / closeIfOpen are implemented. */
    @Nested
    inner class StopSession {

        @Test
        fun `first StopTransaction closes the session`() {
            val id = openSession()

            val outcome = service.stopSession(id, meterStop = 5000, stoppedAt = stoppedAt, reason = "Local")

            assertEquals(StopOutcome.Closed, outcome)
            val stored = load(id)
            assertEquals(SessionStatus.CLOSED, stored.status)
            assertEquals(5000, stored.meterStop)
            assertEquals(stoppedAt, stored.stoppedAt)
            assertEquals("Local", stored.stopReason)
        }

        @Test
        fun `same StopTransaction a second time is AlreadyClosed and changes nothing`() {
            val id = openSession()
            service.stopSession(id, 5000, stoppedAt, "Local")

            val outcome = service.stopSession(id, 5000, stoppedAt, "Local")

            assertEquals(StopOutcome.AlreadyClosed, outcome)
            val stored = load(id)
            assertEquals(SessionStatus.CLOSED, stored.status)
            assertEquals(5000, stored.meterStop)
            assertEquals(stoppedAt, stored.stoppedAt)
        }

        @Test
        fun `later StopTransaction with different values does not overwrite the closed session`() {
            val id = openSession()
            service.stopSession(id, 5000, stoppedAt, "Local")

            val outcome = service.stopSession(id, 9999, stoppedAt.plusSeconds(600), "Remote")

            assertEquals(StopOutcome.AlreadyClosed, outcome)
            val stored = load(id)
            assertEquals(5000, stored.meterStop)
            assertEquals(stoppedAt, stored.stoppedAt)
            assertEquals("Local", stored.stopReason)
        }

        @Test
        fun `unknown transactionId is UnknownTransaction`() {
            assertEquals(StopOutcome.UnknownTransaction, service.stopSession(-1, 5000, stoppedAt, "Local"))
        }

        @Test
        fun `two identical concurrent StopTransactions close the session exactly once`() {
            val id = openSession()
            val threads = 2
            val ready = CountDownLatch(threads)
            val go = CountDownLatch(1)
            val pool = Executors.newFixedThreadPool(threads)

            try {
                val futures = (1..threads).map {
                    pool.submit(
                        Callable {
                            ready.countDown()
                            go.await() // both threads wait here and are released at the same moment
                            service.stopSession(id, 5000, stoppedAt, "Local")
                        },
                    )
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS), "workers did not start")
                go.countDown()

                val outcomes = futures.map { it.get(10, TimeUnit.SECONDS) }

                assertEquals(1, outcomes.count { it == StopOutcome.Closed }, "outcomes: $outcomes")
                assertEquals(1, outcomes.count { it == StopOutcome.AlreadyClosed }, "outcomes: $outcomes")
                assertEquals(5000, load(id).meterStop)
            } finally {
                pool.shutdownNow()
            }
        }
    }
}
