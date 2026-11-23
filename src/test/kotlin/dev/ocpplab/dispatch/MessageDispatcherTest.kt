package dev.ocpplab.dispatch

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import dev.ocpplab.protocol.Call
import dev.ocpplab.protocol.CallError
import dev.ocpplab.protocol.CallResult
import dev.ocpplab.protocol.MessageParser
import dev.ocpplab.protocol.OcppErrorCode
import dev.ocpplab.protocol.OcppMessage
import dev.ocpplab.protocol.OcppParseException
import dev.ocpplab.session.ChargingSession
import dev.ocpplab.session.SessionService
import dev.ocpplab.session.StopOutcome
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The parser and the session service are faked, so these tests depend neither on
 * the real `parse` nor on a database; each test decides what the "parsed" message is.
 */
class MessageDispatcherTest {

    private val json = JsonNodeFactory.instance

    // Same defaults as Spring Boot's ObjectMapper (Kotlin + java.time modules, unknown properties ignored).
    private val mapper = Jackson2ObjectMapperBuilder.json().build<ObjectMapper>()

    private val fixedInstant = Instant.parse("2026-11-16T10:15:30Z")
    private val clock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

    /** Records calls and returns preconfigured answers. */
    private class FakeSessionService(
        var stopOutcome: StopOutcome = StopOutcome.Closed,
        var nextTransactionId: Int = 42,
        var knownTransactions: Set<Int> = setOf(42),
        var failWith: Exception? = null,
    ) : SessionService {
        val started = mutableListOf<ChargingSession>()
        val meterValues = mutableListOf<Triple<Int, Instant, BigDecimal>>()
        val stops = mutableListOf<List<Any>>()

        override fun startSession(chargePointId: String, connectorId: Int, idTag: String, meterStart: Int, startedAt: Instant): ChargingSession {
            failWith?.let { throw it }
            return ChargingSession(nextTransactionId, chargePointId, connectorId, idTag, meterStart, startedAt = startedAt)
                .also { started += it }
        }

        override fun recordMeterValue(transactionId: Int, timestamp: Instant, valueWh: BigDecimal): Boolean {
            if (transactionId !in knownTransactions) return false
            meterValues += Triple(transactionId, timestamp, valueWh)
            return true
        }

        override fun stopSession(transactionId: Int, meterStop: Int, stoppedAt: Instant, reason: String): StopOutcome {
            failWith?.let { throw it }
            stops += listOf(transactionId, meterStop, stoppedAt, reason)
            return stopOutcome
        }
    }

    private val sessions = FakeSessionService()
    private val connectorStatuses = ConnectorStatusRegistry()

    private fun dispatcher(parser: MessageParser, clock: Clock = this.clock) =
        MessageDispatcher(clock, parser, mapper, sessions, connectorStatuses)

    private fun dispatcherReturning(message: OcppMessage, clock: Clock = this.clock) =
        dispatcher(MessageParser { message }, clock)

    private fun dispatcherThrowing(ex: OcppParseException) =
        dispatcher(MessageParser { throw ex })

    /** Dispatches a CALL with the given action and JSON payload. */
    private fun call(action: String, payloadJson: String, id: String = "m-1"): OcppMessage? =
        dispatcherReturning(Call(id, action, mapper.readTree(payloadJson))).dispatch("CP001", "<raw>")

    @Nested
    inner class BootAndHeartbeat {

        @Test
        fun `BootNotification is accepted with current time and 60s interval`() {
            val boot = Call(
                "boot-1",
                "BootNotification",
                json.objectNode().put("chargePointVendor", "VendorX").put("chargePointModel", "ModelY"),
            )

            val response = assertIs<CallResult>(dispatcherReturning(boot).dispatch("CP001", "<raw>"))

            assertEquals("boot-1", response.id)
            assertEquals("Accepted", response.payload["status"].asText())
            assertEquals("2026-11-16T10:15:30Z", response.payload["currentTime"].asText())
            assertEquals(60, response.payload["interval"].asInt())
            assertEquals(3, response.payload.size())
        }

        @Test
        fun `Heartbeat returns current time`() {
            val heartbeat = Call("hb-1", "Heartbeat", json.objectNode())

            val response = assertIs<CallResult>(dispatcherReturning(heartbeat).dispatch("CP001", "<raw>"))

            assertEquals("hb-1", response.id)
            assertEquals("2026-11-16T10:15:30Z", response.payload["currentTime"].asText())
            assertEquals(1, response.payload.size())
        }

        @Test
        fun `currentTime is UTC even if the clock zone is not`() {
            val belgradeClock = Clock.fixed(fixedInstant, ZoneId.of("Europe/Belgrade"))
            val heartbeat = Call("hb-2", "Heartbeat", json.objectNode())

            val response = assertIs<CallResult>(dispatcherReturning(heartbeat, belgradeClock).dispatch("CP001", "<raw>"))

            assertEquals("2026-11-16T10:15:30Z", response.payload["currentTime"].asText())
        }

        @Test
        fun `currentTime keeps millisecond precision`() {
            val clockWithMillis = Clock.fixed(Instant.parse("2026-11-16T10:15:30.123456Z"), ZoneOffset.UTC)
            val heartbeat = Call("hb-3", "Heartbeat", json.objectNode())

            val response = assertIs<CallResult>(dispatcherReturning(heartbeat, clockWithMillis).dispatch("CP001", "<raw>"))

            assertEquals("2026-11-16T10:15:30.123Z", response.payload["currentTime"].asText())
        }
    }

    @Nested
    inner class ErrorsAndUnknownMessages {

        @Test
        fun `unknown action returns NotImplemented`() {
            val call = Call("x-1", "FlyToTheMoon", json.objectNode())

            val response = assertIs<CallError>(dispatcherReturning(call).dispatch("CP001", "<raw>"))

            assertEquals("x-1", response.id)
            assertEquals(OcppErrorCode.NotImplemented, response.errorCode)
            assertEquals(0, response.details.size())
        }

        @Test
        fun `parse error with known id returns FormationViolation`() {
            val dispatcher = dispatcherThrowing(OcppParseException("bad-1", "CALL must have 4 elements"))

            val response = assertIs<CallError>(dispatcher.dispatch("CP001", "<raw>"))

            assertEquals("bad-1", response.id)
            assertEquals(OcppErrorCode.FormationViolation, response.errorCode)
            assertEquals("CALL must have 4 elements", response.description)
        }

        @Test
        fun `parse error without id produces no response`() {
            val dispatcher = dispatcherThrowing(OcppParseException(null, "not JSON"))

            assertNull(dispatcher.dispatch("CP001", "<raw>"))
        }

        @Test
        fun `incoming CALLRESULT and CALLERROR produce no response`() {
            assertNull(dispatcherReturning(CallResult("r-1", json.objectNode())).dispatch("CP001", "<raw>"))
            assertNull(
                dispatcherReturning(CallError("e-1", OcppErrorCode.GenericError, "x", json.objectNode()))
                    .dispatch("CP001", "<raw>"),
            )
        }

        @Test
        fun `missing required field returns FormationViolation`() {
            val response = assertIs<CallError>(call("StartTransaction", """{"connectorId":1,"idTag":"TAG-1","timestamp":"2026-11-20T09:00:00Z"}"""))

            assertEquals(OcppErrorCode.FormationViolation, response.errorCode)
            assertTrue(sessions.started.isEmpty())
        }

        @Test
        fun `failure in the session service returns InternalError`() {
            sessions.failWith = IllegalStateException("database is down")

            val response = assertIs<CallError>(
                call("StartTransaction", """{"connectorId":1,"idTag":"TAG-1","meterStart":0,"timestamp":"2026-11-20T09:00:00Z"}"""),
            )

            assertEquals(OcppErrorCode.InternalError, response.errorCode)
        }
    }

    @Nested
    inner class Transactions {

        @Test
        fun `Authorize always accepts`() {
            val response = assertIs<CallResult>(call("Authorize", """{"idTag":"ANY-TAG"}"""))

            assertEquals("Accepted", response.payload["idTagInfo"]["status"].asText())
        }

        @Test
        fun `StatusNotification stores the last status per connector and returns empty payload`() {
            call("StatusNotification", """{"connectorId":1,"errorCode":"NoError","status":"Preparing","timestamp":"2026-11-20T09:00:00Z"}""")
            val response = assertIs<CallResult>(
                call("StatusNotification", """{"connectorId":1,"errorCode":"NoError","status":"Charging","timestamp":"2026-11-20T09:01:00Z"}"""),
            )

            assertEquals(0, response.payload.size())
            assertEquals(
                ConnectorStatus("Charging", "NoError", Instant.parse("2026-11-20T09:01:00Z")),
                connectorStatuses.get("CP001", 1),
            )
        }

        @Test
        fun `StatusNotification without timestamp uses the clock`() {
            call("StatusNotification", """{"connectorId":2,"errorCode":"NoError","status":"Available"}""")

            assertEquals(fixedInstant, connectorStatuses.get("CP001", 2)?.timestamp)
        }

        @Test
        fun `StartTransaction opens a session and returns its transactionId`() {
            val response = assertIs<CallResult>(
                call("StartTransaction", """{"connectorId":1,"idTag":"TAG-1","meterStart":1000,"timestamp":"2026-11-20T09:00:00Z"}"""),
            )

            assertEquals(42, response.payload["transactionId"].asInt())
            assertEquals("Accepted", response.payload["idTagInfo"]["status"].asText())
            val started = sessions.started.single()
            assertEquals("CP001", started.chargePointId)
            assertEquals(1, started.connectorId)
            assertEquals("TAG-1", started.idTag)
            assertEquals(1000, started.meterStart)
            assertEquals(Instant.parse("2026-11-20T09:00:00Z"), started.startedAt)
        }

        @Test
        fun `MeterValues stores only the energy import register, converting kWh to Wh`() {
            val payload = """
                {"connectorId":1,"transactionId":42,"meterValue":[
                  {"timestamp":"2026-11-20T09:05:00Z","sampledValue":[
                    {"value":"1500"},
                    {"value":"2.5","measurand":"Energy.Active.Import.Register","unit":"kWh"},
                    {"value":"500","measurand":"Energy.Active.Import.Register","phase":"L1"},
                    {"value":"16.0","measurand":"Current.Import","unit":"A"}
                  ]}
                ]}
            """.trimIndent()

            val response = assertIs<CallResult>(call("MeterValues", payload))

            assertEquals(0, response.payload.size())
            assertEquals(
                listOf(BigDecimal("1500"), BigDecimal("2500.0")),
                sessions.meterValues.map { it.third },
            )
            assertTrue(sessions.meterValues.all { it.first == 42 && it.second == Instant.parse("2026-11-20T09:05:00Z") })
        }

        @Test
        fun `MeterValues for unknown transaction still returns empty CallResult`() {
            val response = assertIs<CallResult>(
                call("MeterValues", """{"connectorId":1,"transactionId":999,"meterValue":[{"timestamp":"2026-11-20T09:05:00Z","sampledValue":[{"value":"1"}]}]}"""),
            )

            assertEquals(0, response.payload.size())
            assertTrue(sessions.meterValues.isEmpty())
        }

        private val stopPayload =
            """{"transactionId":42,"meterStop":5000,"timestamp":"2026-11-20T10:00:00Z","idTag":"TAG-1","reason":"EVDisconnected"}"""

        @Test
        fun `StopTransaction passes the values to the session service`() {
            val response = assertIs<CallResult>(call("StopTransaction", stopPayload))

            assertEquals(listOf<Any>(42, 5000, Instant.parse("2026-11-20T10:00:00Z"), "EVDisconnected"), sessions.stops.single())
            assertEquals("Accepted", response.payload["idTagInfo"]["status"].asText())
        }

        @Test
        fun `duplicate StopTransaction gets a CallResult, not a CallError`() {
            sessions.stopOutcome = StopOutcome.AlreadyClosed

            val response = call("StopTransaction", stopPayload, id = "stop-2")

            val result = assertIs<CallResult>(response)
            assertEquals("stop-2", result.id)
        }

        @Test
        fun `StopTransaction for unknown transaction gets a CallResult, not a CallError`() {
            sessions.stopOutcome = StopOutcome.UnknownTransaction

            assertIs<CallResult>(call("StopTransaction", stopPayload))
        }

        @Test
        fun `StopTransaction without reason defaults to Local and without idTag returns empty payload`() {
            val response = assertIs<CallResult>(
                call("StopTransaction", """{"transactionId":42,"meterStop":5000,"timestamp":"2026-11-20T10:00:00Z"}"""),
            )

            assertEquals("Local", sessions.stops.single()[3])
            assertEquals(0, response.payload.size())
        }
    }
}
