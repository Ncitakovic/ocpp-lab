package dev.ocpplab.dispatch

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import dev.ocpplab.protocol.Call
import dev.ocpplab.protocol.CallError
import dev.ocpplab.protocol.CallResult
import dev.ocpplab.protocol.MessageParser
import dev.ocpplab.protocol.OcppErrorCode
import dev.ocpplab.protocol.OcppMessage
import dev.ocpplab.protocol.OcppParseException
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/**
 * The parser is stubbed so these tests do not depend on the real `parse`
 * implementation; each test decides what the "parsed" message is.
 */
class MessageDispatcherTest {

    private val json = JsonNodeFactory.instance
    private val fixedInstant = Instant.parse("2026-11-16T10:15:30Z")
    private val clock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

    private fun dispatcherReturning(message: OcppMessage, clock: Clock = this.clock) =
        MessageDispatcher(clock, MessageParser { message })

    private fun dispatcherThrowing(ex: OcppParseException) =
        MessageDispatcher(clock, MessageParser { throw ex })

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
}
