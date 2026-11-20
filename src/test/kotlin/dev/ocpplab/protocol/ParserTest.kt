package dev.ocpplab.protocol

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

/**
 * Written ahead of the implementation: these fail until `parse` is implemented.
 */
class ParserTest {

    @Nested
    inner class ValidMessages {

        @Test
        fun `parses CALL`() {
            val raw = """[2,"19223201","BootNotification",{"chargePointVendor":"VendorX","chargePointModel":"SingleSocketCharger"}]"""

            val call = assertIs<Call>(parse(raw))

            assertEquals("19223201", call.id)
            assertEquals("BootNotification", call.action)
            assertEquals("VendorX", call.payload["chargePointVendor"].asText())
            assertEquals("SingleSocketCharger", call.payload["chargePointModel"].asText())
        }

        @Test
        fun `parses CALL with empty payload`() {
            val call = assertIs<Call>(parse("""[2,"abc","Heartbeat",{}]"""))

            assertEquals("abc", call.id)
            assertEquals("Heartbeat", call.action)
            assertEquals(0, call.payload.size())
        }

        @Test
        fun `parses CALLRESULT`() {
            val raw = """[3,"19223201",{"status":"Accepted","currentTime":"2013-02-01T20:53:32.486Z","interval":300}]"""

            val result = assertIs<CallResult>(parse(raw))

            assertEquals("19223201", result.id)
            assertEquals("Accepted", result.payload["status"].asText())
            assertEquals(300, result.payload["interval"].asInt())
        }

        @Test
        fun `parses CALLERROR`() {
            val raw = """[4,"162376037","NotSupported","SetDisplayMessageRequest not implemented",{"hint":"x"}]"""

            val error = assertIs<CallError>(parse(raw))

            assertEquals("162376037", error.id)
            assertEquals(OcppErrorCode.NotSupported, error.errorCode)
            assertEquals("SetDisplayMessageRequest not implemented", error.description)
            assertEquals("x", error.details["hint"].asText())
        }

        @Test
        fun `accepts id of exactly 36 characters`() {
            val id = "a".repeat(36)

            val call = assertIs<Call>(parse("""[2,"$id","Heartbeat",{}]"""))

            assertEquals(id, call.id)
        }
    }

    @Nested
    inner class InvalidMessagesWithoutId {

        @ParameterizedTest
        @ValueSource(
            strings = [
                """[2,"abc","Heartbeat",{""",
                """not json at all""",
                """""",
            ],
        )
        fun `broken JSON has no messageId`(raw: String) {
            val ex = assertFailsWith<OcppParseException> { parse(raw) }

            assertNull(ex.messageId)
        }

        @ParameterizedTest
        @ValueSource(
            strings = [
                """{"messageTypeId":2,"id":"abc"}""",
                """"just a string"""",
                """42""",
            ],
        )
        fun `non-array has no messageId`(raw: String) {
            val ex = assertFailsWith<OcppParseException> { parse(raw) }

            assertNull(ex.messageId)
        }

        @ParameterizedTest
        @ValueSource(strings = ["""[]""", """[2]"""])
        fun `too few elements to read an id has no messageId`(raw: String) {
            val ex = assertFailsWith<OcppParseException> { parse(raw) }

            assertNull(ex.messageId)
        }
    }

    @Nested
    inner class InvalidMessagesWithId {

        @Test
        fun `unknown message type carries the id`() {
            val ex = assertFailsWith<OcppParseException> { parse("""[7,"abc",{}]""") }

            assertEquals("abc", ex.messageId)
        }

        @ParameterizedTest
        @ValueSource(
            strings = [
                """[2,"abc","Heartbeat"]""",
                """[2,"abc","Heartbeat",{},{}]""",
                """[3,"abc",{},{}]""",
                """[4,"abc","GenericError",{}]""",
                """[4,"abc","GenericError","desc",{},{}]""",
            ],
        )
        fun `wrong element count for the type carries the id`(raw: String) {
            val ex = assertFailsWith<OcppParseException> { parse(raw) }

            assertEquals("abc", ex.messageId)
        }
    }

    @Nested
    inner class FieldTypes {

        @ParameterizedTest
        @ValueSource(
            strings = [
                """["2","abc","Heartbeat",{}]""",
                """[2.5,"abc","Heartbeat",{}]""",
                """[true,"abc","Heartbeat",{}]""",
            ],
        )
        fun `MessageTypeId must be an integer`(raw: String) {
            val ex = assertFailsWith<OcppParseException> { parse(raw) }

            assertEquals("abc", ex.messageId)
        }

        @ParameterizedTest
        @ValueSource(
            strings = [
                """[2,"abc",42,{}]""",
                """[2,"abc","Heartbeat",[]]""",
                """[2,"abc","Heartbeat","{}"]""",
                """[3,"abc",null]""",
                """[4,"abc","GenericError","desc","details"]""",
                """[4,"abc","NoSuchCode","desc",{}]""",
            ],
        )
        fun `wrongly typed fields carry the id`(raw: String) {
            val ex = assertFailsWith<OcppParseException> { parse(raw) }

            assertEquals("abc", ex.messageId)
        }

        @Test
        fun `trailing garbage after the array is rejected`() {
            assertFailsWith<OcppParseException> { parse("""[2,"abc","Heartbeat",{}] trailing""") }
        }
    }

    @Test
    fun `id longer than 36 characters is rejected`() {
        val id = "a".repeat(37)

        assertFailsWith<OcppParseException> { parse("""[2,"$id","Heartbeat",{}]""") }
    }
}
