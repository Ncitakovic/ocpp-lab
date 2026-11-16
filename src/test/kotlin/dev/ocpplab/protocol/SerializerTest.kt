package dev.ocpplab.protocol

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SerializerTest {

    private val mapper = jacksonObjectMapper()

    @Test
    fun `serializes CALL`() {
        val msg = Call("1", "Heartbeat", mapper.createObjectNode())

        assertEquals("""[2,"1","Heartbeat",{}]""", msg.toJson(mapper))
    }

    @Test
    fun `serializes CALLRESULT`() {
        val msg = CallResult("1", mapper.createObjectNode().put("status", "Accepted"))

        assertEquals("""[3,"1",{"status":"Accepted"}]""", msg.toJson(mapper))
    }

    @Test
    fun `serializes CALLERROR using the wire name of the error code`() {
        val msg = CallError("1", OcppErrorCode.OccurenceConstraintViolation, "bad", mapper.createObjectNode())

        assertEquals("""[4,"1","OccurenceConstraintViolation","bad",{}]""", msg.toJson(mapper))
    }
}
