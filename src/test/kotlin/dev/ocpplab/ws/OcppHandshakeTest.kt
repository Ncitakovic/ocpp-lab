package dev.ocpplab.ws

import dev.ocpplab.TestcontainersConfiguration
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.WebSocketHttpHeaders
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.net.URI
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Exercises only the handshake / subprotocol negotiation, so it does not
 * depend on the parser implementation.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration::class)
class OcppHandshakeTest(@param:LocalServerPort private val port: Int) {

    private class RecordingHandler : TextWebSocketHandler() {
        val closed = CompletableFuture<CloseStatus>()
        override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
            closed.complete(status)
        }
    }

    private fun connect(handler: RecordingHandler, vararg subprotocols: String): WebSocketSession {
        val headers = WebSocketHttpHeaders()
        if (subprotocols.isNotEmpty()) headers.secWebSocketProtocol = subprotocols.toList()
        return StandardWebSocketClient()
            .execute(handler, headers, URI.create("ws://localhost:$port/ocpp/CP001"))
            .get(5, TimeUnit.SECONDS)
    }

    @Test
    fun `accepts ocpp1_6 subprotocol`() {
        val session = connect(RecordingHandler(), "ocpp1.6")

        assertEquals("ocpp1.6", session.acceptedProtocol)
        assertTrue(session.isOpen)
        session.close()
    }

    @Test
    fun `picks ocpp1_6 when offered among others`() {
        val session = connect(RecordingHandler(), "ocpp2.0.1", "ocpp1.6")

        assertEquals("ocpp1.6", session.acceptedProtocol)
        session.close()
    }

    @Test
    fun `completes handshake without subprotocol and then closes when ocpp1_6 is not offered`() {
        val handler = RecordingHandler()

        val session = connect(handler)

        assertTrue(session.acceptedProtocol.isNullOrEmpty(), "server must not send Sec-WebSocket-Protocol")
        assertEquals(CloseStatus.PROTOCOL_ERROR.code, handler.closed.get(5, TimeUnit.SECONDS).code)
    }

    @Test
    fun `closes when only an unsupported subprotocol is offered`() {
        val handler = RecordingHandler()

        val session = connect(handler, "ocpp2.0.1")

        assertTrue(session.acceptedProtocol.isNullOrEmpty())
        assertEquals(CloseStatus.PROTOCOL_ERROR.code, handler.closed.get(5, TimeUnit.SECONDS).code)
    }
}
