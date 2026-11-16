package dev.ocpplab.ws

import org.springframework.context.annotation.Configuration
import org.springframework.web.socket.config.annotation.EnableWebSocket
import org.springframework.web.socket.config.annotation.WebSocketConfigurer
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry
import org.springframework.web.socket.server.support.DefaultHandshakeHandler

const val OCPP16_SUBPROTOCOL = "ocpp1.6"

@Configuration
@EnableWebSocket
class WebSocketConfig(
    private val handler: OcppWebSocketHandler,
) : WebSocketConfigurer {

    override fun registerWebSocketHandlers(registry: WebSocketHandlerRegistry) {
        // Only "ocpp1.6" is negotiated. If the client does not offer it, the handshake
        // still completes, but without a Sec-WebSocket-Protocol header; the handler
        // then closes the connection (OCPP-J 1.6, section 3.2).
        val handshakeHandler = DefaultHandshakeHandler().apply {
            setSupportedProtocols(OCPP16_SUBPROTOCOL)
        }

        registry.addHandler(handler, "/ocpp/{chargePointId}")
            .setHandshakeHandler(handshakeHandler)
            .addInterceptors(ChargePointIdInterceptor())
            .setAllowedOriginPatterns("*")
    }
}
