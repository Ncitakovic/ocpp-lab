package dev.ocpplab.ws

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.server.ServerHttpRequest
import org.springframework.http.server.ServerHttpResponse
import org.springframework.web.socket.WebSocketHandler
import org.springframework.web.socket.server.HandshakeInterceptor
import java.net.URLDecoder
import java.nio.charset.StandardCharsets

/**
 * Extracts the charge point id from `/ocpp/{chargePointId}` and stores it
 * in the WebSocket session attributes under [CHARGE_POINT_ID].
 */
class ChargePointIdInterceptor : HandshakeInterceptor {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun beforeHandshake(
        request: ServerHttpRequest,
        response: ServerHttpResponse,
        wsHandler: WebSocketHandler,
        attributes: MutableMap<String, Any>,
    ): Boolean {
        val chargePointId = extractChargePointId(request.uri.rawPath)
        if (chargePointId == null) {
            log.warn("Rejecting handshake, no charge point id in path {}", request.uri.path)
            response.setStatusCode(HttpStatus.BAD_REQUEST)
            return false
        }
        attributes[CHARGE_POINT_ID] = chargePointId
        return true
    }

    override fun afterHandshake(
        request: ServerHttpRequest,
        response: ServerHttpResponse,
        wsHandler: WebSocketHandler,
        exception: Exception?,
    ) = Unit

    companion object {
        const val CHARGE_POINT_ID = "chargePointId"

        /** The handler mapping only matches `/ocpp/{chargePointId}`, so the id is the last path segment. */
        fun extractChargePointId(rawPath: String): String? =
            URLDecoder.decode(rawPath.substringAfterLast('/'), StandardCharsets.UTF_8)
                .takeIf { it.isNotBlank() }
    }
}
