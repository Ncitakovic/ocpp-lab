package dev.ocpplab

import dev.ocpplab.protocol.MessageParser
import dev.ocpplab.protocol.parse
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class OcppConfig {

    /** Injected everywhere time is needed, so tests can use Clock.fixed(...). */
    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun messageParser(): MessageParser = MessageParser { raw -> parse(raw) }
}
