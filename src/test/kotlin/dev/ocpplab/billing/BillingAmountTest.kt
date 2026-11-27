package dev.ocpplab.billing

import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

class BillingAmountTest {

    @Test
    fun `amount is energy in kWh times price, with 4 decimals`() {
        assertEquals(BigDecimal("1.3500"), computeAmount(4500, BigDecimal("0.30")))
        assertEquals(BigDecimal("0.0000"), computeAmount(0, BigDecimal("0.30")))
    }

    @Test
    fun `rounding is HALF_UP`() {
        // 1 Wh * 0.25 = 0.00025 -> HALF_UP gives 0.0003 (HALF_EVEN would give 0.0002)
        assertEquals(BigDecimal("0.0003"), computeAmount(1, BigDecimal("0.25")))
        // 1 Wh * 0.24 = 0.00024 -> 0.0002
        assertEquals(BigDecimal("0.0002"), computeAmount(1, BigDecimal("0.24")))
    }
}
