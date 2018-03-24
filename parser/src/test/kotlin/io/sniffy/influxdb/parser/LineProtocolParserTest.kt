package io.sniffy.influxdb.parser

import io.sniffy.influxdb.lineprotocol.FieldFloatValue
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class LineProtocolParserTest {

    @Test
    fun simpleTest() {
        assertTrue(true)
    }

    @Test
    fun parseOneValidLine() {
        val parser = LineProtocolParser("weather,location=us-midwest temperature=82 1465839830100400200")

        assertTrue(parser.hasNext())

        val point = parser.next()

        assertEquals("weather", point.measurement)
        assertEquals(mapOf("location" to "us-midwest"), point.tags)
        assertEquals(mapOf("temperature" to FieldFloatValue(82.0)), point.values)
        assertEquals(1465839830100400200, point.timestamp)

        assertFalse(parser.hasNext())
    }

    @Test
    fun hasNextSkippedThrowsException() {
        val parser = LineProtocolParser("weather,location=us-midwest temperature=82 1465839830100400200")

        assertThrows(NoSuchElementException::class.java, {
            parser.next()
        })
    }

}