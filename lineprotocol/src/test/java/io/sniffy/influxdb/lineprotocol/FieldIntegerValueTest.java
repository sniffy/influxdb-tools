package io.sniffy.influxdb.lineprotocol;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldIntegerValueTest {

    @Test
    public void testAsBoolean() {
        assertTrue(new FieldIntegerValue(1).asBoolean());
        assertFalse(new FieldIntegerValue(0).asBoolean());
    }

    @Test
    public void testAsLong() {
        assertEquals(1L, new FieldIntegerValue(1).asLong());
        assertEquals(0L, new FieldIntegerValue(0).asLong());
    }

    @Test
    public void testAsDouble() {
        assertEquals(1., new FieldIntegerValue(1).asDouble(), 0.1);
        assertEquals(0., new FieldIntegerValue(0).asDouble(), 0.1);
    }

    @Test
    public void testAsString() {
        assertEquals("1", new FieldIntegerValue(1).asString());
        assertEquals("0", new FieldIntegerValue(0).asString());
    }

}
