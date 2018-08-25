package io.sniffy.influxdb.lineprotocol;

import org.junit.Test;

import static org.junit.Assert.*;

public class FiledFloatValueTest {

    @Test
    public void testAsBoolean() {
        assertTrue(new FieldFloatValue(1.).asBoolean());
        assertFalse(new FieldFloatValue(0.).asBoolean());
    }

    @Test
    public void testAsLong() {
        assertEquals(1L, new FieldFloatValue(1.).asLong());
        assertEquals(0L, new FieldFloatValue(0.).asLong());
    }

    @Test
    public void testAsDouble() {
        assertEquals(1., new FieldFloatValue(1.).asDouble(), 0.1);
        assertEquals(0., new FieldFloatValue(0.).asDouble(), 0.1);
    }

    @Test
    public void testAsString() {
        assertEquals("1.0", new FieldFloatValue(1.).asString());
        assertEquals("0.0", new FieldFloatValue(0.).asString());
    }

}
