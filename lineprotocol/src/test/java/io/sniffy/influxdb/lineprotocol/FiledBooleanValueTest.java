package io.sniffy.influxdb.lineprotocol;

import org.junit.Test;

import static org.junit.Assert.*;

public class FiledBooleanValueTest {

    @Test
    public void testAsBoolean() {
        assertTrue(new FieldBooleanValue(true).asBoolean());
        assertFalse(new FieldBooleanValue(false).asBoolean());
    }

    @Test
    public void testAsLong() {
        assertEquals(1L, new FieldBooleanValue(true).asLong());
        assertEquals(0L, new FieldBooleanValue(false).asLong());
    }

    @Test
    public void testAsDouble() {
        assertEquals(1., new FieldBooleanValue(true).asDouble(), 0.1);
        assertEquals(0., new FieldBooleanValue(false).asDouble(), 0.1);
    }

    @Test
    public void testAsString() {
        assertEquals("true", new FieldBooleanValue(true).asString());
        assertEquals("false", new FieldBooleanValue(false).asString());
    }

}
