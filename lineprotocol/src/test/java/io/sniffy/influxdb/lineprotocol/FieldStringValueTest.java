package io.sniffy.influxdb.lineprotocol;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldStringValueTest {

    @Test
    public void testAsBoolean() {
        assertTrue(new FieldStringValue("true").asBoolean());
        assertFalse(new FieldStringValue("false").asBoolean());
        assertFalse(new FieldStringValue("").asBoolean());
        assertFalse(new FieldStringValue("some string").asBoolean());
    }

    @Test
    public void testAsLong() {
        assertEquals(1L, new FieldStringValue("1").asLong());
        assertEquals(1L, new FieldStringValue("1.0").asLong());
        assertEquals(0L, new FieldStringValue("0").asLong());
        assertEquals(0L, new FieldStringValue("0.").asLong());
        assertEquals(0L, new FieldStringValue("").asLong());
        assertEquals(0L, new FieldStringValue("foo").asLong());
    }

    @Test
    public void testAsDouble() {
        assertEquals(1., new FieldStringValue("1").asDouble(), 0.1);
        assertEquals(1., new FieldStringValue("1.0").asDouble(), 0.1);
        assertEquals(0., new FieldStringValue("0").asDouble(), 0.1);
        assertEquals(0., new FieldStringValue("0.").asDouble(), 0.1);
        assertEquals(0., new FieldStringValue("").asDouble(), 0.1);
        assertEquals(0., new FieldStringValue("bar").asDouble(), 0.1);
    }

    @Test
    public void testAsString() {
        assertEquals("bar", new FieldStringValue("bar").asString());
        assertEquals("", new FieldStringValue("").asString());
    }

}
