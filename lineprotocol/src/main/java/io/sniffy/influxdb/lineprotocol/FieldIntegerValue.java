package io.sniffy.influxdb.lineprotocol;

import java.util.Objects;

public class FieldIntegerValue extends FieldValue {

    private final long value;

    public FieldIntegerValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Long.toString(value) + "i";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldIntegerValue that = (FieldIntegerValue) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean asBoolean() {
        return value != 0;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public String asString() {
        return Long.toString(value);
    }

}
