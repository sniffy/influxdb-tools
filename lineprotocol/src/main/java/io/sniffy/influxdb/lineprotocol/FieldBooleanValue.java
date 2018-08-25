package io.sniffy.influxdb.lineprotocol;

import java.util.Objects;

public class FieldBooleanValue extends FieldValue {

    private final boolean value;

    public FieldBooleanValue(boolean value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldBooleanValue that = (FieldBooleanValue) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean asBoolean() {
        return value;
    }

    @Override
    public long asLong() {
        return value ? 1 : 0;
    }

    @Override
    public double asDouble() {
        return value ? 1 : 0;
    }

    @Override
    public String asString() {
        return Boolean.toString(value);
    }

}
