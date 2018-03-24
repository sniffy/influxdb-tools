package io.sniffy.influxdb.lineprotocol;

import java.util.Objects;

public class FieldIntegerValue extends FieldValue {

    private final long value;

    public FieldIntegerValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Long.toString(value);
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

}
