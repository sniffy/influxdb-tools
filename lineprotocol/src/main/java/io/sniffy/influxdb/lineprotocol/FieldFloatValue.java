package io.sniffy.influxdb.lineprotocol;

import java.util.Objects;

public class FieldFloatValue extends FieldValue {

    private final double value;

    public FieldFloatValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        if(value == (long) value)
            return String.format("%d", (long) value);
        else
            return String.format("%s", value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldFloatValue that = (FieldFloatValue) o;
        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean asBoolean() {
        return Math.signum(value) != 0;
    }

    @Override
    public long asLong() {
        return (long) value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public String asString() {
        return Double.toString(value);
    }

}
