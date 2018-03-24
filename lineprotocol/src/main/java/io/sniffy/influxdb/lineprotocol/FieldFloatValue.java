package io.sniffy.influxdb.lineprotocol;

import java.util.Objects;

public class FieldFloatValue extends FieldValue {

    private final double value;

    public FieldFloatValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Double.toString(value);
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

}
