package io.sniffy.influxdb.lineprotocol;

import java.util.Objects;

public class FieldStringValue extends FieldValue {

    private final String value;

    public FieldStringValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldStringValue that = (FieldStringValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

}
