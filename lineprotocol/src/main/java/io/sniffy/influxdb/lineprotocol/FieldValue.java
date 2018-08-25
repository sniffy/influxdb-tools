package io.sniffy.influxdb.lineprotocol;

public abstract class FieldValue {

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    public abstract String asString();

    public abstract long asLong();

    public abstract double asDouble();

    public abstract boolean asBoolean();

}
