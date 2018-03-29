package io.sniffy.influxdb.lineprotocol;

import java.io.*;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Point {

    private final String measurement;
    private final Map<String, String> tags;
    private final Map<String, FieldValue> values;
    private final Long timestamp;

    public Point(String measurement, Map<String, String> tags, Map<String, FieldValue> values, Long timestamp) {
        this.measurement = measurement;
        this.tags = Collections.unmodifiableMap(tags);
        this.values = Collections.unmodifiableMap(values);
        this.timestamp = timestamp;
    }

    public String getMeasurement() {
        return measurement;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Map<String, FieldValue> getValues() {
        return values;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        try {
            writeTo(sw);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sw.toString();
    }

    public void writeTo(OutputStream outputStream) throws IOException {
        writeTo(new OutputStreamWriter(outputStream));
    }

    public void writeTo(Writer writer) throws IOException {
        writer.write(measurement.replace(" ", "\\ ").replace(",", "\\,"));

        if (null != tags && !tags.isEmpty()) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                writer.write(",");
                writer.write(entry.getKey().replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\="));
                writer.write("=");
                writer.write(entry.getValue().replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\="));
            }
        }

        writer.write(" ");

        boolean fieldSeparatorRequired = false;

        for (Map.Entry<String, FieldValue> entry : values.entrySet()) {
            if (fieldSeparatorRequired) {
                writer.write(",");
            }
            writer.write(entry.getKey().replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\="));
            writer.write("=");
            writer.write(entry.getValue().toString());
            fieldSeparatorRequired = true;
        }

        if (null != timestamp) {
            writer.write(" ");
            writer.write(timestamp.toString());
        }

    }

    public static class Builder {

        private final boolean validate;

        private String measurement;
        private final Map<String, String> tags = new HashMap<>();
        private final Map<String, FieldValue> values = new HashMap<>();
        private Long timestamp;

        public Builder() {
            this(false);
        }

        public Builder(boolean validate) {
            this.validate = validate;
        }

        public String getMeasurement() {
            return measurement;
        }

        public void setMeasurement(String measurement) {
            this.measurement = measurement;
        }

        public Builder measurement(String measurement) {
            setMeasurement(measurement);
            return this;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags.clear();
            addTags(tags);
        }

        public Builder addTag(String key, String value) {
            tags.put(key, value);
            return this;
        }

        public Builder addTags(Map<String, String> tags) {
            this.tags.putAll(tags);
            return this;
        }

        public Map<String, FieldValue> getValues() {
            return values;
        }

        public void setValues(Map<String, FieldValue> values) {
            this.values.clear();
            addValues(values);
        }

        public Builder addValue(String key, FieldValue value) {
            this.values.put(key, value);
            return this;
        }

        public Builder addValue(String key, String value) {
            this.values.put(key, new FieldStringValue(value));
            return this;
        }

        public Builder addValue(String key, float value) {
            addValue(key, (double) value);
            return this;
        }

        public Builder addValue(String key, BigDecimal value) {
            addValue(key, value.doubleValue());
            return this;
        }

        public Builder addValue(String key, double value) {
            this.values.put(key, new FieldFloatValue(value));
            return this;
        }

        public Builder addValue(String key, int value) {
            addValue(key, (long) value);
            return this;
        }

        public Builder addValue(String key, short value) {
            addValue(key, (long) value);
            return this;
        }

        public Builder addValue(String key, byte value) {
            addValue(key, (long) value);
            return this;
        }

        public Builder addValue(String key, long value) {
            this.values.put(key, new FieldIntegerValue(value));
            return this;
        }

        public Builder addValue(String key, boolean value) {
            this.values.put(key, new FieldBooleanValue(value));
            return this;
        }

        public Builder addValues(Map<String, FieldValue> values) {
            this.values.putAll(values);
            return this;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public Builder timestamp(Long timestamp) {
            setTimestamp(timestamp);
            return this;
        }

        public Point build() {
            return values.isEmpty() ? null : new Point(
                    measurement,
                    tags,
                    values,
                    timestamp
            );
        }

    }

}