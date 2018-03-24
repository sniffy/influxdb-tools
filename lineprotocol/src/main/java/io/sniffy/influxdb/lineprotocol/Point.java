package io.sniffy.influxdb.lineprotocol;

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

        public Map<String, String> getTags() {
            return tags;
        }

        public void setTags(Map<String, String> tags) {
            this.tags.clear();
            addTags(tags);
        }

        public void addTag(String key, String value) {
            tags.put(key, value);
        }

        public void addTags(Map<String, String> tags) {
            tags.putAll(tags);
        }

        public Map<String, FieldValue> getValues() {
            return values;
        }

        public void setValues(Map<String, FieldValue> values) {
            this.values.clear();
            addValues(values);
        }

        public void addValue(String key, FieldValue value) {
            this.values.put(key, value);
        }

        public void addValue(String key, String value) {
            this.values.put(key, new FieldStringValue(value));
        }

        public void addValue(String key, float value) {
            addValue(key, (double) value);
        }

        public void addValue(String key, BigDecimal value) {
            addValue(key, value.doubleValue());
        }

        public void addValue(String key, double value) {
            this.values.put(key, new FieldFloatValue(value));
        }

        public void addValue(String key, int value) {
            addValue(key, (long) value);
        }

        public void addValue(String key, short value) {
            addValue(key, (long) value);
        }

        public void addValue(String key, byte value) {
            addValue(key, (long) value);
        }

        public void addValue(String key, long value) {
            this.values.put(key, new FieldIntegerValue(value));
        }

        public void addValue(String key, boolean value) {
            this.values.put(key, new FieldBooleanValue(value));
        }

        public void addValues(Map<String, FieldValue> values) {
            this.values.putAll(values);
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
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