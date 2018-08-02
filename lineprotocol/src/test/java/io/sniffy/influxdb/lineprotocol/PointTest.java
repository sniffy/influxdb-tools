package io.sniffy.influxdb.lineprotocol;

import org.junit.Test;
import ru.yandex.qatools.allure.annotations.Issue;

import static org.junit.Assert.*;

public class PointTest {

    @Test
    public void testWriteSimple() {

        Point point = new Point.Builder().
                measurement("weather").
                addTag("location", "us-midwest").
                addValue("temperature", 82.).
                timestamp(1465839830100400200L).
                build();

        assertEquals("weather,location=us-midwest temperature=82 1465839830100400200", point.toString());

    }

    @Test
    public void testWriteNoTimestamp() {

        Point point = new Point.Builder().
                measurement("weather").
                addTag("location", "us-midwest").
                addValue("temperature", 82.).
                build();

        assertEquals("weather,location=us-midwest temperature=82", point.toString());

    }

    @Test
    public void testWriteEscapedMeasurement() {

        {
            Point point = new Point.Builder().
                    measurement(",weather").
                    addTag("location", "us-midwest").
                    addValue("temperature", 82.).
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("\\,weather,location=us-midwest temperature=82 1465839830100400200", point.toString());
        }
        {
            Point point = new Point.Builder().
                    measurement(" weather").
                    addTag("location", "us-midwest").
                    addValue("temperature", 82.).
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("\\ weather,location=us-midwest temperature=82 1465839830100400200", point.toString());
        }
        {
            Point point = new Point.Builder().
                    measurement("wea,ther").
                    addTag("location", "us-midwest").
                    addValue("temperature", 82.).
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("wea\\,ther,location=us-midwest temperature=82 1465839830100400200", point.toString());
        }
        {
            Point point = new Point.Builder().
                    measurement("wea ther").
                    addTag("location", "us-midwest").
                    addValue("temperature", 82.).
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("wea\\ ther,location=us-midwest temperature=82 1465839830100400200", point.toString());
        }
        {
            Point point = new Point.Builder().
                    measurement(",wea ther").
                    addTag("location", "us-midwest").
                    addValue("temperature", 82.).
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("\\,wea\\ ther,location=us-midwest temperature=82 1465839830100400200", point.toString());
        }

    }

    @Test
    @Issue("https://github.com/sniffy/influxdb-tools/issues/1")
    public void testWriteEscapedStringValue() {

        {
            Point point = new Point.Builder().
                    measurement("weather").
                    addTag("location", "us-midwest").
                    addValue("temperature", "\"").
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("weather,location=us-midwest temperature=\"\\\"\" 1465839830100400200", point.toString());
        }

        {
            Point point = new Point.Builder().
                    measurement("weather").
                    addTag("location", "us-midwest").
                    addValue("temperature", "\\\"").
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("weather,location=us-midwest temperature=\"\\\\\\\"\" 1465839830100400200", point.toString());
        }

        {
            Point point = new Point.Builder().
                    measurement("weather").
                    addTag("location", "us-midwest").
                    addValue("temperature", "\\\\\"").
                    timestamp(1465839830100400200L).
                    build();

            assertEquals("weather,location=us-midwest temperature=\"\\\\\\\\\\\"\" 1465839830100400200", point.toString());
        }

    }

}