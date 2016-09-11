package ru.semiot.platform.smartclient;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;

public class GeneralTest {

  @Test
  public void test() {
    DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    TemporalAccessor ta = FORMATTER.parse("16:27:38.692");
    int expected = ta.get(ChronoField.SECOND_OF_DAY) * 1000 + ta.get(ChronoField.MILLI_OF_SECOND);

    assertEquals(59258692, expected);
  }
}
