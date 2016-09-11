package ru.semiot.platform.smartclient;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class LogParser implements Runnable {

  private static final Pattern OBSERVATION_PATTERN = Pattern.compile(
      "(\\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+INFO.*-\\s+Observation\\sexecute\\stime\\sis\\s(\\d+).*");
  private static final Pattern COMMANDRESULT_PATTERN = Pattern.compile(
      "(\\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+INFO.*-\\s+Command\\sis\\sexecuted\\sby\\s(\\d+).*");
  private static final Pattern STARTED_PATTERN = Pattern.compile(
      "(\\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+INFO.*-\\s+Smart-client\\sis\\sstarted.*");
  private static final String FORMAT_POSTFIX = ".csv";
  private static final String OBSERVATION_VALUES_FILENAME_PREFIX = "observations";
  private static final String COMMANDRESULT_VALUES_FILENAME_PREFIX = "commandresults";
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
  private static final int WINDOW_SIZE = 60000; //milliseconds
  private Path filePath;
  private String id;

  public LogParser(Path filePath) {
    this(filePath, "");
  }

  public LogParser(Path filePath, String id) {
    this.filePath = filePath;
    this.id = "_" + id;
  }

  @Override
  public void run() {
    try (Stream<String> lines = Files.lines(filePath);
         BufferedWriter obWriter = Files.newBufferedWriter(Paths.get(
             OBSERVATION_VALUES_FILENAME_PREFIX + id + FORMAT_POSTFIX),
             StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
         BufferedWriter crWriter = Files.newBufferedWriter(Paths.get(
             COMMANDRESULT_VALUES_FILENAME_PREFIX + id + FORMAT_POSTFIX),
             StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

      String[] linesArray = lines.toArray(String[]::new);
      int initialTimestamp = -1;

      for (String line : linesArray) {
        Matcher m = STARTED_PATTERN.matcher(line);
        if (m.matches()) {
          initialTimestamp = milliseconds(m.group(1));
        }
      }

      if (initialTimestamp < 0) {
        throw new IllegalStateException("Couldn't find 'Smart-client is started' message in logs!");
      }

      Processor obProcessor = new Processor("OB", obWriter, initialTimestamp);
      Processor crProcessor = new Processor("CR", crWriter, initialTimestamp);
      for (String line : linesArray) {
        try {
          Matcher m = OBSERVATION_PATTERN.matcher(line);
          if (m.matches()) {
            obProcessor.handle(m);
          } else if ((m = COMMANDRESULT_PATTERN.matcher(line)).matches()) {
            crProcessor.handle(m);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private int milliseconds(String time) {
    TemporalAccessor ta = FORMATTER.parse(time);
    return ta.get(ChronoField.SECOND_OF_DAY) * 1000 + ta.get(ChronoField.MILLI_OF_SECOND);
  }

  private class Processor {
    private final String id;
    private final Writer writer;
    private final int initialTimestamp;

    private int currentWindow;
    private int timestampDiff;
    private Record record;

    Processor(String id, Writer writer, int initialTimestamp) {
      this.id = id;
      this.writer = writer;
      this.initialTimestamp = initialTimestamp;

      this.currentWindow = 1;
      this.timestampDiff = 0;
      this.record = new Record();
    }

    public void handle(Matcher m) throws IOException {
      timestampDiff = milliseconds(m.group(1)) - initialTimestamp;
      if (timestampDiff > currentWindow * WINDOW_SIZE) {

//        System.out.println(id + " : " + currentWindow);
//        System.out.println(id + " : " + timestampDiff);

        if (record.amount > 0) {
          write(currentWindow, record);
        }

        record.clear();

        if ((timestampDiff - currentWindow * WINDOW_SIZE) > WINDOW_SIZE) {
          currentWindow = timestampDiff / WINDOW_SIZE + 1;
        } else {
          currentWindow++;
        }
//        System.out.println(id + " : " + currentWindow);
        record.add(Integer.parseInt(m.group(2)));
      } else {
        record.add(Integer.parseInt(m.group(2)));
      }
    }

    private void write(int window, Record record) throws IOException {
      writer.write(window + "," + record.min + "," + record.max + ","
          + record.sum / record.amount + ","
          + record.amount + "\n");
    }
  }

  private class Record {
    public int min = Integer.MAX_VALUE;
    public int max = Integer.MIN_VALUE;
    public int sum = 0;
    public int amount = 0;

    public void add(int value) {
      this.sum += value;
      this.min = Math.min(value, this.min);
      this.max = Math.max(value, this.max);
      this.amount++;
    }

    public void clear() {
      this.sum = 0;
      this.min = Integer.MAX_VALUE;
      this.max = Integer.MIN_VALUE;
      this.amount = 0;
    }
  }
}
