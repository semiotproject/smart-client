package ru.semiot.platform.smartclient.parsers;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ClientResultParser implements Runnable {

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
  private static final int MAX_WINDOW_HUMBER = 120;
  private Path[] filePaths;
  private String id;

  public ClientResultParser(Path[] filePaths) {
    this(filePaths, "");
  }

  public ClientResultParser(Path[] filePaths, String id) {
    this.filePaths = filePaths;
    this.id = "_" + id;
  }

  @Override
  public void run() {
    try (BufferedWriter obWriter = Files.newBufferedWriter(Paths.get(
        OBSERVATION_VALUES_FILENAME_PREFIX + id + FORMAT_POSTFIX),
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
         BufferedWriter crWriter = Files.newBufferedWriter(Paths.get(
             COMMANDRESULT_VALUES_FILENAME_PREFIX + id + FORMAT_POSTFIX),
             StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

      Processor obProcessor = new Processor(obWriter);
      Processor crProcessor = new Processor(crWriter);

      for (Path path : filePaths) {
        Stream<String> lines = Files.lines(path);
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

        for (String line : linesArray) {
          try {
            Matcher m = OBSERVATION_PATTERN.matcher(line);
            if (m.matches()) {
              obProcessor.handle(m, initialTimestamp);
            } else if ((m = COMMANDRESULT_PATTERN.matcher(line)).matches()) {
              crProcessor.handle(m, initialTimestamp);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }

      obProcessor.writeResults();
      crProcessor.writeResults();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private int milliseconds(String time) {
    TemporalAccessor ta = FORMATTER.parse(time);
    return ta.get(ChronoField.SECOND_OF_DAY) * 1000 + ta.get(ChronoField.MILLI_OF_SECOND);
  }

  private class Processor {
    private final Writer writer;
    private final Map<Integer, Record> records;

    Processor(Writer writer) {
      this.writer = writer;
      this.records = new HashMap<>();
    }

    public void handle(Matcher m, int initialTimestamp) throws IOException {
      int windowNumber = Math.round((milliseconds(m.group(1)) - initialTimestamp) / WINDOW_SIZE);

      if (!records.containsKey(windowNumber)) {
        records.put(windowNumber, new Record());
      }

      Record record = records.get(windowNumber);
      record.add(Integer.parseInt(m.group(2)));
    }

    public void writeResults() throws IOException {
      Integer[] windows = records.keySet().toArray(new Integer[]{});
      Arrays.parallelSort(windows);

      for (Integer windowNumber : windows) {
        if(windowNumber < MAX_WINDOW_HUMBER + 1) {
          Record record = records.get(windowNumber);

          writer.write(windowNumber + "," + record.min + "," + record.max + ","
              + record.sum / record.amount + ","
              + record.amount + "\n");
        } else {
          break;
        }
      }
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
  }
}
