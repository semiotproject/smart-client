package ru.semiot.platform.smart.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class LogParser implements Runnable {

  private static final Pattern OBSERVATION_PATTERN = Pattern.compile(
      "(\\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+INFO.*-\\s+Observation\\sexecute\\stime\\sis\\s(\\d+).*");
  private static final Pattern COMMANDRESULT_PATTERN = Pattern.compile(
      "(\\d{2}:\\d{2}:\\d{2}\\.\\d+)\\s+INFO.*-\\s+Command\\sis\\sexecuted\\sby\\s(\\d+).*");
  private static final String OBSERVATION_VALUES_FILENAME = "observations.csv";
  private static final String COMMANDRESULT_VALUES_FILENAME = "commandresults.csv";
  private Path filePath;

  public LogParser(Path filePath) {
    this.filePath = filePath;
  }

  @Override
  public void run() {
    try (Stream<String> lines = Files.lines(filePath);
         BufferedWriter obWriter = Files.newBufferedWriter(Paths.get(OBSERVATION_VALUES_FILENAME),
             StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
         BufferedWriter crWriter = Files.newBufferedWriter(Paths.get(COMMANDRESULT_VALUES_FILENAME),
             StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
      lines.forEachOrdered(line -> {
        try {
          Matcher m = OBSERVATION_PATTERN.matcher(line);
          if (m.matches()) {
            obWriter.write(m.group(1) + "," + m.group(2) + "\n");
          } else if ((m = COMMANDRESULT_PATTERN.matcher(line)).matches()) {
            crWriter.write(m.group(1) + "," + m.group(2) + "\n");
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
