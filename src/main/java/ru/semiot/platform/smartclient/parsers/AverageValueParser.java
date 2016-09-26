package ru.semiot.platform.smartclient.parsers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public class AverageValueParser implements Callable<Double> {

  private static final String COLUMN_SEPARATOR = ",";
  private static final int TARGET_COLUMN_NUMBER = 3;
  private final Path filePath;

  public AverageValueParser(Path filePath) {
    this.filePath = filePath;
  }

  @Override
  public Double call() {
    try {
      return Files.lines(filePath)
          .mapToDouble((line) -> Double.valueOf(line.split(COLUMN_SEPARATOR)[TARGET_COLUMN_NUMBER]))
          .average()
          .getAsDouble();
    } catch (IOException e) {
      e.printStackTrace();
      throw new IllegalStateException("Couldn't compute the average value!");
    }
  }
}
