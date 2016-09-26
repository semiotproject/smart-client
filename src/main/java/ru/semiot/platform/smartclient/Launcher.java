package ru.semiot.platform.smartclient;

import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.semiot.platform.smartclient.parsers.AverageValueParser;
import ru.semiot.platform.smartclient.parsers.ClientResultParser;
import ru.semiot.platform.smartclient.wamp.WAMPClient;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class Launcher {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);
  private static final ClientConfig CONFIG = ConfigFactory.create(ClientConfig.class);
  private static final String OPTION_COMMAND = "command";
  private static final String OPTION_INPUT = "input";

  public static void init() {
    logger.debug("Try to initialize launcher");
    HTTPClient.getInstance().init(CONFIG.hostUrl(), CONFIG.hostPassword(), CONFIG.hostUsername());

    try {
      WAMPClient.getInstance()
          .init(CONFIG.wampUrl(), "realm1", 15, CONFIG.hostUsername(), CONFIG.hostPassword());
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
  }

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption(Option.builder(OPTION_COMMAND)
        .hasArg()
        .build());
    options.addOption(Option.builder(OPTION_INPUT)
        .hasArgs()
        .build());
    options.addOption(Option.builder("id")
        .hasArg()
        .build());

    CommandLineParser cliParser = new DefaultParser();
    try {
      CommandLine cliArgs = cliParser.parse(options, args);
      if (cliArgs.hasOption(OPTION_COMMAND)) {
        if (cliArgs.getOptionValue(OPTION_COMMAND).equalsIgnoreCase("parseAndCompute")) {
          if (cliArgs.hasOption(OPTION_INPUT)) {
            String[] filePaths = cliArgs.getOptionValues(OPTION_INPUT);
            if (filePaths.length > 0) {
              Path[] inputs = new Path[filePaths.length];
              for (int i = 0; i < filePaths.length; i++) {
                inputs[i] = Paths.get(filePaths[i]);
              }

              ClientResultParser parser;
              if (cliArgs.hasOption("id")) {
                parser = new ClientResultParser(inputs, cliArgs.getOptionValue("id"));
              } else {
                parser = new ClientResultParser(inputs);
              }

              parser.run();
            } else {
              throw new IllegalArgumentException("input is empty!");
            }
          } else {
            throw new IllegalArgumentException("input");
          }
        } else if (cliArgs.getOptionValue(OPTION_COMMAND).equalsIgnoreCase("countAverage")) {
          if (cliArgs.hasOption(OPTION_INPUT)) {
            String[] filePaths = cliArgs.getOptionValues(OPTION_INPUT);
            if (filePaths.length > 0) {
              for (String filePath : filePaths) {
                logger.info("File: {}", filePath);
                AverageValueParser parser = new AverageValueParser(Paths.get(filePath));
                Double average = parser.call();
                logger.info("Average: {}", average);
              }
            } else {
              throw new IllegalArgumentException(Arrays.toString(cliArgs.getArgs()));
            }
          } else {
            throw new IllegalArgumentException(Arrays.toString(cliArgs.getArgs()));
          }
        } else {
          throw new IllegalArgumentException(Arrays.toString(cliArgs.getArgs()));
        }
      } else {
        init();

        BuildingHeatMngRunner.getInstance().run();
      }
    } catch (ParseException e) {
      logger.error(e.getMessage(), e);
    }
  }
}
