package ru.semiot.platform.smartclient.wamp;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFLanguages;
import rx.Observer;

import java.io.StringReader;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class MultiThreadRDFModelObserver implements Observer<String> {

  private static final Executor executor = Executors.newFixedThreadPool(20);

  @Override
  public void onCompleted() {}

  @Override
  public void onNext(String message) {
    long onReceivedTimestamp = System.currentTimeMillis();

    executor.execute(() -> {
      Model model = ModelFactory.createDefaultModel();
      model.read(new StringReader(message), null, RDFLanguages.strLangJSONLD);

      onNext(model, onReceivedTimestamp);
    });
  }

  public abstract void onNext(Model message, long onReceivedTimestamp);
}
