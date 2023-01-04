package org.hypertrace.traceenricher.enrichedspan.constants.utils;

import org.hypertrace.core.datamodel.Event;
import org.hypertrace.core.datamodel.MetricValue;

public class SpanUtils {

  public static double getMetricValue(Event event, String metricName, double defaultValue) {
    if (event.getMetrics() == null || event.getMetrics().getMetricMap().isEmpty()) {
      return defaultValue;
    }

    MetricValue value = event.getMetrics().getMetricMap().get(metricName);
    if (value == null) {
      return defaultValue;
    }
    return value.getValue();
  }
}
