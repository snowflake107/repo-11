package util;

import java.math.RoundingMode;
import java.text.DecimalFormat;

public class MathUtil {
  private static final DecimalFormat df = new DecimalFormat("#.##");

  static {
    df.setRoundingMode(RoundingMode.DOWN);
  }

  private static final double NANOSECONDS_PER_MILLISECONDS = 1000000;
  private static final double MICROSECONDS_PER_MILLISECONDS = 1000;

  public static Double convertNanosToMillis(Double nanos) {
    return (nanos / NANOSECONDS_PER_MILLISECONDS);
  }

  public static Double convertMicrosToMillis(Double micros) {
    return (micros / MICROSECONDS_PER_MILLISECONDS);
  }

  public static Double roundToTwoDecimalPlaces(Double decimal) {
    return Double.parseDouble(df.format(decimal));
  }
}
