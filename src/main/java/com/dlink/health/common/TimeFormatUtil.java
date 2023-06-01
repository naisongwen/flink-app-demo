package com.dlink.health.common;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;


public class TimeFormatUtil {
 public static final DateTimeFormatter RFC3339_TIME_FORMAT;
 public static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT;
 public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT;
 public static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
 public static final DateTimeFormatter SQL_TIME_FORMAT;
 public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT;
 public static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;


 static {
  RFC3339_TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendPattern("'Z'").toFormatter();
  RFC3339_TIMESTAMP_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(RFC3339_TIME_FORMAT).toFormatter();
  ISO8601_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
  ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).appendPattern("'Z'").toFormatter();
  SQL_TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
  SQL_TIMESTAMP_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(SQL_TIME_FORMAT).toFormatter();
  SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(SQL_TIME_FORMAT).appendPattern("'Z'").toFormatter();
 }
}
