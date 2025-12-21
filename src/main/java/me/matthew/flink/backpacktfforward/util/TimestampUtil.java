package me.matthew.flink.backpacktfforward.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for working with timestamps in Kafka consumer context.
 * Provides convenient methods for converting between different timestamp formats.
 */
public class TimestampUtil {
    
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    /**
     * Converts a Unix timestamp in milliseconds to a human-readable string.
     * @param timestampMs Unix timestamp in milliseconds
     * @return Human-readable timestamp string in ISO format
     */
    public static String formatTimestamp(long timestampMs) {
        return Instant.ofEpochMilli(timestampMs)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }
    
    /**
     * Converts an ISO date-time string to Unix timestamp in milliseconds.
     * Expected format: "2023-12-20T10:30:00" (system timezone assumed)
     * @param isoDateTime ISO date-time string
     * @return Unix timestamp in milliseconds
     */
    public static long parseIsoDateTime(String isoDateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(isoDateTime, ISO_FORMATTER);
        return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
    
    /**
     * Converts an ISO date-time string with timezone to Unix timestamp in milliseconds.
     * Expected format: "2023-12-20T10:30:00Z" or "2023-12-20T10:30:00+01:00"
     * @param isoDateTimeWithZone ISO date-time string with timezone
     * @return Unix timestamp in milliseconds
     */
    public static long parseIsoDateTimeWithZone(String isoDateTimeWithZone) {
        return ZonedDateTime.parse(isoDateTimeWithZone).toInstant().toEpochMilli();
    }
    
    /**
     * Gets the current timestamp in milliseconds.
     * @return Current Unix timestamp in milliseconds
     */
    public static long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }
    
    /**
     * Gets a timestamp from X minutes ago.
     * @param minutesAgo Number of minutes to subtract from current time
     * @return Unix timestamp in milliseconds
     */
    public static long getTimestampMinutesAgo(int minutesAgo) {
        return System.currentTimeMillis() - (minutesAgo * 60 * 1000L);
    }
    
    /**
     * Gets a timestamp from X hours ago.
     * @param hoursAgo Number of hours to subtract from current time
     * @return Unix timestamp in milliseconds
     */
    public static long getTimestampHoursAgo(int hoursAgo) {
        return System.currentTimeMillis() - (hoursAgo * 60 * 60 * 1000L);
    }
    
    /**
     * Gets a timestamp from X days ago.
     * @param daysAgo Number of days to subtract from current time
     * @return Unix timestamp in milliseconds
     */
    public static long getTimestampDaysAgo(int daysAgo) {
        return System.currentTimeMillis() - (daysAgo * 24 * 60 * 60 * 1000L);
    }
}