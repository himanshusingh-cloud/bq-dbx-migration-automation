package utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DateUtils {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ISO_LOCAL_DATE;

    /**
     * Normalize date string - if invalid (e.g. 2026-02-31), return last valid day of month.
     * Prevents 500 from analytics API when end_date is invalid.
     */
    public static String normalizeDate(String dateStr) {
        if (dateStr == null || dateStr.isBlank()) return dateStr;
        try {
            LocalDate.parse(dateStr, FMT);
            return dateStr;
        } catch (Exception e) {
            try {
                String[] parts = dateStr.split("-");
                if (parts.length == 3) {
                    int year = Integer.parseInt(parts[0]);
                    int month = Integer.parseInt(parts[1]);
                    LocalDate lastDay = LocalDate.of(year, month, 1).plusMonths(1).minusDays(1);
                    return lastDay.format(FMT);
                }
            } catch (Exception ignored) { }
        }
        return dateStr;
    }

    public static String daysAgo(int days) {
        return LocalDate.now().minusDays(days).format(FMT);
    }

    public static String yesterday() {
        return daysAgo(1);
    }

    /**
     * Computes the delta_end_date: the day before the given startDate.
     */
    public static String deltaEndDate(String startDate) {
        return LocalDate.parse(startDate, FMT).minusDays(1).format(FMT);
    }

    /**
     * Computes the delta_start_date: same period length before the startDate.
     * Period length = endDate - startDate (in days).
     */
    public static String deltaStartDate(String startDate, String endDate) {
        LocalDate start = LocalDate.parse(startDate, FMT);
        LocalDate end = LocalDate.parse(endDate, FMT);
        long periodDays = ChronoUnit.DAYS.between(start, end);
        return start.minusDays(1).minusDays(periodDays).format(FMT);
    }
}
