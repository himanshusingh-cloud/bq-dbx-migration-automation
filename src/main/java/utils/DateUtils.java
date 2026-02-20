package utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DateUtils {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ISO_LOCAL_DATE;

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
