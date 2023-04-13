package Java_Assignment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class CallCenterAnalytics {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/callcenter";
    private static final String USER = "root";
    private static final String PASS = "password";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // Execute a query to get all call records
            System.out.println("Executing query...");
            stmt = conn.createStatement();
            String sql = "SELECT * FROM Call";
            ResultSet rs = stmt.executeQuery(sql);

            // Calculate analytics
            Map<Integer, Integer> callVolumeByHourOfDay = new HashMap<>();
            Map<Integer, Long> callDurationByHourOfDay = new HashMap<>();
            Map<DayOfWeek, Integer> callVolumeByDayOfWeek = new HashMap<>();
            Map<DayOfWeek, Long> callDurationByDayOfWeek = new HashMap<>();
            while (rs.next()) {
                LocalDateTime startDateTime = LocalDateTime.parse(rs.getString("Start_time"), DATE_TIME_FORMATTER);
                LocalDateTime endDateTime = LocalDateTime.parse(rs.getString("End_time"), DATE_TIME_FORMATTER);
                int callDurationInSeconds = rs.getInt("Duration");

                // Hour of the day when the call volume is highest
                int hourOfDay = startDateTime.getHour();
                callVolumeByHourOfDay.merge(hourOfDay, 1, Integer::sum);

                // Hour of the day when the calls are longest
                callDurationByHourOfDay.merge(hourOfDay, (long) callDurationInSeconds, Long::sum);

                // Day of the week when the call volume is highest
                DayOfWeek dayOfWeek = startDateTime.getDayOfWeek();
                callVolumeByDayOfWeek.merge(dayOfWeek, 1, Integer::sum);

                // Day of the week when the calls are longest
                callDurationByDayOfWeek.merge(dayOfWeek, (long) callDurationInSeconds, Long::sum);
            }
            rs.close();

            // Print analytics
            System.out.println("Hour of the day when the call volume is highest:");
            System.out.println(callVolumeByHourOfDay.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey());
            System.out.println("Hour of the day when the calls are longest:");
            System.out.println(callDurationByHourOfDay.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey());
            System.out.println("Day of the week when the call volume is highest:");
            System.out.println(callVolumeByDayOfWeek.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey());
            System.out.println("Day of the week when the calls are longest:");
            System.out.println(callDurationByDayOfWeek.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey());
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}                