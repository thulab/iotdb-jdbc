package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.*;

public class MetadataTest {
    public static int[] maxValueLengthForShow = new int[]{40, 40, 10, 10};
    public static DatabaseMetaData databaseMetaData;

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            databaseMetaData = connection.getMetaData();
            AllColumns();
            System.out.println("----------------------------------------------------------------");
            DeltaObject();
            System.out.println("----------------------------------------------------------------");
            ShowTimeseriesPath();
            System.out.println("----------------------------------------------------------------");
            ShowStorageGroup();
            System.out.println("----------------------------------------------------------------");
            ShowTimeseriesInJson();
        } catch (SQLException e) {
            System.out.println(e);
        } finally {
            connection.close();
        }
    }

    /**
     * get all columns' name under a given path
     */
    public static void AllColumns() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("col", "root", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            // print header
            System.out.printf("|");
            for (int i = 1; i < colCount + 1; i++) {
                String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                System.out.printf(formatStr, resultSetMetaData.getColumnName(i));
            }
            // print content
            while (resultSet.next()) {
                System.out.printf("\n|");
                for (int i = 1; i <= colCount; i++) {
                    String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                    System.out.printf(formatStr, resultSet.getString(i));
                }
            }
            System.out.println("");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * get all delta objects under a given column
     */
    public static void DeltaObject() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("delta", "ln", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            // print header
            System.out.printf("|");
            for (int i = 1; i < colCount + 1; i++) {
                String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                System.out.printf(formatStr, resultSetMetaData.getColumnName(i));
            }
            // print content
            while (resultSet.next()) {
                System.out.printf("\n|");
                for (int i = 1; i <= colCount; i++) {
                    String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                    System.out.printf(formatStr, resultSet.getString(i));
                }
            }
            System.out.println("");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show timeseries <path>
     */
    public static void ShowTimeseriesPath() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("ts", "root", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            // print header
            System.out.printf("|");
            for (int i = 1; i < colCount + 1; i++) {
                String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                System.out.printf(formatStr, resultSetMetaData.getColumnName(i));
            }
            // print content
            while (resultSet.next()) {
                System.out.printf("\n|");
                for (int i = 1; i <= colCount; i++) {
                    String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                    System.out.printf(formatStr, resultSet.getString(i));
                }
            }
            System.out.println("");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show timeseries <path>
     */
    public static void ShowTimeseriesPath2() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("ts", "root.ln.wf01.wt01.status", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            // print dataType of the given path
            while (resultSet.next()) {
                System.out.println(resultSet.getString(TsfileMetadataResultSet.GET_STRING_TIMESERIES_DATATYPE));
            }

        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show storage group
     */
    public static void ShowStorageGroup() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("sg", null, null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            // print header
            System.out.printf("|");
            for (int i = 1; i < colCount + 1; i++) {
                String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                System.out.printf(formatStr, resultSetMetaData.getColumnName(i));
            }
            // print content
            while (resultSet.next()) {
                System.out.printf("\n|");
                for (int i = 1; i <= colCount; i++) {
                    String formatStr = "%" + maxValueLengthForShow[i - 1] + "s|";
                    System.out.printf(formatStr, resultSet.getString(i));
                }
            }
            System.out.println("");
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show metadata in json
     */
    public static void ShowTimeseriesInJson() {
        String metadataInJson = databaseMetaData.toString();
        System.out.println(metadataInJson);
    }
}
