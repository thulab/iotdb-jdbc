package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataTest {
    Connection connection = null;
    DatabaseMetaData databaseMetaData;

    @Before
    public void setUp() throws Exception {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root",
                    "root");
            databaseMetaData = connection.getMetaData();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    /**
     * show metadata in json
     */
    public void ShowTimeseriesInJson() {
        String metadataInJson = null;
        try {
            metadataInJson = databaseMetaData.toString();
        } catch (OutOfMemoryError outOfMemoryError) {
            System.out.println(outOfMemoryError);
        }
        System.out.println(metadataInJson);
    }

    @Test
    /**
     * show timeseries <path>
     */
    public void ShowTimeseriesPath() {
        try {
            ResultSet resultSet = ((TsfileDatabaseMetadata) databaseMetaData).getShowTimeseriesPath("root");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            // print header
            System.out.printf("|");
            for(int i=1;i<colCount+1;i++) {
                String formatStr = "%"+ ((TsfileMetadataResultMetadata) resultSetMetaData).getMaxValueLength(i)+"s|";
                System.out.printf(formatStr,resultSetMetaData.getColumnName(i));
            }
            // print content
            while (resultSet.next()) {
                System.out.printf("\n|");
                for (int i = 1; i <= colCount; i++) {
                    String formatStr = "%"+ ((TsfileMetadataResultMetadata) resultSetMetaData).getMaxValueLength(i)+"s|";
                    System.out.printf(formatStr,resultSet.getString(i));
                }
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    @Test
    /**
     * show storage group
     */
    public void ShowStorageGroup() {
        try {
            ResultSet resultSet = ((TsfileDatabaseMetadata) databaseMetaData).getShowStorageGroups();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            // print header
            System.out.printf("|");
            for(int i=1;i<colCount+1;i++) {
                String formatStr = "%"+ ((TsfileMetadataResultMetadata) resultSetMetaData).getMaxValueLength(i)+"s|";
                System.out.printf(formatStr,resultSetMetaData.getColumnName(i));
            }
            // print content
            while(resultSet.next()) {
                System.out.printf("\n|");
                for (int i = 1; i <= colCount; i++) {
                    String formatStr = "%"+ ((TsfileMetadataResultMetadata) resultSetMetaData).getMaxValueLength(i)+"s|";
                    System.out.printf(formatStr,resultSet.getString(i));
                }
            }
//            while (resultSet.next()) {
//                StringBuilder builder = new StringBuilder();
//                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
//                    builder.append(resultSet.getString(i)).append(",");
//                }
//                System.out.println(builder);
//            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

}
