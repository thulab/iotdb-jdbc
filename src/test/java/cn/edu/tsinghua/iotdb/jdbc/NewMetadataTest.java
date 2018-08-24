package cn.edu.tsinghua.iotdb.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.service.IoTDB;

import java.sql.*;

import static org.junit.Assert.fail;

public class NewMetadataTest {
    private IoTDB deamon;

    private DatabaseMetaData databaseMetaData;

    private static String[] insertSqls = new String[]{
            "SET STORAGE GROUP TO root.vehicle",
            "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
            "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE"
    };

    @Before
    public void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.closeMemControl();
        deamon = IoTDB.getInstance();
        deamon.active();
        EnvironmentUtils.envSetUp();
    }

    @After
    public void tearDown() throws Exception {
        deamon.stop();
        Thread.sleep(5000);
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void Test() throws ClassNotFoundException, SQLException {
        prepare();

        // begin test
        AllColumns();
        DeltaObject();
        ShowTimeseriesPath();
        ShowTimeseriesPath2();
        ShowStorageGroup();
        ShowTimeseriesInJson();
    }

    public void prepare() throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:tsfile://127.0.0.1:6667/", "root", "root");
            databaseMetaData = connection.getMetaData();
            Statement statement = connection.createStatement();
            for (String sql : insertSqls) {
                statement.execute(sql);
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * get all columns' name under a given path
     */
    public void AllColumns() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("col", "root", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            String standard = "Column,\n" +
                    "root.vehicle.d0.s0,\n" +
                    "root.vehicle.d0.s1,\n" +
                    "root.vehicle.d0.s2,\n";
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) {
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * get all delta objects under a given column
     */
    public void DeltaObject() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("delta", "vehicle", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            String standard = "Column,\n" +
                    "root.vehicle.d0,\n";
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) {
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show timeseries <path>
     * usage 1
     */
    public void ShowTimeseriesPath() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("ts", "root", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            String standard = "Timeseries,Storage Group,DataType,Encoding,\n" +
                    "root.vehicle.d0.s0,root.vehicle,INT32,RLE,\n" +
                    "root.vehicle.d0.s1,root.vehicle,INT64,RLE,\n" +
                    "root.vehicle.d0.s2,root.vehicle,FLOAT,RLE,\n";
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) {
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show timeseries <path>
     * usage 2
     */
    public void ShowTimeseriesPath2() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("ts", "root.vehicle.d0.s0", null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            String standard = "DataType,\n" +
                    "INT32,\n";
            StringBuilder resultStr = new StringBuilder();
            resultStr.append(resultSetMetaData.getColumnName(3)).append(",\n");
            while (resultSet.next()) {
                resultStr.append(resultSet.getString(TsfileMetadataResultSet.GET_STRING_TIMESERIES_DATATYPE)).append(",");
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show storage group
     */
    public void ShowStorageGroup() {
        try {
            ResultSet resultSet = databaseMetaData.getColumns("sg", null, null, null);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();
            String standard = "Storage Group,\n" +
                    "root.vehicle,\n";
            StringBuilder resultStr = new StringBuilder();
            for (int i = 1; i < colCount + 1; i++) {
                resultStr.append(resultSetMetaData.getColumnName(i)).append(",");
            }
            resultStr.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= colCount; i++) {
                    resultStr.append(resultSet.getString(i)).append(",");
                }
                resultStr.append("\n");
            }
            Assert.assertEquals(resultStr.toString(), standard);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    /**
     * show metadata in json
     */
    public void ShowTimeseriesInJson() {
        String metadataInJson = databaseMetaData.toString();
        String standard = "===  Timeseries Tree  ===\n" +
                "\n" +
                "root:{\n" +
                "    vehicle:{\n" +
                "        d0:{\n" +
                "            s0:{\n" +
                "                 DataType: INT32,\n" +
                "                 Encoding: RLE,\n" +
                "                 args: {},\n" +
                "                 StorageGroup: root.vehicle \n" +
                "            },\n" +
                "            s1:{\n" +
                "                 DataType: INT64,\n" +
                "                 Encoding: RLE,\n" +
                "                 args: {},\n" +
                "                 StorageGroup: root.vehicle \n" +
                "            },\n" +
                "            s2:{\n" +
                "                 DataType: FLOAT,\n" +
                "                 Encoding: RLE,\n" +
                "                 args: {},\n" +
                "                 StorageGroup: root.vehicle \n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Assert.assertEquals(metadataInJson, standard);
    }
}
