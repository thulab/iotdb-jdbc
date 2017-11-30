package cn.edu.tsinghua.kafka_iotdbDemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

public class SendDataToIotdb {
	Connection connection = null;
	Statement statement = null;
	ResultSet resultSet = null;

	void connectToIotdb() throws Exception {
		// 1. load JDBC driver of IoTDB
		Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
		// 2. DriverManager connect to IoTDB
		connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
	
		statement = connection.createStatement();
	}

	void sendData(String out) throws Exception {

		String item[] = out.split(",");
		
		// get table structure information from IoTDB-JDBC
		DatabaseMetaData databaseMetaData = connection.getMetaData();
	    
		//String path is the path to insert
		String path = "root.vehicle.sensor." + item[0];
		
		//get path set iterator
		resultSet = databaseMetaData.getColumns(null, null, path, null);
		
	    //if path set iterator is null，then create path
	    if(!resultSet.next())
	    {
			String epl = "CREATE TIMESERIES " + path + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
			statement.execute(epl);
	    }
		//insert data to IoTDB
		String template = "INSERT INTO root.vehicle.sensor(timestamp,%s) VALUES (%s,'%s')";
		String epl = String.format(template, item[0], item[1], item[2]);
		statement.execute(epl);
	}

	public static void main(String[] args) throws Exception {
		SendDataToIotdb sendDataToIotdb = new SendDataToIotdb();
		sendDataToIotdb.connectToIotdb();
		sendDataToIotdb.sendData("sensor4,2017/10/24 19:33:00,121 93 99");
	}
}

