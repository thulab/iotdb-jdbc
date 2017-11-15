package cn.edu.tsinghua.kafka_iotdbDemo;

import cn.edu.tsinghua.kafka_iotdbDemo.dataModel.IoTModel;

import java.sql.*;

public class SendDataToIotdb {
	Connection connection = null;
	Statement statement = null;
	ResultSet resultSet = null;

	void connectToIotdb() throws Exception {
		// 1. 加载IoTDB的JDBC驱动程序
		Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
		// 2. 使用DriverManager连接IoTDB
		connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
		// 3. 操作IoTDB
		statement = connection.createStatement();
	}

	void sendData(String out) throws Exception {

		// CSV格式文件为逗号分隔符文件，这里根据逗号切分
		String item[] = out.split(",");
		
		// 使用IoTDB-JDBC获取表结构信息
		DatabaseMetaData databaseMetaData = connection.getMetaData();
	    
		//path为将要插入的路径
		String path = "root.vehicle.sensor." + item[0];
		
		//获得path路径集合迭代器
		resultSet = databaseMetaData.getColumns(null, null, path, null);
		
	    //如果路径迭代器中为null，则表示path不存在，需创建
	    if(!resultSet.next())
	    {
			String epl = "CREATE TIMESERIES " + path + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
			statement.execute(epl);
	    }
		//向IoTDB插入数据
		String template = "INSERT INTO root.vehicle.sensor(timestamp,%s) VALUES (%s,'%s')";
		String epl = String.format(template, item[0], item[1], item[2]);
		statement.execute(epl);
	}

	public void sendData(IoTModel model) throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		String path = model.getFullPath();
		// register metadata
		resultSet = metaData.getColumns(null, null, path, null);
		if(!resultSet.next()) {
			String epl;
			try{
				epl = "SET STORAGE GROUP TO " + model.deltaObj;
				statement.execute(epl);
			} catch (Exception e) {

			}
			epl = "CREATE TIMESERIES " + path + " WITH DATATYPE=TEXT, ENCODING=PLAIN";
			statement.execute(epl);
		}
		// do insert
		String template = "INSERT INTO %s (timestamp,%s) VALUES (%s,'%s')";
		String epl = String.format(template, model.deltaObj, model.measurement, model.sampleTime, model.value);
		try{
			statement.execute(epl);
		} catch (Exception e) {
			System.out.println("Cannot Insert " + path);
			System.out.println(e.getMessage());
		}
	}


	public static void main(String[] args) throws Exception {
		SendDataToIotdb sendDataToIotdb = new SendDataToIotdb();
		sendDataToIotdb.connectToIotdb();
		sendDataToIotdb.sendData("sensor4,2017/10/24 19:33:00,121 93 99");
	}
}

