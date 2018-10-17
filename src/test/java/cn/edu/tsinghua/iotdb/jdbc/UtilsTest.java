package cn.edu.tsinghua.iotdb.jdbc;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_Status;
import cn.edu.tsinghua.iotdb.jdbc.thrift.TS_StatusCode;

public class UtilsTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseURL() throws TsfileURLException {
		String userName = "test";
		String userPwd = "test";
		String host = "localhost";
		int port = 6667;
		Properties properties = new Properties();
		properties.setProperty(TsfileJDBCConfig.AUTH_USER, userName);
		properties.setProperty(TsfileJDBCConfig.AUTH_PASSWORD, userPwd);
		TsfileConnectionParams params = Utils.parseURL(String.format("jdbc:tsfile://%s:%s/", host, port), properties);
		assertEquals(params.getHost(), host);
		assertEquals(params.getPort(), port);
		assertEquals(params.getUsername(), userName);
		assertEquals(params.getPassword(), userPwd);
	}

	@Test
	public void testVerifySuccess() {
		try {
			Utils.verifySuccess(new TS_Status(TS_StatusCode.SUCCESS_STATUS));
		} catch (Exception e) {
			fail();
		}
		
		try {
			Utils.verifySuccess(new TS_Status(TS_StatusCode.ERROR_STATUS));
		} catch (Exception e) {
			return;
		}
		fail();
	}

	@Test
	public void testConvertAllSchema() {
		assertEquals(Utils.convertAllSchema(null), null);
	}
//
//	@Test
//	public void testConvertRowRecords() {
//		fail("Not yet implemented");
//	}

}
