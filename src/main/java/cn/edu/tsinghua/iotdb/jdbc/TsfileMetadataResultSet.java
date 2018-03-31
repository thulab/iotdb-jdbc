package cn.edu.tsinghua.iotdb.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;


public class TsfileMetadataResultSet extends TsfileQueryResultSet {

	private Iterator<?> columnItr;

	private MetadataType type;

	private ColumnSchema currentColumn;

	private String currentDeltaObject;

	// for show timeseries result
	private List<String> currentTimeseries; // current row for show timeseries
	private static int colCount = 4; // the number of columns for show timeseries
	private static String[] showTsLabels = new String[]{"Timeseries", "Storage Group", "DataType", "Encoding"}; // headers for show timeseries
	private int[] maxValueLength; // the max length of the name of timeseries for show timeseries

	public TsfileMetadataResultSet(List<ColumnSchema> columnSchemas, List<String> deltaObjectList) {
		if (columnSchemas != null) {
			columnItr = columnSchemas.iterator();
			type = MetadataType.COLUMN;
		} else if (deltaObjectList != null) {
			columnItr = deltaObjectList.iterator();
			type = MetadataType.DELTA_OBJECT;
		}
	}

	// for show timeseries result
	public TsfileMetadataResultSet(List<List<String>> tslist) {
		type = MetadataType.SHOW_TIMESERIES;
		columnItr = tslist.iterator();
		/*
		if (tslist.size() >= 1) {
			colCount = tslist.get(0).size();
		}
		*/
		maxValueLength = new int[colCount];
		for (int i = 0; i < colCount; i++) {
			int tmp = showTsLabels[i].length();
			for (List<String> tsrow : tslist) {
				int len = tsrow.get(i).length();
				tmp = tmp > len ? tmp : len;
			}
			maxValueLength[i] = tmp;
		}
	}

	// for show timeseries result
	public int getMaxValueLength(int columnIndex) throws SQLException { // start from 1
		if (type == MetadataType.SHOW_TIMESERIES && columnIndex >= 1 && columnIndex <= colCount) {
			return maxValueLength[columnIndex-1];
		} else {
			throw new SQLException(String.format("select column index %d does not exists", columnIndex));
		}
	}

	// for show timeseries result
	public int getColCount() {
		return colCount;
	}

	// for show timeseries result
	public static String[] getShowTsLabels() {
		return showTsLabels;
	}


	@Override
	public void close() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if(type == MetadataType.SHOW_TIMESERIES) {
			return new TsfileMetadataResultMetadata(showTsLabels);
		}
		else {
			throw new SQLException("Method not supported");
		}
	}

	@Override
	public boolean next() throws SQLException {
		boolean hasNext = columnItr.hasNext();
		switch (type) {
			case COLUMN:
				if (hasNext) {
					currentColumn = (ColumnSchema) columnItr.next();
				}
				return hasNext;
			case DELTA_OBJECT:
				if (hasNext) {
					currentDeltaObject = (String) columnItr.next();
				}
				return hasNext;
			case SHOW_TIMESERIES:
				if (hasNext) {
					currentTimeseries = (List<String>) columnItr.next();
				}
				return hasNext;
			default:
				break;
		}
		return false;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		switch (type) {
			case DELTA_OBJECT:
				if (columnIndex == 1) {
					return getString("DELTA_OBJECT");
				}
				break;
			case COLUMN:
				if (columnIndex == 1) {
					return getString("COLUMN_NAME");
				} else if (columnIndex == 2) {
					return getString("COLUMN_TYPE");
				}
				break;
			case SHOW_TIMESERIES:
				for(int i = 1; i <= colCount; i++) {
					if(columnIndex == i) {
						return getString(showTsLabels[i-1]);
					}
				}
				break;
			default:
				break;
		}
		throw new SQLException(String.format("select column index %d does not exists", columnIndex));
	}

	@Override
	public String getString(String columnName) throws SQLException {
		// use special key word to judge return content
		switch (columnName) {
			case "COLUMN_NAME":
				return currentColumn.name;
			case "COLUMN_TYPE":
				if (currentColumn.dataType != null) {
					return currentColumn.dataType.toString();
				}
			case "DELTA_OBJECT":
				return currentDeltaObject;
			case "Timeseries":
				return currentTimeseries.get(0);
			case "Storage Group":
				return currentTimeseries.get(1);
			case "DataType":
				return currentTimeseries.get(2);
			case "Encoding":
				return currentTimeseries.get(3);
			default:
				break;
		}
		return null;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getType() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLException("Method not supported");
	}

	private enum MetadataType{
		DELTA_OBJECT, COLUMN, SHOW_TIMESERIES
	}
}
