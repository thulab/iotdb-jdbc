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
import java.util.Set;


public class TsfileMetadataResultSet extends TsfileQueryResultSet {

	private Iterator<?> columnItr;

	private MetadataType type;

	private String currentColumn;
	public static final String GET_STRING_COLUMN = "COLUMN";

	private String currentStorageGroup;
	// NOTE: 'GET_STRING_STORAGE_GROUP' must not equal 'GET_STRING_TIMESERIES_STORAGE_GROUP'
	public static final String GET_STRING_STORAGE_GROUP = "STORAGE GROUP";

	private List<String> currentTimeseries;
	public static final String GET_STRING_TIMESERIES_NAME = "Timeseries";
   	public static final String GET_STRING_TIMESERIES_STORAGE_GROUP = "Storage Group";
   	public static final String GET_STRING_TIMESERIES_DATATYPE = "DataType";
   	public static final String GET_STRING_TIMESERIES_ENCODING = "Encoding";

    	// for display
    	private int colCount; // the number of columns for show
    	private String[] showLabels; // headers for show

    	/**
     	* Constructor used for the result of DatabaseMetadata.getColumns()
     	*/
		public TsfileMetadataResultSet(List<String> columns, Set<String> storageGroupSet,
									   List<List<String>> showTimeseriesList) throws SQLException {
			if (columns != null) {
				type = MetadataType.COLUMN;
				showLabels = new String[]{GET_STRING_COLUMN};
				colCount = 1;
				columnItr = columns.iterator();
			} else if (storageGroupSet != null) {
				type = MetadataType.STORAGE_GROUP;
				showLabels = new String[]{GET_STRING_STORAGE_GROUP};
				colCount = 1;
				columnItr = storageGroupSet.iterator();
			} else if (showTimeseriesList != null) {
				type = MetadataType.TIMESERIES;
				showLabels = new String[]{GET_STRING_TIMESERIES_NAME, GET_STRING_TIMESERIES_STORAGE_GROUP,
						GET_STRING_TIMESERIES_DATATYPE, GET_STRING_TIMESERIES_ENCODING};
				colCount = 4;
				columnItr = showTimeseriesList.iterator();
			} else {
				throw new SQLException("TsfileMetadataResultSet constructor is wrongly used.");
			}
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
		return new TsfileMetadataResultMetadata(showLabels);
	}

	@Override
	public boolean next() throws SQLException {
		boolean hasNext = columnItr.hasNext();
		if (hasNext) {
			if (type == MetadataType.STORAGE_GROUP) {
				currentStorageGroup = (String) columnItr.next();
			} else if (type == MetadataType.COLUMN) {
				currentColumn = (String) columnItr.next();
			} else {
				currentTimeseries = (List<String>) columnItr.next();
			}
		}
		return hasNext;
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
		if (columnIndex >= 1 && columnIndex <= colCount) {
			return getString(showLabels[columnIndex - 1]);
		}
		throw new SQLException(String.format("select column index %d does not exists", columnIndex));
	}

	@Override
	public String getString(String columnName) throws SQLException {
		// use special key word to judge return content
		switch (columnName) {
			case GET_STRING_STORAGE_GROUP:
                		return currentStorageGroup;
            		case GET_STRING_TIMESERIES_NAME:
                		return currentTimeseries.get(0);
            		case GET_STRING_TIMESERIES_STORAGE_GROUP:
                		return currentTimeseries.get(1);
            		case GET_STRING_TIMESERIES_DATATYPE:
                		return currentTimeseries.get(2);
            		case GET_STRING_TIMESERIES_ENCODING:
                		return currentTimeseries.get(3);
            		case GET_STRING_COLUMN:
                		return currentColumn;
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
		return type.ordinal();
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

	public enum MetadataType{
    	STORAGE_GROUP, TIMESERIES, COLUMN
	}
}
