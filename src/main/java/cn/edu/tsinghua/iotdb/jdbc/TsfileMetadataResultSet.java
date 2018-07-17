package cn.edu.tsinghua.iotdb.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class TsfileMetadataResultSet extends TsfileQueryResultSet {

	private Iterator<?> columnItr;

	private MetadataType type;

	private ColumnSchema currentColumn;
    public static final String GET_STRING_COLUMN = "COLUMN";

	private String currentStorageGroup;
    public static final String GET_STRING_STORAGE_GROUP = "STORAGE_GROUP";

    private List<String> currentTimeseries; // current row for show timeseries
    private TSIService.Iface client = null;
    private String STSpath;
    private int batchFetchIdx;
    private boolean emptyResultSet;
    public static final String GET_STRING_TIMESERIES_NAME = "Timeseries";
    public static final String GET_STRING_TIMESERIES_STORAGE_GROUP = "Storage Group";
    public static final String GET_STRING_TIMESERIES_DATATYPE = "DataType";
    public static final String GET_STRING_TIMESERIES_ENCODING = "Encoding";
    
    // for display
    private int colCount; // the number of columns for show
    private String[] showLabels; // headers for show

    /**
     * Constructor used for DatabaseMetadata.getColumns(“col”,...) or getColumns(“delta”,...) results
     *
     * @param columns
     */
    public TsfileMetadataResultSet(List<String> columns) {
        type = MetadataType.COLUMN;
        colCount = 1;
        showLabels = new String[]{"Column"};
        columnItr = columns.iterator();
    }
    
     /**
     * Constructor used for SHOW_STORAGE_GROUP results
     *
     * @param storageGroupSet
     */
    public TsfileMetadataResultSet(Set<String> storageGroupSet) {
        type = MetadataType.STORAGE_GROUP;
        colCount = 1;
        showLabels = new String[]{"Storage Group"};
        columnItr = storageGroupSet.iterator();
    }

	/**
     * Constructor used for SHOW_TIMESERIES_PATH results
     *
     * @param path
     */
    public TsfileMetadataResultSet(String path, TSIService.Iface client) {
        type = MetadataType.TIMESERIES;

        this.client = client;
        STSpath = path;
        batchFetchIdx = 0;
        emptyResultSet = false;

        colCount = 4;
        showLabels = new String[]{GET_STRING_TIMESERIES_NAME, GET_STRING_TIMESERIES_STORAGE_GROUP,
                GET_STRING_TIMESERIES_DATATYPE, GET_STRING_TIMESERIES_ENCODING};

        columnItr = null;
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
		if (type == MetadataType.COLUMN) {
			throw new SQLException("Method not supported for COLUMN type");
		}
		return new TsfileMetadataResultMetadata(showLabels);

	}

	@Override
	public boolean next() throws SQLException {
		if (type == MetadataType.TIMESERIES) {
            if ((columnItr == null || !columnItr.hasNext()) && !emptyResultSet) {
                TSFetchMetadataReq req = new TSFetchMetadataReq(TsFileDBConstant.GLOBAL_SHOW_TIMESERIES_REQ);
                req.setColumnPath(STSpath);
                req.setBatchFetchIdx(this.batchFetchIdx);
                req.setBatchFetchSize(TsfileJDBCConfig.metaDataBatchFetchSize);
                TSFetchMetadataResp resp;
                try {
                    resp = client.fetchMetadata(req);
                    Utils.verifySuccess(resp.status);
                    if (!resp.hasResultSet) {
                        emptyResultSet = true;
                        this.batchFetchIdx = 0;
                        this.currentTimeseries = null;
                    } else {
                        List<List<String>> showTsList = resp.getShowTimeseriesList();
                        columnItr = showTsList.iterator();
                        this.batchFetchIdx += TsfileJDBCConfig.metaDataBatchFetchSize;
                    }
                } catch (TException e) {
                    throw new SQLException("Cannot fetch result from server, because of network connection");
                }
            }
            if (emptyResultSet) {
                return false;
            }
            currentTimeseries = (List<String>) (columnItr.next());
            return true;
        } else {
            boolean hasNext = columnItr.hasNext();
            if (hasNext) {
                if (type == MetadataType.STORAGE_GROUP) {
                    currentStorageGroup = (String) columnItr.next();
                } else if (type == MetadataType.COLUMN) {
                    currentColumn = (String) columnItr.next();
                }
            }
            return hasNext;
        }
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
			case STORAGE_GROUP:
                if (columnIndex == 1) {
                    return getString(GET_STRING_STORAGE_GROUP);
                }
                break;
            case TIMESERIES:
                if (columnIndex >= 1 && columnIndex <= colCount) {
                    return getString(showLabels[columnIndex - 1]);
                }
                break;
            case COLUMN:
                if (columnIndex == 1) {
                    return getString(GET_STRING_COLUMN);
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

	private enum MetadataType{
		// NOTE: DO NOT CHANGE SEQUENCE OF THE TWO ELEMENTS because @getType() is used in iotdb abstractClient.
        STORAGE_GROUP, TIMESERIES, COLUMN
	}
}
