package cn.edu.tsinghua.iotdb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/*
   This class is mainly used for show timeseries results(TsfileMetadataResultSet)
   to be retrieved in a similar way to TsfileQueryResultSet, for example:

   boolean hasResultSet = statement.execute("show timeseries root.ln.wf01.wt01");
   if(hasResultSet) {
       ResultSet resultSet = statement.getResultSet();
       ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
       while (resultSet.next()) {
           StringBuilder builder = new StringBuilder();
           for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
               builder.append(resultSet.getString(i)).append(",");
           }
           System.out.println(builder);
       }
   }
   statement.close();

   */
public class TsfileMetadataResultMetadata implements ResultSetMetaData {
    private String[] showTsLabels;
    public TsfileMetadataResultMetadata(String[] showTsLabels) {
        this.showTsLabels = showTsLabels;
    }
    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCatalogName(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getColumnClassName(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getColumnCount() throws SQLException {
        if (showTsLabels == null || showTsLabels.length == 0) {
            throw new SQLException("No column exists");
        }
        return showTsLabels.length;
    }

    @Override
    public int getColumnDisplaySize(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        if (showTsLabels == null || showTsLabels.length == 0) {
            throw new SQLException("No column exists");
        }
        if(column > showTsLabels.length){
            throw new SQLException(String.format("column %d does not exist", column));
        }
        if(column <= 0) {
            throw new SQLException(String.format("column index should start from 1", column));
        }
        return showTsLabels[column-1];
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnLabel(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        // TODO Auto-generated method stub
        if (showTsLabels == null || showTsLabels.length == 0) {
            throw new SQLException("No column exists");
        }
        if (column > showTsLabels.length) {
            throw new SQLException(String.format("column %d does not exist", column));
        }
        if (column <= 0) {
            throw new SQLException(String.format("column index should start from 1", column));
        }

        // TEXT
        return Types.VARCHAR;

    }

    @Override
    public String getColumnTypeName(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getPrecision(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getScale(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getSchemaName(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTableName(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isAutoIncrement(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCaseSensitive(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isCurrency(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int isNullable(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isReadOnly(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSearchable(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSigned(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isWritable(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

}
