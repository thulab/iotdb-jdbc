package cn.edu.tsinghua.iotdb.jdbc;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ColumnSchema implements Serializable {
	private static final long serialVersionUID = -8257474930341487207L;
	// column name
	public String name;
	public TSDataType dataType;
	public TSEncoding encoding;
	private Map<String, String> args;

	public ColumnSchema(String name, TSDataType dataType, TSEncoding encoding) {
		this.name = name;
		this.dataType = dataType;
		this.encoding = encoding;
		this.args = new HashMap<>();
	}

	public void putKeyValueToArgs(String key, String value) {
		this.args.put(key, value);
	}

	public String getValueFromArgs(String key) {
		return args.get(key);
	}

	public Map<String, String> getArgsMap() {
		return args;
	}

	public void setArgsMap(Map<String, String> argsMap) {
		this.args = argsMap;
	}
}
