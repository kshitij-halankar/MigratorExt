package metadata;

public class DBPropertiesObj {
	public String inputSource = "";
	public String outputSource = "";
	public String inputType = "";
	public String outputType = "";
	public String collectionName = "";
	public String tableName = "";
	public String columnName = "";
	public String inputConnectionString = "";
	public String outputConnectionString = "";
	public String dbUserName = "";
	public String dbPassword = "";
	public String includedColumns = "";

	public String getInputSource() {
		return inputSource;
	}

	public String getOutputSource() {
		return outputSource;
	}

	public String getInputType() {
		return inputType;
	}

	public String getOutputType() {
		return outputType;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getInputConnectionString() {
		return inputConnectionString;
	}

	public String getOutputConnectionString() {
		return outputConnectionString;
	}

	public String getBbUserName() {
		return dbUserName;
	}

	public String getBbPassword() {
		return dbPassword;
	}

	public String getIncludedColumns() {
		return includedColumns;
	}
}
