package metadata;

import org.json.JSONObject;

import utils.Constants;

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

//	public DBPropertiesObj(JSONObject metadata) {
//		if (metadata.has(Constants.INPUT_SOURCE))
//			inputSource = metadata.get(Constants.INPUT_SOURCE).toString();
//		if (metadata.has(Constants.OUTPUT_SOURCE))
//			outputSource = metadata.get(Constants.OUTPUT_SOURCE).toString();
//		if (metadata.has(Constants.INPUT_TYPE))
//			inputType = metadata.get(Constants.INPUT_TYPE).toString();
//		if (metadata.has(Constants.OUTPUT_TYPE))
//			outputType = metadata.get(Constants.OUTPUT_TYPE).toString();
//		if (metadata.has(Constants.COLLECTION_NAME))
//			collectionName = metadata.get(Constants.COLLECTION_NAME).toString();
//		if (metadata.has(Constants.TABLE_NAME))
//			tableName = metadata.get(Constants.TABLE_NAME).toString();
//		if (metadata.has(Constants.COLUMN_NAME))
//			columnName = metadata.get(Constants.COLUMN_NAME).toString();
//		if (metadata.has(Constants.INPUT_CONNECTION_STRING))
//			inputConnectionString = metadata.get(Constants.INPUT_CONNECTION_STRING).toString();
//		if (metadata.has(Constants.OUTPUT_CONNECTION_STRING))
//			outputConnectionString = metadata.get(Constants.OUTPUT_CONNECTION_STRING).toString();
//		if (metadata.has(Constants.DB_USER_NAME))
//			dbUserName = metadata.get(Constants.DB_USER_NAME).toString();
//		if (metadata.has(Constants.DB_PASSWORD))
//			dbPassword = metadata.get(Constants.DB_PASSWORD).toString();
//		if (metadata.has(Constants.INCLUDED_COLUMNS))
//			includedColumns = metadata.get(Constants.INCLUDED_COLUMNS).toString();
//	}

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
