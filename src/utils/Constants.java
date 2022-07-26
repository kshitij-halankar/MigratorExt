package utils;

public class Constants {
	public static final String MIGRATOR_EXT = "MigratorExt";
	// input connection details
	public static final String INPUT_SOURCE_TYPE = "InputSourceType";
	public static final String INPUT_SOURCE = "InputSource";
	public static final String INPUT_SOURCE_LOGIN_USERNAME = "InputSourceLoginUsername";
	public static final String INPUT_SOURCE_LOGIN_PASSWORD = "InputSourceLoginPassword";

	// input connection details
	public static final String OUTPUT_SOURCE_TYPE = "OutputSourceType";
	public static final String OUTPUT_SOURCE = "OutputSource";
	public static final String OUTPUT_SOURCE_LOGIN_USERNAME = "OutputSourceLoginUsername";
	public static final String OUTPUT_SOURCE_LOGIN_PASSWORD = "OutputSourceLoginPassword";

	// Schema details
	public static final String SCHEMA = "Schema";
	public static final String INPUT_SCHEMA = "InputSchema";
	public static final String OUTPUT_SCHEMA = "OutputSchema";

	// entity details
	public static final String ENTITIES = "Entities";
	public static final String INPUT_ENTITY_NAME = "InputEntityName";
	public static final String OUTPUT_ENTITY_NAME = "OutputEntityName";
	public static final String MAPPINGS = "Mappings";
	public static final String INPUT_ATTRIBUTE_NAME = "InputAttributeName";
	public static final String OUTPUT_ATTRIBUTE_NAME = "OutputAttributeName";

	// error handling
	public static final String MIGRATION_STATUS = "MigrationStatus";
	public static final String FAILURE_CAUSE = "FailureCause";

	// input types
	public static final String MONGO = "Mongo";
	public static final String ORACLE = "Oracle";
	public static final String CSV = "CSV";
	public static final String XML = "XML";
	public static final String JSON = "JSON";

	// sql query
	public static final String SQL_INSERT = "INSERT INTO ";
	public static final String SQL_VALUES = " VALUES ";
	public static final int BATCH_SIZE = 1000;
	
	//response
	public static final String RESPONSE_SUCCESS="Success";
	public static final String RESPONSE_FAILURE="Failure";
	public static final String RESPONSE_STATUS="ResponseStatus";
	public static final String RESPONSE_TOTAL_RECORDS_INSERTED="RecordsInserted";
	public static final String RESPONSE_CAUSE="Cause";
}
