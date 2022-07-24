package connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.OracleConnection;
import java.sql.DatabaseMetaData;

public class OracleDBConnector {

	String dbURL = "";
	String dbUsername = "";
	String dbPass = "";

//	final static String DB_URL = "jdbc:oracle:thin:@db20220717220553_high?TNS_ADMIN=./Wallet_DB20220717220553";
	final static String DB_URL = "jdbc:oracle:thin:@localhost:1521:xe";
	final static String DB_USER = "SYSTEM";
	final static String DB_PASSWORD = "12345678";

//	try (Connection conn = DriverManager.getConnection(
//            "jdbc:oracle:thin:@localhost:1521:orcl", "system", "Password123")) {
//
//        if (conn != null) {
//            System.out.println("Connected to the database!");
//        } else {
//            System.out.println("Failed to make connection!");
//        }
//
//    } catch (SQLException e) {
//        System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
//    } catch (Exception e) {
//        e.printStackTrace();
//    }

	public void setDBURL(String dbURL) {
		this.dbURL = dbURL;
	}

	public void setDBUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}

	public void setDBPass(String dbPass) {
		this.dbPass = dbPass;
	}

//	public Connection getConnection() {
//		Connection conn = null;
//		try {
//			conn = DriverManager.getConnection(dbURL, dbUsername, dbPass);
//		} catch (Exception ex) {
//
//		}
//		return conn;
//	}

	public Connection getConnection(String dbURL, String dbUser, String dbPassword) throws SQLException {
		Connection con = DriverManager.getConnection(dbURL, dbUser, dbPassword);
		return con;
//		Properties info = new Properties();
//		info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
//		info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
////		info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");
//		OracleDataSource ods = new OracleDataSource();
//		ods.setURL(DB_URL);
//		ods.setConnectionProperties(info);
//		System.out.println(System.getProperty("user.dir"));
//		try (Connection connection = ods.getConnection()) {
//			DatabaseMetaData dbmd = connection.getMetaData();
//			System.out.println("Driver Name: " + dbmd.getDriverName());
////			System.out.println("Driver Version: " + dbmd.getDriverVersion());
//////		System.out.println("Default Row Prefetch Value is: " +
////			System.out.println("Database Username is: " + connection.getUserName());
//			System.out.println();
//			return connection;
//		}
	}
}
