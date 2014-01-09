package sg.smu.util;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import snaq.db.ConnectionPool;
import snaq.db.Select1Validator;
import snaq.db.SimpleQueryValidator;


import sg.smu.util.Config;

public class DBConnFactory {

	static Log logger = LogFactory.getLog(DBConnFactory.class.getName());
	
	private static ConnectionPool poolDevt;
	private static ConnectionPool poolProd;
	private static final int MIN_POOL = 6;
	private static final int MAX_POOL = 10;
	private static final int MAX_SIZE = 10;
	private static final int IDLE_TIMEOUT = 6 * 3600;
	//private static final int IDLE_TIMEOUT = 2 * 60;
	private static final int WAIT_FOR_CONNECTION = 2000;
	
	public static void initConnectionPools(){
		String schema = Config.getParameter("dbname");
		initDevtConnectionPool(schema);
		initProdConnectionPool(schema);
	}

	public static void initDevtConnectionPool(String schemaName){

		try {
			Class c = Class.forName("com.mysql.jdbc.Driver");
			Driver driver;
			driver = (Driver)c.newInstance();
			DriverManager.registerDriver(driver);
			String ipaddr = Config.getParameter("dbhost");
			String jdbc = "jdbc:mysql://"+ipaddr+":3306/"+schemaName+"";
			String user = Config.getParameter("dbuser");
			String pass = Config.getParameter("dbpasswd");
		
			poolDevt = new ConnectionPool("devsvr_pool",
					MIN_POOL,
					MAX_POOL,
					MAX_SIZE,
					IDLE_TIMEOUT,
					jdbc,
					user,
					pass);
			poolDevt.setValidator(new Select1Validator());
			poolDevt.init();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	public static void initProdConnectionPool(String schemaName){
		String ipaddr = Config.getParameter("prod_dbhost");
		String jdbc = "jdbc:mysql://"+ipaddr+":3306/"+schemaName+"";
		String user = Config.getParameter("prod_dbuser");
		String pass = Config.getParameter("prod_dbpasswd");
		
		try {
			Class c = Class.forName("com.mysql.jdbc.Driver");
			Driver driver;
			driver = (Driver)c.newInstance();
			DriverManager.registerDriver(driver);
			poolProd = new ConnectionPool("prodsvr_pool",
					MIN_POOL,
					MAX_POOL,
					MAX_SIZE,
					IDLE_TIMEOUT,
					jdbc,
					user,
					pass);
			poolProd.setValidator(new Select1Validator());
			poolProd.init();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	public static Connection getConnectionWODataSource(String schemaName){
		if(schemaName == null || schemaName.length() == 0)
			return null;
		try {
			if(poolDevt == null || poolDevt.isReleased()){
				initDevtConnectionPool(schemaName);
			}
			Connection conn =  (Connection) poolDevt.getConnection();
			
			while(conn == null){
				System.out.println("Failed to get connection to Devt Svr.Sleep for "+WAIT_FOR_CONNECTION/1000+" sec.");
				Thread.sleep(WAIT_FOR_CONNECTION);
				System.out.println("Retry get connection to Devt Svr.");
				conn = getConnection();
				
			}
			return conn;
		} catch (SQLException e) {
			logger.info(e);
		} catch (InterruptedException e) {
			logger.info(e);
		}
		return null;
	}

	public static Connection getProductionConnectionWODataSource(String schemaName){
		if(schemaName == null || schemaName.length() == 0)
			return null;
		try {
			if(poolProd == null || poolProd.isReleased()){
				initProdConnectionPool(schemaName);
			}
			Connection conn = (Connection) poolProd.getConnection();
			
			while(conn == null){
				System.out.println("Failed to get connection to Prod Svr.Sleep for "+WAIT_FOR_CONNECTION/1000+" sec.");
				Thread.sleep(WAIT_FOR_CONNECTION);
				System.out.println("Retry get connection to Prod Svr.");
				conn = getProductionConnection();
			}
			return conn;
		} catch (SQLException e) {
			logger.info(e);
		} catch (InterruptedException e) {
			logger.info(e);
		}
		return null;
	}

	public static Connection getConnection(){
		String schema = Config.getParameter("dbname");
		return getConnectionWODataSource(schema);
	}

	public static Connection getProductionConnection(){
		String schema = Config.getParameter("prod_dbname");
		return getProductionConnectionWODataSource(schema);
	}

	public static void closeConnection(Connection conn){
		if(conn == null )
			return ;
		try {
			if(!conn.isClosed())
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void stopConnPool(){
		if(poolDevt != null && !poolDevt.isReleased()){
			poolDevt.release();
		}
		
		if(poolProd != null && !poolProd.isReleased()){
			poolProd.release();
		}
	}
	
	public static void closeDBResource(ResultSet rs, PreparedStatement ps, Connection conn){
		try {
			if(rs != null && !rs.isClosed()){
				rs.close();
			}

			if(ps != null && !ps.isClosed()){
				ps.close();
			}
			
			DBConnFactory.closeConnection(conn);
			
		}catch (SQLException e) {

			e.printStackTrace();
		}
	}
	
	public static void closeDBResource(PreparedStatement ps, Connection conn){
		try {

			if(ps != null && !ps.isClosed()){
				ps.close();
			}
			
			DBConnFactory.closeConnection(conn);
			
		}catch (SQLException e) {

			e.printStackTrace();
		}
	}
	
	public static void closeDBResource(ResultSet rs, Statement ps, Connection conn){
		try {
			if(rs != null && !rs.isClosed()){
				rs.close();
			}

			if(ps != null && !ps.isClosed()){
				ps.close();
			}
			
			DBConnFactory.closeConnection(conn);
			
		}catch (SQLException e) {

			e.printStackTrace();
		}
	}
}
