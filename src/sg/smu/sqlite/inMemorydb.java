package sg.smu.sqlite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;

public class inMemorydb {

	public static String setToString(String setSTR, String separator) {
		if (setSTR == null || setSTR.isEmpty()) {
			return "";
		}
		return setSTR.substring(1, setSTR.length()-1).replace(",", separator).trim();
	}
	
	public static void insertSqlit() throws Exception{
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:userLinks.db");
		Statement stat = conn.createStatement();
		stat.executeUpdate("drop table if exists userFollow;");
		stat.executeUpdate("create table userFollow (user, followlist);");
		PreparedStatement prep = conn.prepareStatement("insert into userFollow values (?, ?);");

		BufferedReader br = new BufferedReader(new FileReader("./sqliteData/userAll.links"));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split("\t");
			prep.setString(1, tokens[0].trim());
			prep.setString(2, setToString(tokens[1], " "));
			prep.addBatch();
		}
		conn.setAutoCommit(false); 
		prep.executeBatch();
		conn.setAutoCommit(true);
		prep.close();
		br.close();
		stat.close();
		conn.close();
	}

	public static void insertDegreeSqlit() throws Exception{
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:userDegree.db");
		Statement stat = conn.createStatement();
		stat.executeUpdate("drop table if exists userDegree;");
		stat.executeUpdate("create table userDegree (user, degree);");
		PreparedStatement prep = conn.prepareStatement("insert into userDegree values (?, ?);");

		BufferedReader br = new BufferedReader(new FileReader("./sqliteData/user.degrees"));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split("\t");
			prep.setString(1, tokens[0].trim());
			prep.setInt(2, Integer.parseInt(tokens[1]));
			prep.addBatch();
		}
		conn.setAutoCommit(false); 
		prep.executeBatch();
		conn.setAutoCommit(true);
		prep.close();
		br.close();
		stat.close();
		conn.close();
	}
	
	public static void searchSqlit(String uid) throws Exception{
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:linkAlbum.db");
	
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery("select * from userFollow where user='" + uid + "';");
		System.out.println(rs.getFetchSize());
		System.out.println("select followlist from userFollow where user = " + uid + ";");
		while(rs.next()) { 
			System.out.println("job = " + rs.getString("followlist")); 
		}
			
		rs.close();
		conn.close();
		stat.close();
	}
	public static void main(String[] args) throws Exception {
		insertSqlit();
		insertDegreeSqlit();
		//insertSqlit();
		//searchSqlit("214335696");
		/*
		 * prep.setString(1, "Gandhi"); prep.setString(2, "politics");
		 * prep.addBatch(); prep.setString(1, "Turing"); prep.setString(2,
		 * "computers"); prep.addBatch(); prep.setString(1, "Wittgenstein");
		 * prep.setString(2, "smartypants"); prep.addBatch();
		 * 
		 * conn.setAutoCommit(false); prep.executeBatch();
		 * conn.setAutoCommit(true);
		 * 
		 * ResultSet rs = stat.executeQuery("select * from people;"); while
		 * (rs.next()) { System.out.println("name = " + rs.getString("name"));
		 * System.out.println("job = " + rs.getString("occupation")); }
		 * rs.close(); conn.close();
		 */
	}

}
