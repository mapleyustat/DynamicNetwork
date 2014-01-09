package sg.smu.sqlite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.security.interfaces.RSAKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import sg.smu.util.AllDocCollector;
import sg.smu.util.DBConnFactory;
import sg.smu.util.userNode;

public class buildSqliteFile {
	
	static Log logger = LogFactory.getLog(buildSqliteFile.class.getName());
	
	private static HashSet<String> userSet; 
	
	private static void initUserDegree() throws Exception{
		String sql ="select * from degrees";
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		BufferedWriter bw = new BufferedWriter(new FileWriter("./sqliteData/user.degrees"));
		
		try {
			conn = DBConnFactory.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				bw.write(rs.getString("id") + "\t" + rs.getString("degree"));
				bw.newLine();
				bw.flush();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnFactory.closeDBResource(rs, ps, conn);
			DBConnFactory.stopConnPool();
			bw.close();
		}
	}
	
	private static void initUserSet() throws Exception{
		String sql ="select distinct followerId from links";
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		userSet = new HashSet<String>();
		try {
			conn = DBConnFactory.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				userSet.add(rs.getString("followerId"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnFactory.closeDBResource(rs, ps, conn);
			DBConnFactory.stopConnPool();
		}
	}
	
	private static void initUserLink() throws Exception{

		BufferedWriter bw = new BufferedWriter(new FileWriter("./sqliteData/userAll.links"));
		int count = 0;
		for(String user : userSet){
			
			bw.write(user + "\t" + queryLink(user));
			bw.newLine();
			bw.flush();
			System.out.println((count++) +"\t" + userSet.size() );
			
		}
		bw.close();
		DBConnFactory.stopConnPool();
	}
	
	
	
	public static HashSet<String> queryLink(String uid){
		String sql ="select id from links where followerId = " + uid;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		HashSet<String> followSet = new HashSet<>();
		try {
			conn = DBConnFactory.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while(rs.next()){
				followSet.add(rs.getString("id"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnFactory.closeDBResource(rs, ps, conn);
			DBConnFactory.stopConnPool();
		}
		return followSet;
	}

	private static void search(String keyword) throws Exception{
		QueryParser parser = new QueryParser(Version.LUCENE_35, "text", new StandardAnalyzer(Version.LUCENE_35));
		Query query = parser.parse(keyword);
		Directory fsDir = FSDirectory.open(new File("tweetIndex1129"));
		AllDocCollector fsCollector = new AllDocCollector();
		IndexReader iReader = IndexReader.open(fsDir);
		IndexSearcher iSearcher = new IndexSearcher(iReader);
		iSearcher.search(query, fsCollector);
	
		List<ScoreDoc> list =  fsCollector.getHits();
		for(ScoreDoc scoreDoc : list){
			Document document = iSearcher.doc(scoreDoc.doc);
			String uid = document.get("userid");
			
			if(uid.equals("103681236")){
				System.out.println(document.get("catime") + "\t" + document.get("text") + "\t" + document.get("tweetid"));
				System.out.println(new Timestamp(Long.parseLong(document.get("catime"))).toLocaleString());
			}
			
			//userSet.add(uid);
		}
		System.out.println(userSet.size());
	}
	
	public static void main(String[] args) throws Exception {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC+08:00"));
		initUserSet();
		initUserLink();
		//userSet = new HashSet<String>();
	
		//search("steve");
		//search("train");
		//search("album");
		//initUserLink();	
	}
}
