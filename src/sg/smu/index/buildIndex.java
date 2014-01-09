package sg.smu.index;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;

import com.ctc.wstx.dtd.DFAState;

import sg.cmu.analyzer.cmuAnalyzer;
import sg.smu.util.DBConnFactory;

public class buildIndex {

	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String indexPath = "tweetIndex";
	
	public static String formatTime(String time){
		return "\"" +time + "\"" ;
	}
	
	@SuppressWarnings("deprecation")
	public static void buildTweetIndex(String startTime, String endTime, String analyzerKind){

		File file = new File(indexPath);
		if(file.exists()){
			file.delete();
		}
		file.mkdir();
		Directory fsDir = null;
		try {
			fsDir = FSDirectory.open(file);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		Analyzer analyzer = null;
		if(analyzerKind.equals("s")){
			analyzer = new StandardAnalyzer(Version.LUCENE_35);
		}else if(analyzerKind.equals("c")){
			analyzer = new cmuAnalyzer(Version.LUCENE_35);
		}else{
			System.err.println("Input analyzer kind is not allowed! Options: c / s");
			System.exit(-1);
		}
		
		IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_35,analyzer);
		iwc.setOpenMode(OpenMode.CREATE);
		IndexWriter iWriter = null;
		try {
			iWriter = new IndexWriter(fsDir, iwc);
		}  catch (Exception e) {
			e.printStackTrace();
		}	
		
		String sql = "select status_ID,user_ID,tweet,t from tweets where t >= " + formatTime(startTime) + " and t< " + formatTime(endTime);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = DBConnFactory.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			
			while(rs.next()){
				Document doc = new Document();
				Field tidField = new Field("tweetid",rs.getString("status_ID"),Field.Store.YES, Field.Index.NOT_ANALYZED);
				doc.add(tidField);
				Field uidField = new Field("userid",rs.getString("user_ID"),Field.Store.YES, Field.Index.NOT_ANALYZED);
				doc.add(uidField);
				Field textFiled = new Field("text",rs.getString("tweet"),Field.Store.YES, Field.Index.ANALYZED);
				doc.add(textFiled);
				doc.add(new NumericField("catime",NumericUtils.PRECISION_STEP_DEFAULT,Field.Store.YES, true).setLongValue(df.parse(rs.getString("t")).getTime()));
				iWriter.addDocument(doc);	
			}
			iWriter.commit();
			iWriter.optimize();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnFactory.closeDBResource(rs, ps, conn);
			DBConnFactory.stopConnPool();
			try {
				iWriter.close();
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
