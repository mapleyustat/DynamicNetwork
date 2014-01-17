package sg.smu.index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.jgrapht.graph.SimpleDirectedGraph;

import sg.cmu.analyzer.cmuAnalyzer;
import sg.smu.util.AllDocCollector;
import sg.smu.util.NetworkEdge;
import sg.smu.util.userNode;

public class buildGraphVersion3 {
	
	static Log logger = LogFactory.getLog(buildGraphVersion3.class.getName());
	
	private static TreeMap<Long, HashSet<String>> timeUserMap ; //Ascending
	private static HashSet<String> currUserSet; // for simulation twitter message update
	private static HashMap<String, HashSet<String>> userLink = null; // key:user value:followlist
	private static TreeMap<String, userNode> nodesMap= null;
	private static TreeMap<String, Long> edgesMap= null;
	private static HashMap<String, Long> userEarlyTime ;
	
	private static SimpleDirectedGraph<String,NetworkEdge> graph;
	
	private static String indexPath = "tweetIndex";
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public buildGraphVersion3(){
		
		timeUserMap = new TreeMap<Long, HashSet<String>>();
		userLink = new HashMap<String, HashSet<String>>();
		currUserSet = new HashSet<String>();
		nodesMap = new TreeMap<String, userNode>();
		edgesMap = new TreeMap<String, Long>();
		userEarlyTime = new HashMap<String, Long>();
		
		graph = new SimpleDirectedGraph<String, NetworkEdge>(NetworkEdge.class);
		try {
			df.setTimeZone(TimeZone.getDefault());
			initUserLink();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initUserLink() throws Exception{
		
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:userLinks.db");
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery("select * from userFollow;");
		
		while(rs.next()) { 
			HashSet<String> tmpSet = new HashSet<String>();
			String[] splits = rs.getString("followlist").trim().split(" ");
			for(int i=0;i<splits.length;i++){
				if(splits[i].isEmpty()) { continue; }
				tmpSet.add(splits[i]);
			}
			userLink.put(rs.getString("user"), tmpSet);
		}
		rs.close();
		conn.close();
		stat.close();
	}

	public void build(String curvePath, String indpendPath, String incrPath) throws IOException{
	
		BufferedWriter cbWriter = new BufferedWriter(new FileWriter(curvePath));
		BufferedWriter zInbWriter = new BufferedWriter(new FileWriter(indpendPath));
		BufferedWriter ibWriter = new BufferedWriter(new FileWriter(incrPath));
		
		int prevNumUser = 0;
		int prevIndpendUser = 0;
		long endIncrStamp = -1;
		int prevIncrUser = 0;
		
		for(Map.Entry<Long, HashSet<String>> entry : timeUserMap.entrySet()){
			
			Long tStamp = entry.getKey();
			
			if(endIncrStamp == -1){
				endIncrStamp = tStamp + 60 * 1000;
			}
			
			// write increment 
			if(tStamp > endIncrStamp){
				int incrUserNum = currUserSet.size() - prevIncrUser;
				ibWriter.write(df.format(new Timestamp(endIncrStamp)) + "\t" + incrUserNum);
				ibWriter.newLine();
				ibWriter.flush();
				prevIncrUser = currUserSet.size();
				endIncrStamp += 60*1000;
			}

			HashSet<String> uSet = entry.getValue();
			for(String uid : uSet){
				// fill node
				if(nodesMap.containsKey(uid)){
					nodesMap.get(uid).putInstance(tStamp);
				}else{
					userNode uNode = new userNode();
					uNode.putInstance(tStamp);
					nodesMap.put(uid, uNode);
				}
				
				if(!graph.containsVertex(uid)){
					graph.addVertex(uid);
				}
				// fill edge
				if(currUserSet.size()!=0){
					// get uid's follow's user list		
					if(!userLink.containsKey(uid)){
						currUserSet.add(uid);
						continue;
					}
					HashSet<String> tmpSet = userLink.get(uid);		
					for(String cuid : tmpSet){
						if(currUserSet.contains(cuid) && !edgesMap.containsKey(cuid+"->"+uid) && !cuid.equals(uid) && userEarlyTime.get(cuid) < userEarlyTime.get(uid)){
							edgesMap.put(cuid+"->"+uid, tStamp);
							
							if(!cuid.equals(uid) && !graph.containsEdge(cuid,uid)){
								graph.addEdge(cuid, uid, new NetworkEdge(cuid,uid));
							}
						}
					}
				}
				currUserSet.add(uid);
			} // end this's timestamp's user	
			
			int zeroInDegree = 0;
			for(String cuser : currUserSet){
				if(graph.inDegreeOf(cuser)==0){
					zeroInDegree++;
				}
			}
			
			if(zeroInDegree != prevIndpendUser){
				zInbWriter.write(df.format(new Timestamp(entry.getKey())) + "\t" + zeroInDegree);
				zInbWriter.newLine();
				zInbWriter.flush();
				prevIndpendUser = zeroInDegree;
			}

			// write relevant user's curve
			if(currUserSet.size() != prevNumUser){
				cbWriter.write(df.format(new Timestamp(entry.getKey())) + "\t" + currUserSet.size());
				cbWriter.newLine();
				cbWriter.flush();
				prevNumUser = currUserSet.size();
			}
		} // end all timestamp 
		
		int lastIncr = currUserSet.size() - prevIncrUser;
		if(lastIncr != 0 ){
			ibWriter.write(df.format(new Timestamp(endIncrStamp)) + "\t" + lastIncr);
			ibWriter.newLine();
			ibWriter.flush();
		}
		ibWriter.close();
		cbWriter.close();
		zInbWriter.close();
	}
	
	public void search(String keyword,String analyzerKind) throws Exception{
		
		QueryParser parser = null;
		if(analyzerKind.equals("s")){
			parser = new QueryParser(Version.LUCENE_35, "text", new StandardAnalyzer(Version.LUCENE_35));
		}else if(analyzerKind.equals("c")){
			parser = new QueryParser(Version.LUCENE_35, "text", new cmuAnalyzer(Version.LUCENE_35));
		}else{
			System.err.println("Input analyzer kind is not allowed! Options: c / s");
			System.exit(-1);
		}
		
		Query query = parser.parse(keyword);
		Directory fsDir = FSDirectory.open(new File(indexPath));
		AllDocCollector fsCollector = new AllDocCollector();
		IndexSearcher iSearcher = new IndexSearcher(IndexReader.open(fsDir));
		iSearcher.search(query, fsCollector);
		List<ScoreDoc> list =  fsCollector.getHits();
		
		for(ScoreDoc scoreDoc : list){
			Document doc = iSearcher.doc(scoreDoc.doc);
			Long dtime = Long.parseLong(doc.get("catime"));
			String uid = doc.get("userid");
			HashSet<String> tmpSet = null;
			if(timeUserMap.containsKey(dtime)){
				tmpSet = timeUserMap.get(dtime);
			}else{
				tmpSet = new HashSet<String>();
			}
			tmpSet.add(uid);
			timeUserMap.put(dtime, tmpSet);
			
			// add user's early timestamp
			if(userEarlyTime.containsKey(uid)){
				if(userEarlyTime.get(uid) > dtime){
					userEarlyTime.put(uid, dtime);
				}
			}else{
				userEarlyTime.put(uid, dtime);
			}
		}
	}
}
