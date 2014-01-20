package sg.smu.index;

import it.uniroma1.dis.wsngroup.gexf4j.core.Edge;
import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeValue;
import it.uniroma1.dis.wsngroup.gexf4j.core.dynamic.Spell;
import it.uniroma1.dis.wsngroup.gexf4j.core.dynamic.TimeFormat;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.SpellImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeValueImpl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import sg.smu.feature.NetworkEdge;
import sg.smu.util.AllDocCollector;
import sg.smu.util.DBConnFactory;
import sg.smu.util.userNode;

public class buildGraphVersion4 {
	//static Log logger = LogFactory.getLog(buildGraphVersion4.class.getName());
	
	private static TreeMap<Long, HashSet<String>> timeUserMap ; //Ascending
	private static HashSet<String> currUserSet; // for simulation twitter message update
	private static HashMap<String, HashSet<String>> userLink = null; // key:user value:followlist
	private static TreeMap<String, userNode> nodesMap= null;
	private static TreeMap<String, Long> edgesMap= null;
	private static HashMap<String, Long> userEarlyTime ;
	private static HashMap<String, Integer> localDegreeMap;
	private static HashMap<String, Integer> userDegreeMap;
	private static HashMap<String, TreeMap<Long, String>> userTimeMap; // user time mid ascending
	private static HashMap<String, HashMap<String, Long>> userMidTimeMap; // user mid time ascending
	private static HashMap<String, String> mid2UserMap;
	
	private static TreeMap<Long, HashSet<String>> time2independUMap;
	private static SimpleDirectedGraph<String,NetworkEdge> graph;
	
	private static String indexPath = "tweetIndex";
	private long totalendTimeStamp = 0;
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public buildGraphVersion4(String endString){
		
		//----------------------------------------------------------
		timeUserMap = new TreeMap<Long, HashSet<String>>();
		userLink = new HashMap<String, HashSet<String>>();
		currUserSet = new HashSet<String>();
		nodesMap = new TreeMap<String, userNode>();
		edgesMap = new TreeMap<String, Long>();
		//----------------------------------------------------------
		userEarlyTime = new HashMap<String, Long>();
		localDegreeMap = new HashMap<String, Integer>();
		userDegreeMap = new HashMap<String, Integer>();
		//----------------------------------------------------------
		userTimeMap = new HashMap<String, TreeMap<Long, String>>();
		userMidTimeMap = new HashMap<String, HashMap<String, Long>>();
		mid2UserMap = new HashMap<String, String>();
		time2independUMap = new TreeMap<Long, HashSet<String>>();
		//-----------------------------------------------------------
		graph = new SimpleDirectedGraph<String, NetworkEdge>(NetworkEdge.class);
	
		try {
			df.setTimeZone(TimeZone.getDefault());
			totalendTimeStamp = df.parse(endString).getTime();
			initUserLink();
			initUserDegree();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initUserDegree() throws Exception{
		
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:userDegree.db");
		Statement stat = conn.createStatement();
		ResultSet rs = stat.executeQuery("select * from userDegree;");
		while(rs.next()) { 
			userDegreeMap.put(rs.getString("user"), Integer.parseInt(rs.getString("degree")));
		}
		rs.close();
		conn.close();
		stat.close();
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

	public void build(String curvePath, String incrPath, String independPath) throws IOException{
	
		BufferedWriter cbWriter = new BufferedWriter(new FileWriter(curvePath));
		BufferedWriter ibWriter = new BufferedWriter(new FileWriter(incrPath));
		
		int prevNumUser = 0;
		long endIncrStamp = -1;
		int prevIncrUser = 0;
		
		for(Map.Entry<Long, HashSet<String>> entry : timeUserMap.entrySet()){
			Long tStamp = entry.getKey();	
			
			if(endIncrStamp == -1){ endIncrStamp = tStamp + 60 * 1000;}
			
			if(tStamp > endIncrStamp){
				ibWriter.write(df.format(new Timestamp(endIncrStamp)) + "\t" + (currUserSet.size() - prevIncrUser));
				ibWriter.newLine();
				ibWriter.flush();
				prevIncrUser = currUserSet.size();
				endIncrStamp += 60*1000;
			}
			
			for(String uid : entry.getValue()){
				// fill node
				if(nodesMap.containsKey(uid)){
					nodesMap.get(uid).putInstance(tStamp);
				}else{
					userNode uNode = new userNode();
					uNode.putInstance(tStamp);
					nodesMap.put(uid, uNode);
				}
				
				if(!graph.containsVertex(uid)){ graph.addVertex(uid); }
				
				// fill edge
				if(currUserSet.size()!=0){						
					if(!userLink.containsKey(uid)){ // get uid's follow's user list							
						currUserSet.add(uid); // the user follow noting
						continue;
					}
					for(String cuid : userLink.get(uid)){
						if(currUserSet.contains(cuid) && !edgesMap.containsKey(cuid+"->"+uid) 
								&& !cuid.equals(uid) && userEarlyTime.get(cuid) < userEarlyTime.get(uid)){
							edgesMap.put(cuid+"->"+uid, tStamp);
							if(localDegreeMap.containsKey(cuid)){
								int tmpNum = localDegreeMap.get(cuid) + 1;
								localDegreeMap.put(cuid, tmpNum);
							}else{
								localDegreeMap.put(cuid, 1);
							}
							if(!cuid.equals(uid) && !graph.containsEdge(cuid,uid)){
								graph.addEdge(cuid, uid, new NetworkEdge(cuid,uid));
							}
						}
					}
				}
				currUserSet.add(uid);		
			} // end this's timestamp's user
			
			HashSet<String> tmpZeroUSet = new HashSet<String>();
			for(String cuser : currUserSet){
				if(graph.inDegreeOf(cuser)==0){ tmpZeroUSet.add(cuser);}
			}
			time2independUMap.put(tStamp, tmpZeroUSet);
	
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
		complementGraph(independPath);
	}
	
	public HashSet<String> getRTs(String mid, String kind) {
		String sql = null;
		if(kind.equals("rt")){
			sql = "select distinct original_status_id from retweet_2011 where status_id = " + mid;
		}else if(kind.equals("rted")){
			sql = "select distinct status_id from retweet_2011 where original_status_id = " + mid;
		}else{
			System.err.println("Input kind(rt/rted) is not allowed.");
			System.exit(-1);
		}

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		HashSet<String> midSet = new HashSet<String>();
		try {
			conn = DBConnFactory.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				if(kind.equals("rt")){
					midSet.add(rs.getString("original_status_id"));
				}else if(kind.equals("rted")){
					midSet.add(rs.getString("status_id"));
				}else{
					System.err.println("Input kind(rt/rted) is not allowed.");
					System.exit(-1);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnFactory.closeDBResource(rs, ps, conn);
			DBConnFactory.stopConnPool();
		}
		return midSet;
	}
	
	public void complementGraph(String independPath) throws IOException{
		
		HashSet<String> newNoneIndependUSet = new HashSet<String>();
		Set<String> allMidSet = mid2UserMap.keySet();
		
		for(String user : graph.vertexSet()){
			if(graph.inDegreeOf(user)==0 && graph.outDegreeOf(user)==0){				
				Long earliestTime = userTimeMap.get(user).firstKey();
				String earliestMid = userTimeMap.get(user).firstEntry().getValue();

				// other -> this user midRTSet
				for(String rtMid : getRTs(earliestMid, "rt")){
					if(rtMid.equals(earliestMid)){ continue; }
					
					if(allMidSet.contains(rtMid)){
						String fromUid = mid2UserMap.get(rtMid);
						String toUid = mid2UserMap.get(earliestMid);
						if(!edgesMap.containsKey(fromUid+"->"+toUid) && !fromUid.equals(toUid) ){
							edgesMap.put(fromUid+"->"+toUid, earliestTime);
							if(localDegreeMap.containsKey(fromUid)){
								int tmpNum = localDegreeMap.get(fromUid) + 1;
								localDegreeMap.put(fromUid, tmpNum);
							}else{
								localDegreeMap.put(fromUid, 1);
							}
							newNoneIndependUSet.add(user);
						}// end of add edge
					}
				}
				// this user -> other midRTEDSet
				for(String rtedMid : getRTs(earliestMid, "rted")){
					if(rtedMid.equals(earliestMid)){ continue; }
					if(allMidSet.contains(rtedMid)){
						String fromUid = mid2UserMap.get(earliestMid);
						String toUid = mid2UserMap.get(rtedMid);
						if(!edgesMap.containsKey(fromUid+"->"+toUid) && !fromUid.equals(toUid) ){
							edgesMap.put(fromUid+"->"+toUid, userMidTimeMap.get(toUid).get(rtedMid));
							if(localDegreeMap.containsKey(fromUid)){
								int tmpNum = localDegreeMap.get(fromUid) + 1;
								localDegreeMap.put(fromUid, tmpNum);
							}else{
								localDegreeMap.put(fromUid, 1);
							}
						} //end of add edge
					}
				}//end of rted
			}
		} // end all user
		
		// write the curve of independent user
		int prevIndpendUser = 0;
		BufferedWriter bw = new BufferedWriter(new FileWriter(independPath));
		for(Map.Entry<Long, HashSet<String>> entry : time2independUMap.entrySet()){
			int count = 0;
			for(String user : entry.getValue()){
				if(!newNoneIndependUSet.contains(user)){
					count++;
				}
			}
			if(count != prevIndpendUser){
				bw.write(df.format(new Timestamp(entry.getKey())) + "\t" + count);
				bw.newLine();
				bw.flush();
				prevIndpendUser = count;
			}
		}
		bw.close();
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
			// add user time map
			TreeMap<Long, String> tmpTimeMap = null;
			HashMap<String, Long> tmpMidMap = null;
			if(userTimeMap.containsKey(uid)){
				tmpTimeMap = userTimeMap.get(uid);
				tmpMidMap = userMidTimeMap.get(uid);
			}else{
				tmpTimeMap = new TreeMap<Long, String>();
				tmpMidMap = new HashMap<String,Long>();
			}
			tmpTimeMap.put(Long.parseLong(doc.get("catime")), doc.get("tweetid"));
			tmpMidMap.put(doc.get("tweetid"), Long.parseLong(doc.get("catime")));
			userTimeMap.put(uid, tmpTimeMap);
			userMidTimeMap.put(uid, tmpMidMap);
			mid2UserMap.put(doc.get("tweetid"), uid);
		}
	}
	
	public void generateGexf(String gexfPath) throws ParseException{
		
		Node node = null;
		Spell spell = null;
		AttributeValue attValue = null;
		Gexf gexf = new GexfImpl();
		Calendar dateS = Calendar.getInstance(TimeZone.getTimeZone("UTC+08:00"));
		Calendar dateE = Calendar.getInstance(TimeZone.getTimeZone("UTC+08:00"));
		Calendar TotaldateE = Calendar.getInstance(TimeZone.getTimeZone("UTC+08:00"));
		TotaldateE.setTimeInMillis(totalendTimeStamp);
		gexf.getMetadata().setLastModified(dateS.getTime()).setCreator("SMU").setDescription("Dynamic Tweet Network");
		gexf.setVisualization(true);

		Graph graph = gexf.getGraph();
		graph.setDefaultEdgeType(EdgeType.DIRECTED).setMode(Mode.DYNAMIC).setTimeType(TimeFormat.XSDDATETIME);

		AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(attrList);
		
		Attribute attDegree = attrList.createAttribute("2", AttributeType.INTEGER, "degree");
		Attribute attLocalDegree = attrList.createAttribute("3", AttributeType.INTEGER, "local degree");
		Attribute attRatio = attrList.createAttribute("4", AttributeType.DOUBLE, "degree ratio");
		AttributeList attrListNodeDynamic = new AttributeListImpl(AttributeClass.NODE).setMode(Mode.DYNAMIC);
		graph.getAttributeLists().add(attrListNodeDynamic);
		Attribute attRelTweets = attrListNodeDynamic.createAttribute("1",AttributeType.INTEGER, "relevant tweets");
		
		// add nodes
		HashMap<String, Node> id2NodeMap = new HashMap<String, Node>();
		for (Map.Entry<String, userNode> entry : nodesMap.entrySet()) {
			node = graph.createNode(entry.getKey().trim());
			node.setLabel(entry.getKey().trim());

			Iterator<Map.Entry<Long,Integer>> iterator = entry.getValue().getDnRelMap().entrySet().iterator();

			int number = 0;
			long timestamp = 0;
			
			Map.Entry<Long, Integer> m = null;

			if (entry.getValue().getDnRelMap().size() == 0) {
				continue;
			} else {
				int degree = 0;
				if(userDegreeMap.containsKey(entry.getKey())){
					degree = userDegreeMap.get(entry.getKey());
				}
	
				int localDegree = 0;
				if(localDegreeMap.containsKey(entry.getKey())){
					localDegree = localDegreeMap.get(entry.getKey());
				}
	
				double insertRatio = 0;
				if(degree == 0){
					insertRatio = -1;
				}else{
					BigDecimal bg = new BigDecimal((double)localDegree / (double) degree);
					insertRatio = bg.setScale(5, BigDecimal.ROUND_HALF_UP).doubleValue();
				}
				
				node.getAttributeValues().addValue(attDegree,String.valueOf(degree)).addValue(attLocalDegree, String.valueOf(localDegree))
					.addValue(attRatio, String.valueOf(insertRatio));
				
				m =  iterator.next();
				number = m.getValue();
				timestamp = m.getKey();
				dateS.setTimeInMillis(timestamp);
				
				// set spell
				spell = new SpellImpl();
				spell.setStartValue(dateS.getTime());
				spell.setEndValue(TotaldateE.getTime());
				
				node.getSpells().add(spell);
				
				while (iterator.hasNext()) {
					m =  iterator.next();
					dateE.setTimeInMillis(m.getKey() );
					attValue = new AttributeValueImpl(attRelTweets);
					attValue.setValue(String.valueOf(number));
					attValue.setStartValue(dateS.getTime());
					attValue.setEndValue(dateE.getTime());
					node.getAttributeValues().add(attValue);
					number = m.getValue();
					dateS.setTimeInMillis(m.getKey());
				}

				attValue = new AttributeValueImpl(attRelTweets);
				attValue.setValue(String.valueOf(number));
				attValue.setStartValue(dateS.getTime());
				attValue.setEndValue(TotaldateE.getTime());
				node.getAttributeValues().add(attValue);
			}
			if (!id2NodeMap.containsKey(entry.getKey())) {
				id2NodeMap.put(entry.getKey(), node);
			}
		}

		// add edges
		int edgecount = 0;
		for (Map.Entry<String, Long> entry : edgesMap.entrySet()) {
			String[] splits = entry.getKey().split("->");
			if (!id2NodeMap.get(splits[0]).hasEdgeTo(id2NodeMap.get(splits[1]).getId())) {
				Edge edge = id2NodeMap.get(splits[0]).connectTo(String.valueOf(edgecount),String.valueOf(edgecount),
						EdgeType.DIRECTED,id2NodeMap.get(splits[1]));
				spell = new SpellImpl();
				dateS.setTimeInMillis(entry.getValue());
				spell.setStartValue(dateS.getTime());
				spell.setEndValue(TotaldateE.getTime());
				edge.getSpells().add(spell);
			}
			edgecount++;
		}

		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File file = new File(gexfPath);
		Writer out = null;
		try {
			out = new FileWriter(file, false);
			graphWriter.writeToStream(gexf, out, "UTF-8");
			System.out.println(file.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
