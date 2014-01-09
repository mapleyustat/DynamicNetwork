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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import sg.cmu.analyzer.cmuAnalyzer;
import sg.smu.util.AllDocCollector;
import sg.smu.util.userNode;

public class buildGraphVersion1 {
	
	static Log logger = LogFactory.getLog(buildGraphVersion1.class.getName());
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private TreeMap<Long, HashSet<String>> timeUserMap ; //Ascending
	private HashSet<String> currUserSet; // for simulation twitter message update
	private HashMap<String, HashSet<String>> userLink = null; // key:user value:followlist
	private TreeMap<String, userNode> nodesMap= null;
	private TreeMap<String, Long> edgesMap= null;
	private long totalendTimeStamp = 0;
	private static String indexPath = "tweetIndex";
	
	public buildGraphVersion1(String endString){
		timeUserMap = new TreeMap<Long, HashSet<String>>();
		userLink = new HashMap<String, HashSet<String>>();
		currUserSet = new HashSet<String>();
		nodesMap = new TreeMap<String, userNode>();
		edgesMap = new TreeMap<String, Long>();
		try {
			df.setTimeZone(TimeZone.getDefault());
			totalendTimeStamp = df.parse(endString).getTime();
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

	public void build(){
		
		for(Map.Entry<Long, HashSet<String>> entry : timeUserMap.entrySet()){
			Long tStamp = entry.getKey();
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
				// fill edge
				if(currUserSet.size()!=0){
					// get uid's follow's user list
					if(!userLink.containsKey(uid)){
						currUserSet.add(uid);
						// the user follow noting
						continue;
					}
					HashSet<String> tmpSet = userLink.get(uid);
					for(String cuid : tmpSet){
						if(currUserSet.contains(cuid) && !edgesMap.containsKey(cuid+"->"+uid)){
							edgesMap.put(cuid+"->"+uid, tStamp);
						}
					}
				}
				currUserSet.add(uid);
			} // end this's timestamp's user
		} // end all timestamp 
	}
	
	public void search(String keyword, String analyzerKind) throws Exception{
		
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
			HashSet<String> tmpSet = null;
			if(timeUserMap.containsKey(dtime)){
				tmpSet = timeUserMap.get(dtime);
			}else{
				tmpSet = new HashSet<String>();
			}
			tmpSet.add(doc.get("userid"));
			timeUserMap.put(dtime, tmpSet);
		}
	}
	
	public void generateGexf(String gexfPath){
		
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
