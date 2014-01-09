package sg.smu.util;

import java.util.TreeMap;

public class userNode {

	private TreeMap<Long,Integer> dnRelMap;
	private int maxRel;
	
	public userNode(){
		this.dnRelMap = new TreeMap<Long,Integer>();
		this.maxRel = 0;
	}
	
	public void putInstance(long time){	
		this.maxRel += 1;
		dnRelMap.put(time,this.maxRel);
	}
	
	public TreeMap<Long,Integer> getDnRelMap(){
		return this.dnRelMap;
	}
}
