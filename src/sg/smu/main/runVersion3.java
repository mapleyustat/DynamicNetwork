package sg.smu.main;

import java.io.File;
import java.util.TimeZone;

import sg.smu.index.buildGraphVersion3;
import sg.smu.index.buildIndex;

public class runVersion3 {

	public static void main(String[] args) throws Exception{
		
		TimeZone.setDefault(TimeZone.getTimeZone("UTC+08:00"));
		
		if(args.length != 7){
			System.err.println("Usage: java runVersion2 keyword startTime(Format:\"2012-02-03 12:12:12\") endTime analyzer(c/s) curvePath zeroPath incrPath");
			System.exit(-1);
		}
		
		String keyword = args[0];
		String startTime = args[1];
		String endTime = args[2];
		String analyzerKind = args[3];
		String curvePath = args[4];
		String indpendPath = args[5];
		String incrPath = args[6];
		
		//build Index
		buildIndex.buildTweetIndex(startTime, endTime,analyzerKind);
		// build Graph
		buildGraphVersion3 bGraphV3 = new buildGraphVersion3();
		bGraphV3.search(keyword,"s");
		bGraphV3.build(curvePath,indpendPath,incrPath);
		
		File file = new File("tweetIndex");
		file.delete();
	}
}
