package sg.smu.main;

import java.io.File;
import java.util.TimeZone;

import sg.smu.index.buildGraphVersion5;
import sg.smu.index.buildIndex;

public class runVersion5 {

	public static void main(String[] args) throws Exception{
		
		TimeZone.setDefault(TimeZone.getTimeZone("UTC+08:00"));
		
		if(args.length != 8){
			System.err.println("Usage: runVersion4 keyword startTime(\"2012-02-03 12:12:12\") endTime analyzer(c/s) gexfPath curvePath indpendPath incrPath");
			System.exit(-1);
		}
		
		String keyword = args[0];
		String startTime = args[1];
		String endTime = args[2];
		String analyzerKind = args[3];
		String gexfPath = args[4];
		String curvePath = args[5];
		String indpendPath = args[6];
		String incrPath = args[7];
		
		//build Index
		buildIndex.buildTweetIndex(startTime, endTime,analyzerKind);
		// build Graph
		buildGraphVersion5 bGraphV5 = new buildGraphVersion5(endTime);
		bGraphV5.search(keyword,"s");
		bGraphV5.build(curvePath, incrPath, indpendPath);
		bGraphV5.generateGexf(gexfPath);
		File file = new File("tweetIndex");
		file.delete();
	}
}
