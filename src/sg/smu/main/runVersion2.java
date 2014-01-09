package sg.smu.main;

import java.io.File;
import java.util.TimeZone;

import sg.smu.index.buildGraphVersion2;
import sg.smu.index.buildIndex;

public class runVersion2 {

	public static void main(String[] args) throws Exception{
		
		TimeZone.setDefault(TimeZone.getTimeZone("UTC+08:00"));
		
		if(args.length != 6){
			System.err.println("Usage: java runVersion2 keyword startTime(Format:\"2012-02-03 12:12:12\") endTime analyzer(c/s) gexfPath curvePath");
			System.exit(-1);
		}
		
		String keyword = args[0];
		String startTime = args[1];
		String endTime = args[2];
		String analyzerKind = args[3];
		String gexfPath = args[4];
		String curvePath = args[5];
		
		//build Index
		buildIndex.buildTweetIndex(startTime, endTime,analyzerKind);
		// build Graph
		buildGraphVersion2 bGraphV2 = new buildGraphVersion2(endTime);
		bGraphV2.search(keyword,"s");
		bGraphV2.build(curvePath);
		bGraphV2.generateGexf(gexfPath);
		File file = new File("tweetIndex");
		file.delete();
	}
}
