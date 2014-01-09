package sg.smu.main;

import java.io.File;
import java.io.FileWriter;
import java.util.TimeZone;

import sg.smu.index.buildGraphVersion1;
import sg.smu.index.buildIndex;

public class runVersion1 {

	public static void main(String[] args) throws Exception{
		
		TimeZone.setDefault(TimeZone.getTimeZone("UTC+08:00"));
		
		if(args.length != 5){
			System.err.println("Usage: java runVersion1 keyword startTime(Format:\"2012-02-03 12:12:12\") endTime analyzer(c/s) gexfPath");
			System.exit(-1);
		}
		
		String keyword = args[0];
		String startTime = args[1];
		String endTime = args[2];
		String analyzerKind = args[3];
		String gexfPath = args[4];
		
		//build Index
		buildIndex.buildTweetIndex(startTime, endTime,analyzerKind);
		// build Graph
		buildGraphVersion1 bGraphV1 = new buildGraphVersion1(endTime);
		bGraphV1.search(keyword,"s");
		bGraphV1.build();
		bGraphV1.generateGexf(gexfPath);
		File file = new File("tweetIndex");
		file.delete();
		
	}
}
