package sg.cmu.analyzer;

import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class FileIO {
	
	private static String reader2String(Reader reader) throws IOException {
        BufferedReader br = new BufferedReader(reader);
        String str = null;
        StringBuffer sb = new StringBuffer("");
       while ((str = br.readLine()) != null) {
    	   sb.append(str);
        }
       return sb.toString();
    }
	
	public static String readerToString(Reader reader) throws IOException {

		String str = null;
		str = FileIO.reader2String(reader);
		List<String> list = Twokenize.tokenizeRawTweetText(str);
		StringBuffer sb = new StringBuffer("");
		for(String s : list){
			sb.append(s).append(" ");
		}
		
		return sb.toString().trim();
	}
}
