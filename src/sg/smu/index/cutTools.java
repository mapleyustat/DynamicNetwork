package sg.smu.index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

public class cutTools {

	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static void cut(String inPath, String startTime, String endTime, String outPath) throws Exception{
		
		Date sDate = df.parse(startTime);
		Date eDate = df.parse(endTime);
		
		if(!new File(inPath).exists()){
			System.err.println("The " + inPath + " is not exist.");
			System.exit(-1);
		}
		
		Date tmpDate = null;
		BufferedReader br = new BufferedReader(new FileReader(inPath));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outPath));
		String line = null;
		while((line = br.readLine())!=null){
			String[] tokens = line.split("\t");
			tmpDate = df.parse(tokens[0].trim());
			if(tmpDate.getTime() >= sDate.getTime() && tmpDate.getTime() <= eDate.getTime()){
				bw.write(line);
				bw.newLine();
				bw.flush();
			}
		}
		br.close();
		bw.close();
	}
	
	public static void main(String[] args) throws Exception{
		cut("user_curve.txt","2011-10-02 03:20:06","2011-10-02 03:20:41","cut.out");
	}
}
