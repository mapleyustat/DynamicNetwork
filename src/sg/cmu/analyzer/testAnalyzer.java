package sg.cmu.analyzer;

import java.io.IOException;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

public class testAnalyzer {

	public static void main(String[] args) throws IOException {

		cmuAnalyzer cAnalyzer = new cmuAnalyzer(Version.LUCENE_35);
		
		Analyzer a1 = new StandardAnalyzer(Version.LUCENE_35);
		Analyzer a2 = new StopAnalyzer(Version.LUCENE_35);
		Analyzer a3 = new SimpleAnalyzer(Version.LUCENE_35);
		Analyzer a4 = new WhitespaceAnalyzer(Version.LUCENE_35);

		String txt = "@hushnowlove Ricky is mr cap right? Heh";
		AnalyzerUtils.displayTokens(cAnalyzer, txt);
		System.out.println("--------whitespace-------------");
		AnalyzerUtils.displayTokens(a4, txt);
	}

}
