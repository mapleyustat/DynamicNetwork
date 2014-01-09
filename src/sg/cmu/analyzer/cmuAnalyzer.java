package sg.cmu.analyzer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.util.Version;

public class cmuAnalyzer extends Analyzer {

	private Version matchVersion;
	
	public static final Set<?> ENGLISH_STOP_WORDS_SET;
	  
	  static {
	    final List<String> stopWords = Arrays.asList(
	      "a", "an", "and", "are", "as", "at", "be", "but", "by",
	      "for", "if", "in", "into", "is", "it",
	      "no", "not", "of", "on", "or", "such",
	      "that", "the", "their", "then", "there", "these",
	      "they", "this", "to", "was", "will", "with"
	    );
	    final CharArraySet stopSet = new CharArraySet(Version.LUCENE_35, stopWords.size(), false);
	    stopSet.addAll(stopWords);  
	    ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet); 
	  }
	
	
	public cmuAnalyzer(Version matchVersion){
		this.matchVersion = matchVersion;
	}
	
	@Override
	public TokenStream tokenStream(String fieldName, Reader reader) {
		// TODO Auto-generated method stub
		String string = null;
		try {
			string = FileIO.readerToString(reader);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Reader sReader = new StringReader(string);
		
		TokenStream stream = new WhitespaceTokenizer(Version.LUCENE_35, sReader );
		stream = new StandardFilter(Version.LUCENE_35,stream);
		stream = new LowerCaseFilter(Version.LUCENE_35,stream);
		stream = new StopFilter(Version.LUCENE_35, stream, ENGLISH_STOP_WORDS_SET);
		return stream; 
	}
}
