package sinaweibo.vectorizer;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.wltea.analyzer.lucene.IKAnalyzer;

// 生成mahout支持的中文分词器，参考org/apache/mahout/vectorizer/DefaultAnalyzer.java
public class MahoutChineseFinestAnalyzer extends Analyzer{

	private final IKAnalyzer m_analyzer = new IKAnalyzer(false);	//最细粒度
	
	  @Override
	  public TokenStream tokenStream(String fieldName, Reader reader) {
	    return m_analyzer.tokenStream(fieldName, reader);
	  }

	  @Override
	  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
	    return m_analyzer.reusableTokenStream(fieldName, reader);
	  }
	
}


