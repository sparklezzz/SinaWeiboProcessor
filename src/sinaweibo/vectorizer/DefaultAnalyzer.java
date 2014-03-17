package sinaweibo.vectorizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Reader;

/**
 *  A subclass of the Lucene StandardAnalyzer that provides a no-argument constructor. 
 *  Used as the default analyzer in many cases where an analyzer is instantiated by
 *  class name by calling a no-arg constructor.
 */
public final class DefaultAnalyzer extends Analyzer {

  private final StandardAnalyzer stdAnalyzer = new StandardAnalyzer(Version.LUCENE_36);

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return stdAnalyzer.tokenStream(fieldName, reader);
  }
  
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return stdAnalyzer.reusableTokenStream(fieldName, reader);
  }
}
