package sinaweibo.common.lucene;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.util.Iterator;

/** Used to emit tokens from an input string array in the style of TokenStream */
public final class IteratorTokenStream extends TokenStream {
  private final CharTermAttribute termAtt;
  private final Iterator<String> iterator;

  public IteratorTokenStream(Iterator<String> iterator) {
    this.iterator = iterator;
    this.termAtt = addAttribute(CharTermAttribute.class);
  }

  @Override
  public boolean incrementToken() {
    if (iterator.hasNext()) {
      clearAttributes();
      termAtt.append(iterator.next());
      return true;
    } else {
      return false;
    }
  }
}
