package sinaweibo.preprocessor;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import sinaweibo.common.StringTuple;

public class TestIKAnalyzer {

	// GENERAL_PUNCTUATION 判断中文的“号
	// CJK_SYMBOLS_AND_PUNCTUATION 判断中文的。号
	// HALFWIDTH_AND_FULLWIDTH_FORMS 判断中文的，号
	private static final boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	public static final boolean isChinese(String strName) {
		char[] ch = strName.toCharArray();
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (isChinese(c)) {
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) throws IOException {
		final Analyzer analyzer = new IKAnalyzer(true); // 智能分词

		String str = "// 【新鲜糗事】千万别随便打我擦！手误啊 手误啊！！欲哭无泪啊 http://t.cn/zYLQZ5N @搞笑囧图App";

		TokenStream stream = analyzer.reusableTokenStream(str.toString(),
				new StringReader(str.toString()));
		CharTermAttribute termAtt = stream
				.addAttribute(CharTermAttribute.class);
		StringTuple document = new StringTuple();
		stream.reset();
		while (stream.incrementToken()) {
			if (termAtt.length() > 0) {
				document.add(new String(termAtt.buffer(), 0, termAtt.length()));
			}
			if (isChinese(termAtt.toString())) {
				System.out.println(termAtt.toString());
			}
		}

	}
}
