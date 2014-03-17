package sinaweibo;


import org.apache.hadoop.util.ProgramDriver;

import sinaweibo.preprocessor.BaiduKeywordTitleSplitter;


/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class ProgramLoader {

  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("KeywordTitleSplitter", BaiduKeywordTitleSplitter.class,
                   "A map/reduce program that split file into several reducers");

      pgd.driver(argv);
      // Success
      exitCode = 0;
    }
    catch(Throwable e){
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}

