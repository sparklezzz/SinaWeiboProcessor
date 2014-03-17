package sinaweibo.app;

import java.util.HashSet;

public class GlobalName {

	//public static final String PROP_TAG = "2";
	
	public static final String WEIBO_CONTENT_TAG = "-1";
	
	public static final String WEIBO_LABELED_VECTOR_DIR_PREFIX = "weibo_labeled_vector_";
	
	public static final String PROFILE_CHUNK_FILE_PREFIX = "profile_chunk_";

	public static final String UID_CHUNK_FILE_PREFIX = "uid_chunk_";

	public static final String UID_DIR = "UID_DIR";

	public static final String CURR_CHUNK_PROFILE_FILE_PATH1 = "CURR_CHUNK_PROFILE_FILE_PATH1";

	public static final String CURR_CHUNK_PROFILE_FILE_PATH2 = "CURR_CHUNK_PROFILE_FILE_PATH2";

	public static HashSet<String> PROFILE_PROPERTY_SET = new HashSet<String>();

	static {
		PROFILE_PROPERTY_SET.add("norm_公司");
		PROFILE_PROPERTY_SET.add("norm_大学");
		PROFILE_PROPERTY_SET.add("norm_年龄段");
		PROFILE_PROPERTY_SET.add("norm_性别");
		PROFILE_PROPERTY_SET.add("norm_感情状况");
		PROFILE_PROPERTY_SET.add("norm_所在地省级");
		PROFILE_PROPERTY_SET.add("norm_标签");
	}
}
