package com.run.datainput.flume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class ExcelJsonZJJ {
	
	private static final String SELECT = "select";
	private static final String FROM = "from";
	
	private static final String JSON_DATA_NAME = "data";
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		ConfigSet cs = new ConfigSet();
		
		//模拟从Excel解析出来的数据
		cs.addConfig(new Config("OBJ003","WA_OBJECT_Z002_9996","B030004","zjlx","data",1));
		cs.addConfig(new Config("OBJ003","WA_OBJECT_Z002_9996","B030005","zjhm","data",1));
		
		cs.addConfig(new Config("","","","ltgj","data",2));
		cs.addConfig(new Config("OBJ003","WA_OBJECT_Z002_9996","B020005","hm","ltgj",1));
		
		cs.addConfig(new Config("","","","jtgj","data",2));
		cs.addConfig(new Config("OBJ003","WA_OBJECT_Z002_9990","C030001","lb","jtgj",1));
		cs.addConfig(new Config("OBJ003","WA_OBJECT_Z002_9990","C030002","pzhm","jtgj",1));
		
		//构造查询中间件的SQL
		Map<String, String> buildAllTablesSqlString = cs.buildAllTablesSqlString();
		
		for(Entry<String, String> entry : buildAllTablesSqlString.entrySet()) {
			System.out.println("table ["+entry.getKey()+"], sql ["+entry.getValue()+"]");//执行中间件查询SQL
			
			//准备对中间件返回的数据进行处理
			Map<String, Set<Config>> fields = cs.tablesAndFields.get(entry.getKey());//处理当前表（数据集）的每个返回字段内容
			
			//处理中间件返回的一条结果，将中间件返回的数据拼接到JSON上
			for(Entry<String, Set<Config>> fieldsEntry : fields.entrySet()) {
				fieldsEntry.getKey();//以此作为条件从中间件的返回数据里获取相应数据
				for(Config config : fieldsEntry.getValue()) {
					if(config.jsonParentName.contentEquals(JSON_DATA_NAME)) {
						cs.jsonDataMap.put(config.jsonFieldName, config.dataCodeName+"-value");//填中间件获取的数据
					} else {
						List<TreeMap<String, String>> subSubListMap = (List<TreeMap<String, String>>)cs.jsonDataMap.get(config.jsonParentName);
						
						if(subSubListMap.size()==1 && subSubListMap.get(0).get(config.jsonFieldName)==null) {
							subSubListMap.get(0).put(config.jsonFieldName, config.dataCodeName+"-value");
						} else {
							TreeMap<String, String> subJsonMap = new TreeMap<String, String>();
							subJsonMap.put(config.jsonFieldName, config.dataCodeName+"-value");//填中间件获取的数据
							subSubListMap.add(subJsonMap);
						}

					}
				}
			}
		}
		
		List<Message> messageList = new ArrayList<Message>();
		messageList.add(new Message("", "", cs.jsonDataMap));
		String buildJsonMessage = buildJsonMessage(messageList);
		
		System.out.println(buildJsonMessage);
	}
	
	public static String buildJsonMessage(List<Message> messageList) {
		Gson gson = new GsonBuilder().create();

		return gson.toJson(messageList, new TypeToken<List<Message>>() {}.getType());
	}
	
	static class ConfigSet {
		List<Config> list = new ArrayList<Config>();
		Map<String, Map<String,Set<Config>>> tablesAndFields = new HashMap<String,Map<String,Set<Config>>>();
		 NavigableMap<String, Object> jsonDataMap = new TreeMap<String, Object>();
		
		@SuppressWarnings("unchecked")
		public ConfigSet addConfig(Config config) {
			
			String dataSetNameSpaceName = config.getDataSetNameSpaceName();
			String dataSetName = config.getDataSetName();
			
			//构造查询中间件的数据结构
			if(null!=dataSetNameSpaceName && null!=dataSetName && !dataSetName.isEmpty() && !dataSetNameSpaceName.isEmpty()) {
				String fullTableName = dataSetNameSpaceName+"."+dataSetName;
				Map<String,Set<Config>> fields = tablesAndFields.get(fullTableName);
				if(null==fields) {
					fields = new HashMap<String, Set<Config>>();
				}
				Set<Config> oneFieldConfigs = fields.get(config.dataCodeName);//一个 数据元素可能映射给多个json属性
				if(null==oneFieldConfigs) {
					oneFieldConfigs = new HashSet<Config>();
				}
				oneFieldConfigs.add(config);
				fields.put(config.dataCodeName, oneFieldConfigs);
				tablesAndFields.put(fullTableName, fields);
			}
			
			//构造 拼接 JSON 的数据结构
			if(null!=config.jsonParentName && !config.jsonParentName.isEmpty()) {//必须有父节点
				
				if(config.jsonParentName.contentEquals(JSON_DATA_NAME)) {//如果parent是ROOT，parent是ROOT的有两种
					if(config.levelType==1)
						jsonDataMap.put(config.jsonFieldName, "");
					else {
						List<TreeMap<String, String>> list = new ArrayList<TreeMap<String, String>>();
						jsonDataMap.put(config.jsonFieldName, list);
					}
				} else {//如果parent不是ROOT，需要在 rootSubMap中找
					List<TreeMap<String, String>> subSubListMap = (List<TreeMap<String, String>>)jsonDataMap.get(config.jsonParentName);
					if(subSubListMap.isEmpty()) {
						TreeMap<String, String> subSubMap = new TreeMap<String, String>();
						subSubMap.put(config.jsonFieldName, null);
						subSubListMap.add(subSubMap);
					} else {
						TreeMap<String, String> subSubMap = subSubListMap.get(0);
						subSubMap.put(config.jsonFieldName, null);
					}
				}
			}
			
			return this;
		}
		
		public Set<String> tablesName() {
			return tablesAndFields.keySet();
		}
		
		public Map<String, String> buildAllTablesSqlString() {
			
			Map<String, String> result = new HashMap<String, String>();
			for(Entry<String, Map<String,Set<Config>>> entry : tablesAndFields.entrySet()) {
				result.put(entry.getKey(), buildSqlString(entry.getKey()));
			}
			return result;
		}
		
		public String buildSqlString(String fullTableName) {
			Map<String,Set<Config>> fields = tablesAndFields.get(fullTableName);
			if(null==fields) {
				return null;
			} else {
				StringBuffer sb = new StringBuffer();
				
				sb
				.append(SELECT);
				Iterator<String> iterator = fields.keySet().iterator();
				sb.append(" ").append(iterator.next());
				while(iterator.hasNext()) {
					sb.append(" ").append(",").append(iterator.next());
				}

				sb.append(" ").append(FROM).append(" ").append(fullTableName);
				
				return sb.toString();
			}
		}
		
		public String buildSqlString(String fullTableName, String whereString) {
			return this.buildSqlString(fullTableName).concat(" ").concat(whereString);
		}
		
	}
	
	static class Config {
		
		public String getDataSetNameSpaceName() {
			return dataSetNameSpaceName;
		}
		public void setDataSetNameSpaceName(String dataSetNameSpaceName) {
			this.dataSetNameSpaceName = dataSetNameSpaceName;
		}
		public String getDataSetName() {
			return dataSetName;
		}
		public void setDataSetName(String dataSetName) {
			this.dataSetName = dataSetName;
		}
		public String getDataCodeName() {
			return dataCodeName;
		}
		public void setDataCodeName(String dataCodeName) {
			this.dataCodeName = dataCodeName;
		}
		public String getJsonFieldName() {
			return jsonFieldName;
		}
		public void setJsonFieldName(String jsonFieldName) {
			this.jsonFieldName = jsonFieldName;
		}
		public String getJsonParentName() {
			return jsonParentName;
		}
		public void setJsonParentName(String jsonParentName) {
			this.jsonParentName = jsonParentName;
		}
		private String dataSetNameSpaceName;
		private String dataSetName ;
		private String dataCodeName;
		private String jsonFieldName;//对应Excel“字段名称”
		private String jsonParentName;
		private int levelType;


		public Config(String dataSetNameSpaceName, String dataSetName, String dataCodeName, String jsonFieldName,
				String jsonParentName, int levelType) {
			super();
			this.dataSetNameSpaceName = dataSetNameSpaceName;
			this.dataSetName = dataSetName;
			this.dataCodeName = dataCodeName;
			this.jsonFieldName = jsonFieldName;
			this.jsonParentName = jsonParentName;
			this.levelType = levelType;
		}
		@Override
		public String toString() {
			return "Config [dataSetNameSpaceName=" + dataSetNameSpaceName + ", dataSetName=" + dataSetName
					+ ", dataCodeName=" + dataCodeName + ", jsonFieldName=" + jsonFieldName + ", jsonParentName="
					+ jsonParentName + ", levelType=" + levelType + "]";
		}
		public int getLevelType() {
			return levelType;
		}
		public void setLevelType(int levelType) {
			this.levelType = levelType;
		}
		@Override
		public int hashCode() {
			return this.toString().hashCode();
		}
		@Override
		public boolean equals(Object obj) {
			return this.toString().equals(obj);
		}
	}	
	
	static class Message {
		private String objid;
		private String objtype;
		private NavigableMap<String, Object> data;
		
		public String getObjid() {
			return objid;
		}

		public void setObjid(String objid) {
			this.objid = objid;
		}

		public String getObjtype() {
			return objtype;
		}

		public void setObjtype(String objtype) {
			this.objtype = objtype;
		}

		public NavigableMap<String, Object> getData() {
			return data;
		}

		public void setData(NavigableMap<String, Object> data) {
			this.data = data;
		}

		public Message(String objid, String objtype, NavigableMap<String, Object> data) {
			super();
			this.objid = objid;
			this.objtype = objtype;
			this.data = data.descendingMap();
		}


	}
	
}


