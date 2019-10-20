package com.run.datainput.flume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ExcelJsonZJJ {
	
	private static final String SELECT = "select";
	private static final String FROM = "from";

	static class ConfigSet {
		List<Config> list = new ArrayList<Config>();
		Map<String, Map<String,Set<Config>>> tablesAndFields = new HashMap<String,Map<String,Set<Config>>>();
		
		public ConfigSet addConfig(Config config) {
			String dataSetNameSpaceName = config.getDataSetNameSpaceName();
			String dataSetName = config.getDataSetName();
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
			tablesAndFields.put(fullTableName, fields);
			
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
		private String jsonFieldName;
		private String jsonParentName;
		@Override
		public String toString() {
			return "Config [dataSetNameSpaceName=" + dataSetNameSpaceName + ", dataSetName=" + dataSetName
					+ ", dataCodeName=" + dataCodeName + ", jsonFieldName=" + jsonFieldName + ", jsonParentName="
					+ jsonParentName + "]";
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
}


