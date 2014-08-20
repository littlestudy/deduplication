package org.model2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Record {
	private String key;
	private String values;
	public Record(String key, String values) {
		super();
		this.key = key;
		this.values = values;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValues() {
		return values;
	}
	public void setValues(String values) {
		this.values = values;
	}
	@Override
	public String toString() {
		return key + "," + values;
	}			
}

public class DedupInMemory {
	private static List<Record> records;
	
	public static void main(String[] args) {
		String input = "/home/ym/ytmp/data/testdata";
		String output = "/home/ym/ytmp/data/testdataC";
		initRecords(input);
		
		System.out.println("--------------------------------------------------------");
		for (Record r : records)
			System.out.println(r);
		System.out.println("--------------------------------------------------------");
		dedup();
		for (Record r : records)
			System.out.println(r);
		System.out.println("--------------------------------------------------------");
		dedup();
		for (Record r : records)
			System.out.println(r);
		dedup();
		dedup();
		writeFile(output);
	}
	
	public static void dedup(){		
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < records.size(); i++){			
			String originKey = records.get(i).getKey();	
			String originValue = records.get(i).getValues();
			//System.out.println("key" + key);
			String newKey = originKey.substring(0, originKey.lastIndexOf(","));			
			String newValue = originKey.substring(originKey.lastIndexOf(",") + 1);
			if (!"".equals(originValue))				
				newValue = newValue + "," + originValue;
			newValue = "{" + newValue + "}";
			System.out.println("newKey: " + newKey + "     newValue: " + newValue);
			String valueInMap = map.get(newKey);
			if (valueInMap == null){
				map.put(newKey, newValue);
				continue;
			}				
			valueInMap = valueInMap + "," + newValue;
			map.put(newKey, valueInMap);
		}
		System.out.println(map);
		records.clear();
		for (String k : map.keySet()){
			records.add(new Record(k, "{" + map.get(k) + "}"));
			//System.out.println(k + " = " + "{" + map.get(k).substring(1) + "}");
		}
	}
	
	private static void initRecords(String file){
		List<String> list = readFile(file);
		records = new ArrayList<Record>();
		for (int i = 0; i < list.size(); i++){
			//System.out.println(list.get(i));
			records.add(new Record(list.get(i), ""));
		}
	}
	
	private static List<String> readFile(String file){
		List<String> list = new ArrayList<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = reader.readLine()) != null)
				list.add(line.trim());			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{			
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	private static void writeFile(String file){
		PrintStream ps = null;
		try {
			ps = new PrintStream(file);
			for(Record r : records)
				ps.println(r.toString());
			ps.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally{			
			if (ps != null)
				ps.close();
		}
	}
}
