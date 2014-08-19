package org.model1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class SampleData {

	public static void main(String[] args) {
		String sourceFile = "/home/ym/ytmp/data/1/0701.part";
		String savedFile = "/home/ym/ytmp/data/testdata";
		String[] targetFileds = new String [] {
			"deviceOsVersion"
			, "deviceResolution"
			, "ip"
			, "logCity"
			, "persistedTime"
			, "occurTime"
			, "sessionUuid" 
			, "eventId"
			, "sessionStep"				
			};
		
		abstractData(sourceFile, savedFile, targetFileds);
	}
	
	public static void abstractData(String sourceFile, String savedFile, String[] targetFileds){
		BufferedReader reader = null;
		PrintStream ps = null;
		
		try {
			reader = new BufferedReader(new FileReader(sourceFile));
			ps = new PrintStream(savedFile);
			
			String line = null;
			while ((line = reader.readLine()) != null){
				String [] fileds = line.split(",");
				Map<String, String> map = switchToMap(fileds);
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < targetFileds.length; i++){
					String value = map.get("\"" + targetFileds[i].trim() + "\"");
					if (value == null)
						value = "";
					sb.append("," + value);
				}
				ps.println(sb.toString().substring(1));
			}
			ps.flush();
			reader.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (ps != null)
				ps.close();
		}
	}
	
	public static Map<String, String> switchToMap(String[] fileds){
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < 27; i++){
			//System.out.println(fileds[i]);
			String key = fileds[i].split(":")[0];
			String value = fileds[i].split(":")[1];
			map.put(key, value);
		}
		
		return map;
	}
}
