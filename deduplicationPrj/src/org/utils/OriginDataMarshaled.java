package org.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class OriginDataMarshaled {
	private static final JSONParser jsonParser = new JSONParser();
	private static final int CACHE_SIZE = 1000;
	
	public static String [] OriginFields = {
		//{
		"appChannel"			//:""
		,"appKey"				//:"pris"
		,"appVersion"			//:"1.2.2"
		,"city"					//:"37"
		,"dataType"				//:"e"
		,"deviceCarrier"		//:""
		,"deviceHashMac"		//:"0d659d68cc46a7430711fa80675c3e5c"
		,"deviceIMEI"			//:""
		,"deviceMacAddr"		//:"0c:37:dc:6d:4e:91"
		,"deviceModel"			//:"MediaPad 10 FHD"
		,"deviceNetwork"		//:""
		,"deviceOs"				//:"androidpad"
		,"deviceOsVersion"		//:"4.1.2"
		,"deviceResolution"		//:"1200/1920"
		,"deviceUdid"			//:"0c:37:dc:6d:4e:91"
		,"ip"					//:"58.59.9.20"
		,"isp"					//:"电信"
		,"logCity"				//:"济南市"
		,"logProvince"			//:"山东"
		,"occurTime"			//:"1372656265000"
		,"persistedTime"		//:"1352764553000"
		,"sessionUuid"			//:"0c:37:dc:6d:4e:91_anonymous_1372656258000"
		,"userName"				//:"anonymous"
		,"eventId"				//:"server_login"
		,"costTime"				//:"0"
		,"logSource"			//:"pris_app"
		,"sessionStep"			//:"9"
		,"attributes"			//:{"firstlogintime":"1352764553000","firstOccurTime":"1352764553000"}
		//}
	};
	
	public static String [] NewFields = {
		 "appChannel"			//:""				
		,"appKey"				//:"pris"
		,"logSource"			//:"pris_app"
		,"appVersion"			//:"1.2.2"
		,"dataType"				//:"e"
		// -----  4
		
		,"city"					//:"37"
		,"ip"					//:"58.59.9.20"
		,"isp"					//:"电信"
		,"logCity"				//:"济南市"
		,"logProvince"			//:"山东"	
		// -----  9
		
		,"userName"				//:"anonymous"
		,"deviceCarrier"		//:""
		,"deviceHashMac"		//:"0d659d68cc46a7430711fa80675c3e5c"
		,"deviceIMEI"			//:""
		,"deviceMacAddr"		//:"0c:37:dc:6d:4e:91"
		,"deviceModel"			//:"MediaPad 10 FHD"
		,"deviceNetwork"		//:""
		,"deviceOs"				//:"androidpad"
		,"deviceOsVersion"		//:"4.1.2"
		,"deviceResolution"		//:"1200/1920"
		,"deviceUdid"			//:"0c:37:dc:6d:4e:91"
		// -----  20
		
		,"eventId"				//:"server_login"
		,"persistedTime"		//:"1352764553000"
		// ----- 22
		
		,"occurTime"			//:"1372656265000"		
		//,"sessionUuid"			//:"0c:37:dc:6d:4e:91_anonymous_1372656258000"			
		,"costTime"				//:"0"		
		,"sessionStep"			//:"9"
		//,"attributes"			//:{"firstlogintime":"1352764553000","firstOccurTime":"1352764553000"}
	};
	
	public static void main(String[] args) {
		String originFile = "/home/ym/ytmp/data/dedup/0701.part";
		String savedFile = "/home/ym/ytmp/data/0701.1234";
		
		marshal(originFile, savedFile);
	}
	
	public static void marshal (String originFile, String savedFile) {		
		List<String> results = new ArrayList<String>(CACHE_SIZE);
		
		BufferedReader reader = null;		
		try {
			reader = new BufferedReader(new FileReader(originFile));
			String line = null;
			int i = 0;
			while((line = reader.readLine()) != null){			
				JSONObject json = (JSONObject)jsonParser.parse(line);
				results.add(jsonToString(json));
				if (++i > CACHE_SIZE - 1){					
					writeFile(results, savedFile);
					i = 0;
					results.clear();
				}
			}
			writeFile(results, savedFile);
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
	
	private static String jsonToString(JSONObject json){
		StringBuilder sb = new StringBuilder();
		
		String value = null;
		for (int i = 0; i < NewFields.length; i++){
			value = json.get(NewFields[i]).toString();
			if (value == null)
				value = "";
			sb.append(value);
			if (i == 4 || i == 9 || i == 20 || i == 22)
				sb.append("#");
			else 
				sb.append(",");				
		}
		//System.out.println("line " + sb.toString());
		return sb.toString();
	}
	
	private static void writeFile(List<String> list, String file){
		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(new FileWriter(file, true));
			for (int i = 0; i < list.size(); i++){
				printWriter.println(list.get(i));
			}
			printWriter.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (printWriter != null)
				printWriter.close();
		}		
	}
}
