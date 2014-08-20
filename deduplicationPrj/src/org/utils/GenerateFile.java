package org.utils;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateFile {

	private static String[] chars = new String[26];
	
	static {
		for (int i = 0; i < 26; i++)
			chars[i] = (char)('a' + i) + "";
	}
	
	public static void main(String[] args) {
		List<String> list = dimension(10, 4);
		//System.out.println(list);
		String output = "/home/ym/ytmp/output/n1";
		writeFile(list, output);
	}
	
	public static void writeFile(List<String> list, String file){
		PrintStream ps = null;
		try {
			ps = new PrintStream(file);
			for (int i = 0; i < list.size(); i++)
				ps.println(list.get(i));
			ps.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (ps != null)
				ps.close();
		}
	}
	
	public static List<String>  dimension(int counts, int dimensions){
		List<String> list = new ArrayList<String>();
		
		Random rand = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < counts; i++){
			for (int j = 0; j < dimensions; j++){
				sb.append("," + chars[rand.nextInt(5)]);
			}
			//System.out.println(sb.toString().substring(1));
			list.add(sb.toString().substring(1));
			sb.delete(0, sb.length());
		}
		
		return list;
	}
}
