package master_shavadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class functions_tool {

	public static void deleteOldFiles(String directory) {
		File file = new File(directory);
		String[] myFiles;
		if (file.isDirectory()) {
			myFiles = file.list();
			for (int i = 0; i < myFiles.length; i++) {
				File myFile = new File(file, myFiles[i]);
				if (!myFiles[i].equals("slave_calcul.jar") && !myFiles[i].equals("motsIgnores.txt"))
					myFile.delete();
			}
		}
	}

	public static void simultation_function(HashMap<String, Double> ips_list_ok, String login, String path_cible, String file_cible,
			String params) throws IOException, InterruptedException {
		ArrayList<exec_parallele> threads = new ArrayList<exec_parallele>();
		ArrayList<String> key_return_tmp = new ArrayList<String>();

		System.out.println("Test d'execution " + params);

		// Select the worker with the best response time
		String ip_selected = ips_list_ok.keySet().toArray(new String[0])[0];

		// Compute on one distant worker
		functions_tool.compute_slave_distant(ip_selected, login, path_cible,file_cible, params);
		System.out.println("Fin d'execution " + params);

		// functions_tool.launch_parallel_exec(ips_list_ok, login, file_cible,
		// params); // => Probleme de parallelisation

		// Execution parallelisée de la simulation de calcul
		params = " " + "simul";
		for (String ip_ok_from_list : ips_list_ok.keySet()) {
			exec_parallele thread_ip = new exec_parallele(ip_ok_from_list, login, path_cible, file_cible, params);
			thread_ip.start();
			threads.add(thread_ip);
		}

		// Attente de la fin d'execution de tous les threads
		for (exec_parallele thread_ip_launched : threads) {
			thread_ip_launched.join();
			for (String log_return : thread_ip_launched.getLogs_return()) {
				key_return_tmp.add(log_return);
			}
		}
		System.out.println("Fin d'execution :" + params);
	}

	// Sx Splitting Function
	public static HashMap<String, String> splitting_function(HashMap<String, Double> ips_list_ok, String login,
			String input_file, String path_cible) {
		// Définition des dictionnaires de clés
		HashMap<String, String> master_split_Sx = new HashMap<String, String>();
		ArrayList<String> list_to_split = new ArrayList<String>();
		int index_select_ip = 0;
		// chargement du fichier Input.txt
		list_to_split = functions_tool.read_file(input_file);

		// création des différents fichiers Sx
		for (String line_to_sx : list_to_split) {
			if (!master_split_Sx.containsKey(line_to_sx) && !line_to_sx.isEmpty()) {
				String file_key = "S" + list_to_split.indexOf(line_to_sx);
				new File(file_key).delete();
				line_to_sx = functions_tool.word_cleaner_regex(line_to_sx);
				functions_tool.write_file(file_key, line_to_sx);
				String ip_word_selected = ips_list_ok.keySet().toArray(new String[index_select_ip])[index_select_ip];
				functions_tool.copy_file_to(ip_word_selected, login, file_key, path_cible, file_key);
				master_split_Sx.put(file_key, ip_word_selected);
				if (index_select_ip < ips_list_ok.keySet().size() - 1){
					index_select_ip += 1;
				} else {
					index_select_ip = 0;
				}
			}
		}
		return master_split_Sx;
	}

	// Send .jar file to distant worker
	public static void copy_file_to(String ip, String login, String rFile, String lpath, String lFile) {
		try {
			Runtime.getRuntime().exec("scp  " + rFile + " " + login + "@" + ip + ":" + lpath + lFile);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// Save available worker's IP to a log file
	public static void write_file(String file_name, String text) {
		try {
			FileWriter file_writer = new FileWriter(file_name, true);
			BufferedWriter output = new BufferedWriter(file_writer);
			output.write(text + "\n");
			output.flush();
			output.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();

		}
	}

	// Read file
	public static ArrayList<String> read_file(String file) {
		ArrayList<String> ips_list = new ArrayList<String>();
		try {
			// read text file
			InputStream ips = new FileInputStream(file);
			InputStreamReader ipsr = new InputStreamReader(ips);
			BufferedReader buffered_reader = new BufferedReader(ipsr);
			String line = null;
			while ((line = buffered_reader.readLine()) != null) {
				if(!line.isEmpty()){
					ips_list.add(line);
					
				}
			}
			buffered_reader.close();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
		return ips_list;
	}

	
	public static ArrayList<String> compute_slave_distant(String ip, String login, String path_cible, String file_cible, String param)
			throws IOException {
		ArrayList<String> logs_return_tmp = new ArrayList<String>();
		String file_source = System.getProperty("user.dir") + "/" + file_cible;

		long time_begining = java.lang.System.currentTimeMillis();

		// Envoi du fchier JAR vers la machine distante
		copy_file_to(ip, login, file_source, path_cible, file_cible);

		// Execution du fichier JAR distant
		ProcessBuilder process = new ProcessBuilder("ssh", login + "@" + ip,
				" cd " + path_cible + ";java -jar " + file_cible + " " + param);
		// System.out.println("cmd :"+process.command());
		Process process_1 = process.start();
		InputStream input_stream_1 = process_1.getInputStream();
		BufferedReader buffered_reader = null;
		buffered_reader = new BufferedReader(new InputStreamReader(input_stream_1));
		String line = null;
		while ((line = buffered_reader.readLine()) != null) {
			System.out.println(line);
			// master_compute.key_return_tmp.add(line);
			logs_return_tmp.add(line);
		}
		long time_end = java.lang.System.currentTimeMillis();
		System.out.println("Slave computations finished on " + ip + " Temps d'execution : ["
				+ (time_end - time_begining) / 1000.0 + "s]");
		return logs_return_tmp;
	}

	
	public static HashMap<String, Integer> words_count(ArrayList<String> list_sentence) {
		HashMap<String, Integer> count = new HashMap<>();
		for (String sentence : list_sentence) {
			String word = sentence.split(" ")[0];
			if (!count.containsKey(word)) {
				count.put(word, 1);
			} else {
				count.put(word, count.get(word) + 1);
			}
		}
		return count;
	}

	
	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}

	
	// Sort HashMap by values
	public static LinkedHashMap<String, Double> sort_hashmap_by_values(HashMap<String, Double> passedMap) {
		List<String> mapKeys = new ArrayList<>(passedMap.keySet());
		List<Double> mapValues = new ArrayList<>(passedMap.values());
		Collections.sort(mapValues);
		Collections.sort(mapKeys);

		LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();

		Iterator<Double> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			Double val = valueIt.next();
			Iterator<String> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				String key = keyIt.next();
				Double comp1 = passedMap.get(key);
				Double comp2 = val;

				if (comp1.equals(comp2)) {
					keyIt.remove();
					sortedMap.put(key, val);
					break;
				}
			}
		}
		return sortedMap;
	}

	
	public static String word_cleaner_regex(String dirty_words){
		String regex_pattern = "[aA-zZ0-9'éèàùç]+";
		String words_cleaned = "";
		Pattern p = Pattern.compile(regex_pattern);
		Matcher m = p.matcher(dirty_words);
		while (m.find()) {
			words_cleaned += m.group() + " ";
		}
	return words_cleaned;
	}
	
	
}
