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
import java.util.HashMap;

public class functions_tool {

// 		1- Sx Splitting Function
public static HashMap <String,String> splitting_function(ArrayList <String> ips_list_ok, String input_file, String login, String path_cible){
	System.out.println("\nSplitting Function Launched");
	
	// Définition des dictionnaires de clés
	HashMap <String,String> master_split_Sx = new HashMap <String, String>();
	ArrayList <String> list_master_split = new ArrayList <String>();
	HashMap <String,String> master_cle_UMx_tmp = new HashMap <String, String>();
	
	// chargement du fichier Input.txt
	list_master_split = functions_tool.read_file(input_file);
	
	// création des différents fichiers Sx et du dictionnaire contenant leurs chemins
	for(String el : list_master_split){
		if(!master_split_Sx.containsKey(el)){
			String file_key = "S" + list_master_split.indexOf(el);
			String file_key_path = System.getProperty("user.dir") + "/" + file_key +".txt";
			new File(file_key_path).delete();
			functions_tool.write_file(file_key_path, el);
			master_split_Sx.put(file_key, file_key_path);
			
			// 2 - Création Dictionnaire UMx - machines
			int ip_cpt = 0;
			//if(list_master_split.size() <= ips_list_ok.size()){
				for(String ip_ok_from_list : ips_list_ok){
					if(ip_cpt == list_master_split.indexOf(el)){
						master_cle_UMx_tmp.put(ip_ok_from_list.split(" ")[1], file_key);
						// Envoi du fchier Sx vers la machine distante
						functions_tool.copy_file_to(ip_ok_from_list.split(" ")[1], login, file_key_path, path_cible + file_key);
					}
					ip_cpt += 1;
			//	}		
			}
		}
	}
	// System.out.println(master_split_Sx);
	// System.out.println(master_cle_UMx_tmp);
	return master_cle_UMx_tmp;
}


// Envoi du Fichier JAR par SCP vers la machine distante
	public static void copy_file_to(String ip, String login, String rFile, String lFile){
		try {
				Process cpyFileLocal = Runtime.getRuntime().exec("scp  " + rFile + " " + login + "@" + ip + ":" + lFile);
//				InputStream stderr = cpyFileLocal.getErrorStream();
//				InputStreamReader isr = new InputStreamReader(stderr);
//				BufferedReader br = new BufferedReader(isr);
//				String line = null;
//				int exitVal = cpyFileLocal.waitFor();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
	}


	//Stockage des IP dans le fichier de logs
	public static void write_file(String file_name, String text){
		//String file_path = "/Users/Wes/Documents/workspace/TPTelecom/MASTER_SHAVADOOP_JAR/" + file_name;
	
		try{
			FileWriter file_writer = new FileWriter(file_name, true);
			BufferedWriter output = new BufferedWriter(file_writer);
			output.write(text+"\n");
			output.flush();
			output.close();
			
		}
		catch(IOException ioe){
			ioe.printStackTrace();
			
		}
	}


	// lecture des IP stockées dans le fichier de logs
	public static ArrayList <String> read_file(String file){
		ArrayList <String> ips_list = new ArrayList <String>();
		try{
				// read text file
				InputStream ips = new FileInputStream(file);
				InputStreamReader ipsr = new InputStreamReader(ips);
				BufferedReader buffered_reader = new BufferedReader(ipsr);
				String line;
				while((line = buffered_reader.readLine())!=null){
					ips_list.add(line);
				}
				buffered_reader.close();
		}
		catch(Exception e){
			System.out.println(e.toString());
		}
		return ips_list;
	}


	public static String connection_distant(String login, String ip) throws InterruptedException {
		ProcessBuilder proc =  new ProcessBuilder("ssh", login + "@"+ ip, " BatchMode yes; echo $((2+3))");
		Process proc_1;
		try {
			proc_1 = proc.start();
			proc_1.waitFor();
			if(proc_1.exitValue() == 0)
				return "Connexion " + ip + " : OK";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	return null;
	}


	//Vérification des machines alive via leur IP
	public static String check_save_goods_ip(String login, String ip_source_list) throws InterruptedException, IOException{
		ArrayList <String> ips_list_all = new ArrayList <String>();
		ips_list_all = read_file(ip_source_list);
		String file_path = System.getProperty("user.dir") + "/" + "logs_connexions.txt";
		new File(file_path).delete();
		new File(file_path).createNewFile();
		for(String ip_from_list : ips_list_all){
			String ip_tmp = connection_distant(login, ip_from_list);
			System.out.println(ip_tmp);	
			if(ip_tmp != null)
				write_file(file_path, ip_tmp);
		}
		return file_path;
	}


	public static ArrayList<String> compute_slave_distant(String ip, String login, String file_cible, String param) throws IOException{
		ArrayList<String> logs_return_tmp = new ArrayList<String>();
		String file_source = System.getProperty("user.dir") + "/" + "slave_calcul.jar";
		// Chemin du JAR : "Users/Wes/Documents/workspace/TPTelecom/master_shavadoop/slave_calcul.jar"
		
		long time_begining = java.lang.System.currentTimeMillis();
		
		// Envoi du fchier JAR vers la machine distante
		copy_file_to(ip, login, file_source, file_cible);
		
		// Execution du fichier JAR distant
		ProcessBuilder process =  new ProcessBuilder("ssh", login + "@" + ip, " cd ~/workspace/TPTelecom/INF727/;java -jar slave_calcul.jar" + param);
		//System.out.println("cmd :"+process.command());
		Process process_1 = process.start();
		InputStream input_stream_1 = process_1.getInputStream(); 
		BufferedReader buffered_reader = null;
		buffered_reader =new BufferedReader(new InputStreamReader(input_stream_1)); 
		String line = null;
		while((line = buffered_reader.readLine()) != null){
			System.out.println(line);
			//master_compute.key_return_tmp.add(line);
			logs_return_tmp.add(line);
		}
		System.out.println("Slave computations finished on "+ip);
		long time_end = java.lang.System.currentTimeMillis();
		System.out.println("Temps d'execution du calcul : " + (time_end - time_begining)/1000.0);
		return logs_return_tmp;
	}
	
	
	//	Fonction Execution parallelisée /!\ => Probleme de parallelisation
	public static ArrayList<String> launch_parallel_exec(ArrayList <String> ips_list_ok, String login, String file_cible, String params) throws InterruptedException{
		ArrayList <exec_parallele> threads = new ArrayList <exec_parallele>();
		ArrayList<String> key_return_tmp = new ArrayList<String>();
		
		for(String ip_ok_from_list : ips_list_ok){
			exec_parallele thread_ip =  new exec_parallele(ip_ok_from_list.split(" ")[1], login, file_cible, params);
			thread_ip.start();
			threads.add(thread_ip);
		}
		
		//Attente de la fin d'execution de tous les threads
		for(exec_parallele thread_ip_launched : threads){
			thread_ip_launched.join();
			for(String log_return : thread_ip_launched.getLogs_return_tmp()){
				key_return_tmp.add(log_return);
			}
		}
		System.out.println("Fin d'execution :" + params);
		return key_return_tmp;
	}


	public static HashMap<String, Integer> nOccurrences(ArrayList<String> list_sentence) {
		HashMap<String, Integer> count = new HashMap<>();
		for (String sentence : list_sentence) {
			for (String word : sentence.split(" ")) {
				if (!count.containsKey(word)) {
					count.put(word, 0);
				}
				count.put(word, count.get(word) + 1);
			}
		}
		return count;
	}


	public static String keyMaxValue(HashMap<String, Integer> count) {
		String maxKey = "";
		int maxValue = 0;
	
		for (String key : count.keySet()) {
			if (count.get(key) > maxValue) {
				maxValue = count.get(key);
				maxKey = key;
			}
		}
		return maxKey;
	}

}
