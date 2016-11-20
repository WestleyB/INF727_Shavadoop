package master_shavadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class master_compute {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		// ================================ Données Inputs ======================================
		String login = "wbirmingham"; // = "pi"; "ghost"; //
		String path_cible = "~/workspace/TPTelecom/INF727/"; // Chemin cible
																// pour l'envoi
																// du fichier
																// JAR
		String file_cible = "slave_calcul.jar";
		String ips_source = "liste_ip.txt";
		String input_file = "forestier_mayotte.txt";	// "Input.txt";		// 
		String final_file = "output_final.txt";
		// =======================================================================================

		String params = "";
		ArrayList<String> ips_list_ok = new ArrayList<String>();
		HashMap<String, Double> list_ip_with_time = new HashMap<String, Double>();
		HashMap<String, Double> temps_execution_functions = new HashMap<String, Double>();
		HashMap<String, String> cles_Sx_machines = new HashMap<String, String>();
		HashMap<String, String> cles_UMx_machines = new HashMap<String, String>();
		HashMap<String, String> cles_SMx_machines = new HashMap<String, String>();
		HashMap<String, Integer> cles_RMx_machines = new HashMap<String, Integer>();
		HashMap<String, ArrayList<String>> cles_UMx_tmp = new HashMap<String, ArrayList<String>>();
		HashMap<String, HashSet<String>> cles_UMx = new HashMap<String, HashSet<String>>();
		//ArrayList<String> ip_not_selected = new ArrayList<String>();
		ArrayList<exec_parallele> threads = new ArrayList<exec_parallele>();
		ArrayList<String> smx_key_return_tmp = new ArrayList<String>();
		ArrayList<String> params_config = new ArrayList<String>();
		ArrayList<String> key_return_tmp = new ArrayList<String>();

		// Vérification des IPs valides
		String file_logs = ssh_manager.check_save_goods_ip(login, ips_source);
		
		// Création de la liste d'IPs valides
		ips_list_ok = functions_tool.read_file(file_logs);
		for(String ip_unformated : ips_list_ok){
			list_ip_with_time.putAll(ssh_manager.extract_ip_from_logs(ip_unformated));
		}
		list_ip_with_time = functions_tool.sort_hashmap_by_values(list_ip_with_time);
	
		
		// Simulation de test
		//functions_tool.simultation_function(list_ip_with_time, login, file_cible, params);

		
		// Sx Splitting Function
		double time_start_1 = java.lang.System.currentTimeMillis();
		
		System.out.println("\n*******************************  Splitting Function Launched  *********************************");
		cles_Sx_machines = functions_tool.splitting_function(list_ip_with_time, login, input_file, path_cible);
		System.out.println("\ncles_Sx_machines : ");
		for(String sx : cles_Sx_machines.keySet()){
			System.out.println("		" + sx + " : " + cles_Sx_machines.get(sx));
		}
		System.out.println("\n*************************************************************************************************");		
		
		double time_end_1 = java.lang.System.currentTimeMillis();
		temps_execution_functions.put("Splitting Function", (time_end_1 - time_start_1) / 1000.0);
		
		
		double time_start_2 = java.lang.System.currentTimeMillis();
		
		// Execution parallelisée de split_mapping du JAR slave
		// extends de la classe Thread à la classe exec_parallele, pour
		// implémenter la function getLogs_return_tmp()
		System.out.println("\n****************************  Splits Mapping Function Launched  *********************************");
		for (String el_splitted : cles_Sx_machines.keySet()) {
			params = " " + "modeSXUMX " + el_splitted;
			exec_parallele thread_ip = new exec_parallele(cles_Sx_machines.get(el_splitted), login, path_cible, file_cible, params);
			thread_ip.start();
			threads.add(thread_ip);
			System.out.println("Fin d'execution :" + params);
		}

		// Attente de la fin d'execution
		for (exec_parallele thread_ip_launched : threads) {
			thread_ip_launched.join();
			for (String log_return : thread_ip_launched.getLogs_return()) {
				key_return_tmp.add(log_return);
			}
		}

		System.out.println("key_return_tmp : " + key_return_tmp);
		
		double time_end_2 = java.lang.System.currentTimeMillis();
		temps_execution_functions.put("Splits Mapping Function", (time_end_2 - time_start_2) / 1000.0);
		
		
		
		// Création du dictionnaire clés-UMx
		for (String keys_tmp : key_return_tmp) {
			ArrayList<String> cles_UMx_sub = new ArrayList<String>();
			for (String el : keys_tmp.split(" ")) {		
				if (!el.trim().substring(0, Math.min(el.length(), 2)).equals("UM")) {
					cles_UMx_sub.add(el);
				}
			}
			if (!cles_UMx_tmp.containsKey(keys_tmp.split(" ")[0])) {
				cles_UMx_tmp.put(keys_tmp.split(" ")[0], cles_UMx_sub);
			}
		}
		System.out.println("cles_UMx_tmp : " + cles_UMx_tmp);

		
		
		// Création de la clés-UMx
		for (String key_umx : cles_UMx_tmp.keySet()) {
			for (String key_str_words : cles_UMx_tmp.get(key_umx)) {
				for (String key_word : key_str_words.split(" ")) {
					if (!cles_UMx.containsKey(key_word)) {
						HashSet<String> umx_list_tmp = new HashSet<String>();
						umx_list_tmp.add(key_umx);
						cles_UMx.put(key_word, umx_list_tmp);
					} else {
						cles_UMx.get(key_word).add(key_umx);
					}
				}
			}
		}
		System.out.println("cles_UMx : " + cles_UMx);
		System.out.println("Tout est fini");

		double time_start_3 = java.lang.System.currentTimeMillis();
		
		
		// Préparation des paramètres à envoyer aux slaves
		params = " " + "modeUMXSMX";
		int index_smx = 0;
		for (String i_word : cles_UMx.keySet()) {
			String params_smx_name = "SM" + index_smx;
			String extend_params = i_word + " " + params_smx_name;
			for (String i_umx_word : cles_UMx.get(i_word)) {
				extend_params = extend_params + " " + i_umx_word;
			}
			params_config.add(params + " " + extend_params);
			index_smx += 1;
		}
		
		
		threads.clear();
		
		System.out.println(params_config);
		int index_select_ip = 0;
		for (String el_param : params_config) {
			String ip_word_selected = "";
		
			ip_word_selected = list_ip_with_time.keySet().toArray(new String[index_select_ip])[index_select_ip];
			if(index_select_ip < list_ip_with_time.keySet().size() - 1){
				index_select_ip += 1;
			}
			else{
				index_select_ip = 0;
			}
			exec_parallele thread_ip = new exec_parallele(ip_word_selected, login, path_cible, file_cible, el_param);
			thread_ip.start();
			threads.add(thread_ip);
		}

		// Attente de la fin d'execution de tous les threads
		for (exec_parallele thread_ip_launched : threads) {
			thread_ip_launched.join();
			for (String log_return : thread_ip_launched.getLogs_return()) {
				smx_key_return_tmp.add(log_return);
				cles_RMx_machines.put(log_return.split(" ")[0], Integer.parseInt(functions_tool.word_cleaner_regex(log_return.split(" ")[1]).trim()));
			}
		}
		System.out.println("Fin d'execution : " + params);
		System.out.println("RMx returns : " + cles_RMx_machines);
		
		double time_end_3 = java.lang.System.currentTimeMillis();
		temps_execution_functions.put("Reducing Sorted Maps Function", (time_end_3 - time_start_3) / 1000.0);
		
			
		new File(final_file).delete();
		for(String final_key_word : cles_RMx_machines.keySet()){
			String str_final_word = final_key_word + " " + cles_RMx_machines.get(final_key_word);
			functions_tool.write_file(final_file, str_final_word);
		}
		System.out.println("Création du fichier : " + final_file);
		
		int tot = 0;
		for(int el : cles_RMx_machines.values()){
			tot += el; 
		}
		System.out.println("WORD COUNT REPORT : " + tot);
		System.out.println("Splitting Function : " + temps_execution_functions.get("Splitting Function"));
		System.out.println("Splits Mapping Function : " + temps_execution_functions.get("Splits Mapping Function"));
		System.out.println("Reducing Sorted Maps Function : " + temps_execution_functions.get("Reducing Sorted Maps Function"));
	}

}
