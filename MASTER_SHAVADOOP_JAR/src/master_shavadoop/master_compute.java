package master_shavadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class master_compute {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		// ================================   Données Inputs   ==========================================
		String login = "wbirmingham"; // "ghost"; //= "pi";
		String path_cible = "~/workspace/TPTelecom/INF727/"; // Chemin cible
																// pour l'envoi
																// du fichier
																// JAR
		String file_cible = path_cible + "slave_calcul.jar";
		String ips_source = "liste_ip.txt";
		String input_file = "Input.txt";
		// ==============================================================================================

		ArrayList<String> ips_list_ok = new ArrayList<String>();
		String params = "";
		HashMap<String, String> cles_Sx_machines = new HashMap<String, String>();
		HashMap<String, String> cles_UMx_machines = new HashMap<String, String>();
		HashMap<String, ArrayList<String>> cles_UMx_tmp = new HashMap<String, ArrayList<String>>();
		HashMap<String, ArrayList<String>> cles_UMx = new HashMap<String, ArrayList<String>>();
		ArrayList<exec_parallele> threads = new ArrayList<exec_parallele>();
		ArrayList<String> key_return_tmp = new ArrayList<String>();
		
		// Vérification des IPs valides
		String file_logs = functions_tool.check_save_goods_ip(login, ips_source);

		// Création de la liste d'IPs valides
		ips_list_ok = functions_tool.read_file(file_logs);

		// //Selection d'une IP aléatoire
		// Random rn = new Random();
		// int ip_index = rn.nextInt(ips_list_ok.size());
		// int ip_cpt = 0;
		//
		// //Execution sur une seule machine
		// for(String ip_ok_from_list : ips_list_ok){
		// ip_cpt += 1;
		// if(ip_cpt == ip_index){
		// // Envoi du fchier JAR vers la machine distante
		// compute_distant(ip_ok_from_list.split(" ")[1], login, file_cible,
		// "");
		// System.out.println("Fin d'execution");
		// }
		// }

		// Execution parallelisée de la simulation de calcul
		params = " " + "simul";
		
		//functions_tool.launch_parallel_exec(ips_list_ok, login, file_cible, params);		// => Probleme de parallelisation
		
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
		
		

		// 1- Sx Splitting Function Master - création du dictionnaire UMx-machines
		cles_Sx_machines = functions_tool.splitting_function(ips_list_ok, input_file, login, path_cible);
		for(String cle_sx : cles_Sx_machines.keySet()){
			String cle_umx_tmp = "UM" + cles_Sx_machines.get(cle_sx).substring(1, 2);
			cles_UMx_machines.put(cle_umx_tmp, cle_sx);
		}
		System.out.println(cles_UMx_machines);
		
		// Execution parallelisée de split_mapping du JAR slave 
		// extends de la classe Thread à la classe exec_parallele, pour implémenter la function getLogs_return_tmp()
		for (String el_splitted : cles_Sx_machines.keySet()) {
			params = " " + "split_mapping " + cles_Sx_machines.get(el_splitted);
			
			exec_parallele thread_ip = new exec_parallele(el_splitted, login, file_cible, params);
			thread_ip.start();
			threads.add(thread_ip);
			System.out.println("Fin d'execution :" + params);
		}

		// Attente de la fin d'execution
		for (exec_parallele thread_ip_launched : threads) {
			thread_ip_launched.join();
			for(String log_return : thread_ip_launched.getLogs_return_tmp()){
				key_return_tmp.add(log_return);
			}
		}

		// Création du dictionnaire clés-UMx
		for (String keys_tmp : key_return_tmp) {
			ArrayList<String> cles_UMx_sub = new ArrayList<String>();
			for (String el : keys_tmp.split(" ")) {
				if (!el.trim().substring(0, 2).equals("UM")) {
					cles_UMx_sub.add(el);
				}
			}
			if (!cles_UMx_tmp.containsKey(keys_tmp.split(" ")[0])) {
				cles_UMx_tmp.put(keys_tmp.split(" ")[0], cles_UMx_sub);
			}
		}
		//System.out.println(cles_UMx_tmp);
		
		//Création de la clés-UMx
		for(String key_umx : cles_UMx_tmp.keySet()){
			for(String key_str_words : cles_UMx_tmp.get(key_umx)){
				//for(String key_str_words : key_list_words){
					for(String key_word : key_str_words.split(" ")){
						if(!cles_UMx.containsKey(key_word)){
							ArrayList<String> umx_list_tmp = new ArrayList<String>();
							umx_list_tmp.add(key_umx);
							cles_UMx.put(key_word, umx_list_tmp);
						}
						else{
							cles_UMx.get(key_word).add(key_umx);
						}
							
					//}
					
				}
			}
		}
		System.out.println(cles_UMx);
	}

}
