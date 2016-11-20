package slave_shavadoop;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

import master_shavadoop.functions_tool;

public class compute_distant {

	public static void simul_compute() throws InterruptedException {
		// System.out.println("Début du calcul");
		Thread.sleep(10000);
	}

	// split Sx file to UMx
	public static void splits_mapping(String sx_file) {
		HashMap<String, ArrayList<String>> slave_cle_umx = new HashMap<String, ArrayList<String>>();
		ArrayList<String> list_words = new ArrayList<String>();
		ArrayList<String> list_unik_words = new ArrayList<String>();
		
		// Extraire le nom de fichier
		String sx_name = Paths.get(sx_file).getFileName().toString();
		String umx_file = "UM" + sx_name.substring(sx_name.length() - 1, sx_name.length());
		new File(umx_file).delete();

		// Lecture du fichier Sx de la machine
		list_words = functions_tool.read_file(sx_file);

		// Ecriture des fichiers UMx
		// identification des clés du fichier
		for (String list_words_line : list_words) {
			for (String word_no_unik : list_words_line.split(" ")) {
				functions_tool.write_file(umx_file, word_no_unik + " 1");
				 if(!list_unik_words.contains(word_no_unik)){
					 list_unik_words.add(word_no_unik);
				 }
			}
		}
		
		// Création de : clé-UMx
		slave_cle_umx.put(umx_file, list_unik_words);

		// Formatage et transfert de la clé via affichage
		String str_tmp = "";
		String str_format = "";
		for (String return_tmp : slave_cle_umx.keySet()) {
			ArrayList<String> sl = slave_cle_umx.get(return_tmp);
			for (String return_tmp_sub : sl) {
				str_tmp = str_tmp + return_tmp_sub + " ";
			}
			str_format = return_tmp + " " + str_tmp;
		}
		System.out.println(str_format);
	}
	
	// UMx > RMx
	public static HashMap<String, Integer> shuffling_maps(String params){
		ArrayList<String> list_words_all = new ArrayList<String>();
		String word = params.split(" ")[0];
		String smx_cible = params.split(" ")[1].toString();
		
		new File(smx_cible).delete();
		for(String el_param : params.split(" ")){
			if(!el_param.equals(word) && !el_param.equals(smx_cible)){
				list_words_all.addAll(functions_tool.read_file(el_param));
			}
		}
		System.out.println(">>>> list_words_all : " + list_words_all);
		for(String word_selected : list_words_all){
			if(word_selected.split(" ")[0].equals(word)){
				functions_tool.write_file(smx_cible, word_selected);
			}
		}
		//System.out.println(smx_cible);
		//return smx_cible;
	
		ArrayList<String> list_words_to_reduce = new ArrayList<String>();
		HashMap<String, Integer> words_reduced = new HashMap<String, Integer>();
		
		String rmx_file = "RM" + params.substring(params.length() - 1, params.length());
		String str_words_reduced_tmp = "";
		
		new File(rmx_file).delete();
		//list_words_to_reduce.addAll(functions_tool.read_file(params));
		list_words_to_reduce.addAll(list_words_all);
		words_reduced = functions_tool.words_count(list_words_to_reduce);
		for(String key_word_reduced :  words_reduced.keySet()){
			str_words_reduced_tmp = key_word_reduced + " " + words_reduced.get(key_word_reduced);
		}
		functions_tool.write_file(rmx_file, str_words_reduced_tmp);
		System.out.println(str_words_reduced_tmp);
		return words_reduced;
	}

	
	public static HashMap<String, Integer> reducing_sorted_maps(String params){
		ArrayList<String> list_words_to_reduce = new ArrayList<String>();
		HashMap<String, Integer> words_reduced = new HashMap<String, Integer>();
		
		String rmx_file = "RM" + params.substring(params.length() - 1, params.length());
		String str_words_reduced_tmp = "";
		
		new File(rmx_file).delete();
		list_words_to_reduce.addAll(functions_tool.read_file(params));
		words_reduced = functions_tool.words_count(list_words_to_reduce);
		for(String key_word_reduced :  words_reduced.keySet()){
			str_words_reduced_tmp = key_word_reduced + " " + words_reduced.get(key_word_reduced);
		}
		functions_tool.write_file(rmx_file, str_words_reduced_tmp);
		System.out.println(str_words_reduced_tmp);
		return words_reduced;
	}
	

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		//HashMap<String, ArrayList<String>> cle_umx_tmp = new HashMap<String, ArrayList<String>>();
		HashMap<String, Integer> cle_rmx_tmp = new HashMap<String, Integer>();
		String cle_smx_tmp;
		
		if (args[0] != null) {
			switch (args[0]) {
			case "simul":
				// System.out.println(">> Execution simul");
				simul_compute();
				break;
			case "modeSXUMX":
				// System.out.println(">> Execution split_mapping");
				splits_mapping(args[1]);
				break;
			case "modeUMXSMX":
				//System.out.println(">> Execution shuffling_maps");
				int nb_arguments = args.length; 	//Integer.parseInt(args[1]);
				int i = 1;
				String str_tmp = "";
				while(i < nb_arguments){
					str_tmp = str_tmp + args[i] + " ";
					i += 1;
				}
				cle_rmx_tmp = shuffling_maps(str_tmp);		// cle_smx_tmp
				//cle_rmx_tmp.putAll(reducing_sorted_maps(cle_smx_tmp));
				break;
			default:
				// System.out.println(">> Execution simul");
				simul_compute();
				break;
			}
		} else {
			// System.out.println(">> Execution simul");
			simul_compute();
		}
	}

}
