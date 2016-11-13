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

	public static HashMap<String, ArrayList<String>> split_mapping(String sx_file) {
		HashMap<String, ArrayList<String>> slave_cle_umx = new HashMap<String, ArrayList<String>>();
		ArrayList<String> list_words = new ArrayList<String>();
		ArrayList<String> list_unik_words = new ArrayList<String>();
		// Extraire le nom de fichier
		String sx_name = Paths.get(sx_file).getFileName().toString();
		String umx_file = "UM" + sx_name.substring(sx_name.length() - 1, sx_name.length());
		new File(umx_file).delete();

		// Lecture du fichier Sx de la machine
		list_words = functions_tool.read_file(sx_file);

		for (String list_words_line : list_words) {
			for (String word_no_unik : list_words_line.split(" ")) {
				String tmp_word = word_no_unik + " 1";
				functions_tool.write_file(umx_file, tmp_word);
				 if(!list_unik_words.contains(word_no_unik)){
					 list_unik_words.add(word_no_unik);
				 }
			}
		}

		// Construction du fichier UMx de la machine /!\ Ne pas sommer les
		// occurences à cette étape !
//		 list_word_count = nOccurrences(list_words);
		
//		 for (String word_counted : list_words) {
//			 if(!list_unik_words.contains(word_counted)){
//				 list_unik_words.add(word_counted);
//			 }
//		 }

		slave_cle_umx.put(umx_file, list_unik_words);

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
		return slave_cle_umx;
	}

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		HashMap<String, ArrayList<String>> cle_umx_tmp = new HashMap<String, ArrayList<String>>();
		
		if (args[0] != null) {
			switch (args[0]) {
			case "simul":
				// System.out.println(">> Execution simul");
				simul_compute();
				break;
			case "split_mapping":
				// System.out.println(">> Execution split_mapping");
				cle_umx_tmp = split_mapping(args[1]);
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
