package master_shavadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ssh_manager {

	// Connection to distant worker
	public static String connection_distant(String login, String ip) throws InterruptedException {
		ProcessBuilder proc = new ProcessBuilder("ssh", login + "@" + ip, " BatchMode yes; echo $((2+3))");	// "-o connectTimeout=1 " + 
		Process proc_1;
		try {
			double time_start = java.lang.System.currentTimeMillis();
			proc_1 = proc.start();
			proc_1.waitFor();
			if (proc_1.exitValue() == 0) {
				double time_end = java.lang.System.currentTimeMillis();
				return "Connection successfully established on " + ip + " in " + (time_end - time_start) / 1000.0 + "s";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	

	// Check available worker's IP
	public static String check_save_goods_ip(String login, String ip_source_list)
			throws InterruptedException, IOException {
		ArrayList<String> ips_list_all = new ArrayList<String>();
		ips_list_all = functions_tool.read_file(ip_source_list);
		String file_path = System.getProperty("user.dir") + "/" + "logs_connexions.txt";
		new File(file_path).delete();
		new File(file_path).createNewFile();
		for (String ip_from_list : ips_list_all) {
			String ip_tmp = connection_distant(login, ip_from_list);
			System.out.println(ip_tmp);
			if (ip_tmp != null)
				functions_tool.write_file(file_path, ip_tmp);
		}
		return file_path;
	}

	// Selection d'une IP aléatoire
	public static String select_random_ip(ArrayList<String> pool_ip) {
		Random rn = new Random();
		int ip_index = rn.nextInt(pool_ip.size());
		return pool_ip.get(ip_index);
	}

	public static boolean checkIP(String ip) {
	
		String[] c = ip.split("\\.");
	
		// vérifie que l'on est bien de la forme X.X.X.X
		if (c.length != 4) {
			return false;
		}
	
		// les points en fin de chaine ne sont pas détectés par split
		if (ip.endsWith(".")) {
			return false;
		}
	
		if (c[0].equals("0")) {
			return false;
		}
	
		// vérifie chacun des X
		for (String s : c) {
			if (!functions_tool.isInteger(s)) {
				return false;
			}
	
			int value = Integer.parseInt(s);
			if ((value < 0) || (value > 255)) {
				return false;
			}
		}
	
		return true;
	}

	// Extract available worker's IP with response time
	public static HashMap<String, Double> extract_ip_from_logs(String ip_from_logs) {
		HashMap<String, Double> good_ip_with_time = new HashMap<String, Double>();
		String good_ip = null;
		String regex_pattern = "[0-9]+(.)[0-9]+";
		Pattern p = Pattern.compile(regex_pattern);
	
		double response_time = 0.0;
		for (String sub_el : ip_from_logs.split(" ")) {
			if (checkIP(sub_el)) {
				Matcher m = p.matcher(ip_from_logs);
				String ip_time = null;
				while (m.find()) {
					ip_time = m.group();
				}
				good_ip_with_time.put(sub_el, Double.parseDouble(ip_time));
			}
		}
		return good_ip_with_time;
	}

	// En construction
	public static HashMap<String, Double> ip_workers_scanner(String login) throws InterruptedException {
		HashMap<String, Double> pool_ip_adress = new HashMap<>();
		String file_path = System.getProperty("user.dir") + "/" + "logs_connexions_v2.txt";
		String pool_init = "137.194.34.";
		int index_ip = 2;
	
		while (index_ip < 100) {
			String ip_tested = pool_init + index_ip;
			String ip_tmp = connection_distant(login, ip_tested);
			System.out.println(ip_tested + " : " + ip_tmp);
			if (ip_tmp != null) {
				pool_ip_adress.putAll(extract_ip_from_logs(ip_tmp));
				functions_tool.write_file(file_path, ip_tmp);
			}
			index_ip += 1;
		}
		return pool_ip_adress;
	}
	
	

}
