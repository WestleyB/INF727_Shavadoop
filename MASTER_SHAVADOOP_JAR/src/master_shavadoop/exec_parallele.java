package master_shavadoop;

import java.io.IOException;
import java.util.ArrayList;

public class exec_parallele extends Thread implements Runnable {
	private String ip;
	private String login;
	private String path_cible;
	private String params;
	public ArrayList<String> logs_return_tmp = new ArrayList<String>();

	public exec_parallele(String ip, String login, String path_cible, String params) {
		// TODO Auto-generated constructor stub
		this.ip = ip;
		this.login = login;
		this.path_cible = path_cible;
		this.params = params;
	}
	
	public ArrayList<String> getLogs_return_tmp() {
		return this.logs_return_tmp;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			System.out.println("Start slave on ip " + this.ip);
			this.logs_return_tmp = functions_tool.compute_slave_distant(this.ip, this.login, this.path_cible, this.params);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}