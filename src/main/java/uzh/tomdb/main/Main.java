package uzh.tomdb.main;

import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		
		if (args.length > 0) {
			TomDB.createLocalPeers(Integer.parseInt(args[0]));
			TomDB.getConnection();
		} else {
			TomDB.getConnection();
		}

		@SuppressWarnings("resource")
		Scanner input = new Scanner(System.in);
		while (input.hasNext()) {
			if (input.nextLine().equals("exit")) {
				TomDB.closeConnection();
			}
		}
		
		
	}

}
