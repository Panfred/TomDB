
package uzh.tomdb.main;

import java.util.Scanner;

/**
 * 
 * Main class to start TomDB as a peer.
 * 
 * Option are:
 *
 * 		one argument: number of local peers to start
 * 		two arguments: IP address to bootstrap to, true/false to choose a random port (for local execution of many peers)
 * 		three arguments: number of local peers, IP address to bootstrap to, true/false to choose a random port
 * 
 * Type exit to stop the execution.
 * 
 * @author Francesco Luminati
 */
public class Main {

	public static void main(String[] args) {
		
		boolean randomPort = false;
		
		if (args.length == 1) {
			if (args[0].contains(".")) {
				TomDB.startDHT(args[0], randomPort);
			} else {
				TomDB.createLocalPeers(Integer.parseInt(args[0]));
				TomDB.startDHT();
			}
				
		} else if (args.length == 2) {
			if (args[0].contains(".")) {
				if (args[1].equals("true")) {
					randomPort = true;
				}
				TomDB.startDHT(args[0], randomPort);
			} else {
				TomDB.createLocalPeers(Integer.parseInt(args[0]));
				TomDB.startDHT(args[1], randomPort);
			}
			
		} else if (args.length == 3) {
			if (args[2].equals("true")) {
				randomPort = true;
			}
			TomDB.createLocalPeers(Integer.parseInt(args[0]));
			TomDB.startDHT(args[1], randomPort);
		} else {
			TomDB.startDHT();
		}
		
		@SuppressWarnings("resource")
		Scanner input = new Scanner(System.in);
		while (input.hasNext()) {
			if (input.nextLine().equals("exit")) {
				TomDB.stopDHT();
			}
		}
		
		
	}

}
