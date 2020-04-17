package dev.shreyas.flink.test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

/**
 * @author shreyas b
 * @created 17/04/2020 - 2:08 AM
 * @project flink-windows
 **/


public class GlobalWindowDataServer {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9096);
        String[] countries = {"US","India","UK","Israel","France"};
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();
                int j;
                while (true) {
                    int i = rand.nextInt(100);
                    String s = countries[i%5]+"," + System.currentTimeMillis() + "," + i;
                    System.out.println(s);
                    /* <timestamp>,<random-number> */
                    out.println(s);
//                    j++;
                    Thread.sleep(50);
                }
            } finally{
                socket.close();
            }

        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
        }
    }
}
