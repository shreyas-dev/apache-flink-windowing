package dev.shreyas.flink.test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;

/**
 * @author shreyas b
 * @created 17/04/2020 - 12:43 AM
 * @project flink-windows
 **/


public class SessionWindowDataServer {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9094);
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                Random rand = new Random();
                Date d = new Date();
                int j;
                int sum ;
                while (true) {
                    j = 0;
                    sum = 0;
                    while (j < 10) {
                        int i = rand.nextInt(100);
                        sum+=i;
                        String s = "" + System.currentTimeMillis() + "," + i;
                        System.out.println(s);
                        /* <timestamp>,<random-number> */
                        out.println(s);
                        j++;
                    }
                    System.out.println("In this event , sum :"+sum);
                    Thread.sleep(3000);
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
