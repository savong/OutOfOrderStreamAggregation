package com.journaldev.socket;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.*;
import java.io.FileReader;

import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
/**
 * This class implements java Socket server
 * @author pankaj
 *
 */
public class DataStreamSource_Transmission {

    public static void main(String[] args) {
        int port = 9999;
        boolean stop = false;
        ServerSocket serverSocket = null;


        try {

            serverSocket = new ServerSocket(port);

            while (!stop) {
                Socket echoSocket = serverSocket.accept();
                PrintWriter out =
                        new PrintWriter(echoSocket.getOutputStream(), true);
                BufferedReader in =
                        new BufferedReader(
                                new InputStreamReader(echoSocket.getInputStream()));
                System.out.println("Connected to client");
                System.out.println("Start sending data stream to client");
                //read data from text file and send through socket
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new FileReader("Sample_out_of_order_data.txt"));
                    String line = reader.readLine();

                    while (line != null) {
                        //System.out.println(line);
                        out.println(line);
                        try {
                            //Thread.sleep(1 * 1000);
                            Thread.sleep(1);//millisecond
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }

                        // read next line
                        line = reader.readLine();
                    }

                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                /*while(true)
                 {
                    out.println("Hello Amazing");
                    out.println("Hi Client "+ rand.nextInt(1000));
                 }
                 */
                // do whatever logic you want.

                //in.close();
                //out.close();
                //echoSocket.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                serverSocket.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
