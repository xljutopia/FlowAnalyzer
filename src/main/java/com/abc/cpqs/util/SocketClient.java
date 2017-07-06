package com.abc.cpqs.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by lijiax on 6/13/17.
 */
public class SocketClient {
    private Socket socket;

    private String ip;

    private int port;

    public SocketClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
        try {
            socket = new Socket(ip, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void send(String msg) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(msg.getBytes("UTF-8"));
        outputStream.flush();
        outputStream.close();
        socket.close();
    }
}
