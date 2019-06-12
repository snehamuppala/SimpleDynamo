package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.net.InetAddress.getByAddress;

public class SimpleDynamoProvider extends ContentProvider {
    private final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    static boolean flag = false;
    String avd[] = {"5554", "5556", "5558", "5560", "5562"};
    HashMap<String, List<String>> succMap = new HashMap<String, List<String>>();
    HashMap<String, List<String>> predMap = new HashMap<String, List<String>>();
    static HashMap<String, String> messageListRecover = new HashMap<String, String>();
    static int SERVER_PORT = 10000;
    String myPort = null;
    String portStr = null;

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        while(!flag) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // TODO Auto-generated method stub
        String content[] = getContext().fileList();
        try {

            if (selection.equals("*") || selection.equals("@")) {
                for (String file : content) {
                    getContext().deleteFile(file);
                }

            }
            else {

                List<String> deletelist = new ArrayList<String>();
                deletelist = getPartitionList(selection);


                if (selection.contains(portStr)) {
                    selection = selection.substring(4);
                    System.out.println("key selection:::" + selection);
                    DeleteIntoFile(selection);
                    return Integer.parseInt(null);
                }
                for (String s : deletelist) {
                    System.out.println("delete in this avd" + s);

                    String messageSend = generateMessage(portStr, "delete", s+selection, null);
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, messageSend, s);

                }
            }
        } catch (Exception e) {
            Log.e("DELETE", e.getLocalizedMessage());
        }
        return 0;
    }

    public synchronized void DeleteIntoFile(String selection) {
        System.out.println("DeleteIntoFile hereeeeee");

        getContext().deleteFile(selection);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public synchronized void insertIntoFile(String key, String value) {
        System.out.println("insertfinalll hereeeeee");
        Log.i("key value at insert= ", key);
        Log.i("MAP VALUES", " ");
        ArrayList<String> content = new ArrayList<String>(Arrays.asList(getContext().fileList()));
        try {
            if (content.contains(key)) {

                Log.i("contains keycondition", " ");
                Log.i("key =", key);
                String fileMessage = null;
                FileInputStream inputstream = getContext().openFileInput(key);
                InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                fileMessage = bufferedReader.readLine();
                String[] value1 = fileMessage.split("#");

                int i = Integer.parseInt(value1[1]);
                i++;
                System.out.println("value starr split:::" + value1[0]);
                value = value + "#" + i;
                System.out.println("incremented valueeee " + value);
                System.out.println("keyyyy " + key);

            } else {

                value = value + "#" + 1;
                System.out.println("value elseee:::" + value);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            FileOutputStream outputStream = null;

            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void insertIntoFileForRecovery(String key, String value) {
        System.out.println("insertfinalll hereeeeee");


        Log.i("key value at insert= ", key);
        Log.i("MAP VALUES", " ");

        ArrayList<String> content = new ArrayList<String>(Arrays.asList(getContext().fileList()));
        try {

            if (content.contains(key)) {
                Log.i("contains keycondition", " ");
                Log.i("key =", key);
                String fileMessage = null;
                FileInputStream inputstream = getContext().openFileInput(key);
                InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                fileMessage = bufferedReader.readLine();
                String[] value1 = fileMessage.split("#");

                int old = Integer.parseInt(value1[1]);
                int newVersion = Integer.parseInt(value.split("#")[1]);

                if (newVersion < old) {
                    value = fileMessage;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            FileOutputStream outputStream = null;
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        while(!flag) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("insert hereeeeee");
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        System.out.println("value " + value);
        List<String> insertlist = new ArrayList<String>();
        insertlist = getPartitionList(key);

        if (value.contains(portStr)) {
            value = value.substring(4);
            System.out.println("value 22:::" + value);
            insertIntoFile(key, value);
            return null;
        }
        for (String s : insertlist) {
            System.out.println("insert in this avd" + s);

            String messageSend = generateMessage(portStr, "request", key, s + value);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, messageSend, s);

        }
        return null;

    }

    public boolean correctAvdCheck(String key, String avd, String prev) {
        boolean Avd = false;
        try {
            if (genHash(key).compareTo((genHash(avd))) <= 0 && genHash(key).compareTo((genHash(prev))) > 0)
                Avd = true;

            if ((genHash(avd)).compareTo((genHash(prev))) < 0 && (genHash(key).compareTo((genHash(avd))) < 0 || genHash(key).compareTo((genHash(prev))) > 0))
                Avd = true;
        } catch (Exception e) {
            Log.e("CHECK", e.getMessage());
        }

        return Avd;
    }


    private List<String> getPartitionList(String key) {
        for (String avd1 : avd) {
            List<String> pp = new ArrayList<String>();
            pp = predMap.get(avd1);
            boolean result = correctAvdCheck(key, avd1, pp.get(0));

            if (result) {
                List<String> ss = new ArrayList<String>();
                List<String> returnlist = new ArrayList<String>();
                ss = succMap.get(avd1);

                returnlist.add(avd1);
                returnlist.add(ss.get(0));
                returnlist.add(ss.get(1));

                return returnlist;
            }
        }

        return null;
    }
    private String generateMessage(String port, String action, String key, String value) {

        String divider = ":";
        StringBuilder sb = new StringBuilder();
        sb.append(port).append(divider);
        sb.append(action).append(divider);
        sb.append(key).append(divider).append(value).append(divider);
        return sb.toString();
    }

    private String getPredecessorAndSuccessor() {


        String pred1 = "";
        String pred2 = "";
        String succ1 = "";
        String succ2 = "";

        List<String> succ = new ArrayList<String>();
        List<String> pred = new ArrayList<String>();

        for (String node : avd) {

            succ = new ArrayList<String>();
            pred = new ArrayList<String>();
            if (node.equals("5554")) {
                pred1 = "5556";//use
                pred2 = "5562";
                succ1 = "5558";
                succ2 = "5560";
            }
            if (node.equals("5556")) {
                pred1 = "5562";
                pred2 = "5560";
                succ1 = "5554";
                succ2 = "5558";
            }
            if (node.equals("5558")) {
                pred1 = "5554";
                pred2 = "5556";
                succ1 = "5560";
                succ2 = "5562";
            }

            if (node.equals("5560")) {
                pred1 = "5558";
                pred2 = "5554";
                succ1 = "5562";
                succ2 = "5556";
            }
            if (node.equals("5562")) {

                pred1 = "5560";
                pred2 = "5558";
                succ1 = "5556";
                succ2 = "5554";

            }

            succ.add(succ1);
            succ.add(succ2);
            pred.add(pred1);
            pred.add(pred2);
            succMap.put(node, succ);
            predMap.put(node, pred);
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        System.out.println("myposrt" + portStr);

        try {

            getPredecessorAndSuccessor();
            if (0 == getContext().fileList().length) {
                flag = true;
                Log.v("ONCREATE", "not faileddd");
            } else {
                Log.v("ONCREATE", "Comingggguppp" + portStr);
                flag = false;
                bringBack();

            }
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // TODO Auto-generated method stub
        return false;
    }
    private void bringBack() {

        Log.v("ONCREATE", "bringback" + portStr);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "failure", portStr);
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        while(!flag) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("query hereeeeee");
        String fileMessage = " ";
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            if (selection.equals("@")) {
                System.out.println("query @@@@@" + portStr);
                String content[] = getContext().fileList();
                for (String file : content) {
                    try {
                        Log.v("QUERY", "inside  @@@@@");
                        FileInputStream inputstream = getContext().openFileInput(file);
                        InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                        BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                        fileMessage = bufferedReader.readLine();

                        String[] fileMessage1 = fileMessage.split("#");
                        System.out.println("query $$$$$::::" + fileMessage1[0]);

                        matrixCursor.addRow(new Object[]{file, fileMessage1[0]});
                    } catch (Exception e) {
                        e.printStackTrace();
                        Log.e("QUERY", e.getLocalizedMessage());
                    }
                }
                return matrixCursor;
            } else if (selection.equals("*")) {
                Log.v("QUERY", "inside  *********");
                String content[] = getContext().fileList();
                for (String file1 : content) {

                    FileInputStream inputstream = getContext().openFileInput(file1);
                    InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                    BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                    fileMessage = bufferedReader.readLine();
                    matrixCursor.addRow(new Object[]{file1, fileMessage});
                }


                for (String avd1 : avd) {

                    Log.v("QUERY", "inside  starlist" + avd1);
                    Log.v("QUERY", "inside  starport" + portStr);
                    if (avd1.equals(portStr))
                        continue;
                    else {

                        try{

                            String messageSend = generateMessage(" ", "Query *", " ", " ");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(avd1) * 2);
                            Log.v("QUERY", "message  star" + messageSend);
                            PrintWriter writer = new PrintWriter(socket.getOutputStream());
                            writer.println(messageSend);
                            writer.flush();
                            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String line = null;
                            while ((line = reader.readLine()) != null) {
                                if (line.length() > 0) {
                                    String[] t = line.split(":");
                                    String k = t[0];
                                    String v = t[1];
                                    Log.v("QUERY", "QQkey" + k);
                                    Log.v("QUERY", "QQQvalue" + v);
                                    matrixCursor.addRow(new Object[]{k, v});
                                }
                            }

                            socket.close();
                            reader.close();
                            writer.close();



                        }catch (Exception ex){

                        }

                        Log.v("QUERY", "inside  star2222" + avd1);
                    }
                    Log.v("QUERY", "nexxxxtttt portttt");
                }


            } else {

                MatrixCursor cursor = singleQuery(selection, matrixCursor);
                return cursor;
            }
            return matrixCursor;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // TODO Auto-generated method stub
        return null;
    }

    private MatrixCursor singleQuery(String selection, MatrixCursor matrixCursor) {
        String fileMessage;
        List<String> querylist = new ArrayList<String>();
        querylist = getPartitionList(selection);
        String latestKey = selection;
        String latestValue = "";
        int latestVersion = -1;

        for (String s : querylist) {

            System.out.println("query replicationnnn" + portStr);
            System.out.println("query replicationnnn" + s);
            try {


                String messageSend = generateMessage(s, "queryAll", selection, " ");
                Socket socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(s) * 2);
                PrintWriter writer = new PrintWriter(socket.getOutputStream());
                writer.println(messageSend);
                writer.flush();
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                fileMessage = reader.readLine();

                System.out.println("query fileMessage  " + fileMessage);

                if(fileMessage != null && fileMessage.length() > 0 && fileMessage.contains("#")){

                    System.out.println("inside query fileMessage  " + fileMessage);

                    int version = Integer.parseInt(fileMessage.split("#")[1]);

                    if(version > latestVersion){
                        latestVersion = version;
                        latestValue = fileMessage.split("#")[0];
                    }
                }

                socket.close();
                reader.close();
                writer.close();


            } catch (UnknownHostException e) {

                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        System.out.println("Outside query  " + latestKey + " " + latestValue);
        matrixCursor.addRow(new Object[]{latestKey, latestValue});

        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<String, String, String> {

        protected String doInBackground(String... msgs) {

            String clientMessage = msgs[0];
            String port = msgs[1];
            System.out.println("pooort" + (Integer.parseInt(port) * 2));
            System.out.println("insidee clienttt  message" + clientMessage);


            try {

                if (clientMessage.equalsIgnoreCase("failure")) {
                    System.out.println("insidee clienttt  failuree" + clientMessage);

                    HashMap<String, String> messageList = new HashMap<String, String>();
                    HashMap<String, String> finalMessageList = new HashMap<String, String>();
                    for (String port1 : avd) {


                        if (port1.equals(portStr)) {
                            continue;
                        }

                        System.out.println("insidee loop" + port1);
                        String getback = generateMessage(port1, "getback", null, null);
                        Socket socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port1) * 2);
                        System.out.println("insidee getbackkk" + getback);
                        PrintWriter writer = new PrintWriter(socket.getOutputStream());
                        writer.println(getback);
                        writer.flush();

                        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            if (line.length() > 0) {
                                String[] t = line.split(":");
                                String k = t[0];
                                String v = t[1];
                                Log.v("QUERY", "QQkey" + k);
                                Log.v("QUERY", "QQQvalue" + v);
                                messageListRecover.put(k, v);
                                System.out.println(" inside client size" + messageListRecover.size());

                            }
                        }

                    }
                    System.out.println("client size" + messageListRecover.size());
                    System.out.println("full data");
                    for (String key1 : messageListRecover.keySet()) {
                        Log.v(key1, String.valueOf(messageListRecover.get(key1)));
                    }
                    List<String> querylist1 = new ArrayList<String>();
                    System.out.println("client value before checcckk" + messageListRecover.size());

                    for (String key : messageListRecover.keySet()) {
                        querylist1 = getPartitionList(key);

                        if (querylist1.contains(portStr)) {

                            String value = messageListRecover.get(key);
                            System.out.println("client value sneha" + messageListRecover.size());
                            insertIntoFileForRecovery(key, value);
                        }

                    }

                            flag=true;
                } else {

                    Socket socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port) * 2);
                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    System.out.println("insidee clienttt" + clientMessage);

                    writer.println(clientMessage);
                    writer.flush();
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            String serverMessage = null;
            ContentValues val = new ContentValues();
            System.out.println("insidee server");
            try {
                while (true) {

                    Socket socket = serverSocket.accept();
                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    serverMessage = reader.readLine();
                    System.out.println("insidee server" + serverMessage);
                    String msg[] = serverMessage.split(":");
                    String portReceived = msg[0];
                    String action = msg[1];
                    String keyReceived = msg[2];
                    String valueReceived = msg[3];
                    String vall[] = valueReceived.split(":");

                    Log.v("Server", "Port received " + portReceived);
                    Log.v("Server", "Action " + action);
                    Log.v("Server", "keyReceived " + keyReceived);
                    Log.v("Server", "valueReceived " + valueReceived);
                    while(!flag) {
                        try {
                            Thread.sleep(500);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    if (action.equalsIgnoreCase("request")) {

                        val.put("key", keyReceived);
                        val.put("value", valueReceived);
                        Uri uri = providerUri;
                        insert(uri, val);
                    }

                    else if (action.equalsIgnoreCase("delete")) {
                        delete(providerUri,keyReceived,null);
                    }

                    else if (action.equalsIgnoreCase("queryAll")) {

                        Log.v("QUERY", "inside queryalll value");

                        FileInputStream inputstream = getContext().openFileInput(keyReceived);
                        InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                        BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                        PrintWriter pw = new PrintWriter(socket.getOutputStream());
                        String value = bufferedReader.readLine();
                        System.out.println("query vvv" + value);
                        pw.println(value);
                        pw.flush();


                    } else if (action.equalsIgnoreCase("Query *")) {
                        Log.v("", "inside queryalll *****" + portReceived);
                        Cursor cursor = query(providerUri, null, "@", null, null);


                        StringBuilder sb = new StringBuilder();
                        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                            int keyIndex = cursor.getColumnIndex("key");
                            int valueIndex = cursor.getColumnIndex("value");
                            String returnKey = cursor.getString(keyIndex);
                            String value = cursor.getString(valueIndex);
                            String[] returnValue = value.split("#");
                            sb.append(returnKey).append(":").append(returnValue[0]).append("\n");
                            Log.v("query", "Works key " + returnKey + " : " + returnValue[0]);

                        }
                        Log.v("SERVER " + Thread.currentThread().getName(), "message " + sb.toString());
                        PrintWriter pw = new PrintWriter(socket.getOutputStream());
                        pw.println(sb.toString());
                        pw.flush();
                        pw.close();
                        cursor.close();

                    } else if (action.equalsIgnoreCase("working")) {
                        writer = new PrintWriter(socket.getOutputStream(), true);
                        writer.println("This node is working");
                        writer.flush();
                    } else if (action.equalsIgnoreCase("getback")) {
                        String fileMessage = null;


                        String content[] = getContext().fileList();
                        StringBuilder sb1 = new StringBuilder();


                        for (String file : content) {
                            try {
                                Log.v("SERVER", "inside  baccccckkkkk" + portReceived);
                                FileInputStream inputstream = getContext().openFileInput(file);
                                InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                                BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                                fileMessage = bufferedReader.readLine();
                                sb1.append(file).append(":").append(fileMessage).append("\n");


                            } catch (Exception e) {
                                e.printStackTrace();
                                Log.e("SERVER", e.getLocalizedMessage());
                            }
                        }
                        writer = new PrintWriter(socket.getOutputStream(), true);
                        writer.println(sb1.toString());
                        writer.flush();
                        writer.close();
                    }
                }
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return null;
        }

    }
}


