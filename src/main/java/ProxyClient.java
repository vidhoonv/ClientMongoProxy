import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.util.Properties;
import java.util.UUID;


public class ProxyClient {

    public static String accEndpoint;
    public static int mongoPort;
    public static int proxyPort;    
    
    public static int maxNumberofCursors = 1000;
    public static int cursorExpiryAgeInMins = 30;

    public static void main(String[] args) throws IOException {

        ProxyClient.ParseConfig();
        
        ProxyClient.PrintConfig();

        UUID connId = null;
        ServerSocket listener = null;
        try 
        {
            listener = new ServerSocket(proxyPort);
            while (true) 
            {
                //MongoClient -> Client PROXY
                Socket incomingSocket = listener.accept();
                System.out.println("Accepted an incoming connection from "+ incomingSocket.getRemoteSocketAddress().toString());
                MongoConnection incomingConnection = MongoConnection.FromIncomingSocket(incomingSocket);

                connId = UUID.randomUUID();

                //Client PROXY -> Mongo server
                //outgoing connection is SSL enabled
                SSLSocketFactory factory = 
                        (SSLSocketFactory)SSLSocketFactory.getDefault();
                SSLSocket outgoingSocket = 
                        (SSLSocket)factory.createSocket(ProxyClient.accEndpoint, ProxyClient.mongoPort);
                System.out.println("Created an outgoing connection to "+ ProxyClient.accEndpoint + ":" + ProxyClient.mongoPort);                
                MongoConnection outgoingConnection = MongoConnection.FromOutgoingSocket(outgoingSocket);
                new MongoForwarder(incomingConnection, outgoingConnection, connId).start();
            }
        } 
        catch(Exception ex)
        {
            System.out.println(ex);
        }
        finally 
        {
            listener.close();
        }

    }

    private static void ParseConfig()
    {
        Properties props = new Properties();
        InputStream in;
        try {
            in = new FileInputStream(ClassLoader.getSystemClassLoader().getResource("config").getPath());
            props.load(in);

            //read and populate properties
            ProxyClient.accEndpoint = props.getProperty("AccEndpoint");
            ProxyClient.mongoPort = Integer.parseInt(props.getProperty("MongoPort"));
            ProxyClient.proxyPort = Integer.parseInt(props.getProperty("ProxyPort"));
            ProxyClient.maxNumberofCursors = Integer.parseInt(props.getProperty("MaxNumberofCursors"));
            ProxyClient.cursorExpiryAgeInMins = Integer.parseInt(props.getProperty("CursorExpiryAgeInMins"));           
            
            
            in.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }       
    }
    
    private static void PrintConfig()
    {
        System.out.println("Summary:");
        System.out.println("---------------------------------------------------------------------");
        System.out.println("Endpoint: "+ProxyClient.accEndpoint+":"+ProxyClient.mongoPort);
        System.out.println("Proxy running at: localhost:"+ProxyClient.proxyPort);
        System.out.println("Cursor map size:"+ProxyClient.maxNumberofCursors);
        System.out.println("Cursor expiry duration:"+ProxyClient.cursorExpiryAgeInMins+ " mins");
        System.out.println("---------------------------------------------------------------------");
        System.out.println();
        System.out.println("CursorProxyClient  starting...");
    }
}
