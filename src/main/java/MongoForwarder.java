import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.bson.BsonDocument;

public class MongoForwarder extends Thread {

    private MongoConnection incomingConnection;
    private MongoConnection outgoingConnection;
    private UUID connId;
    private static Map<Long, BsonDocument> cursorMap = new HashMap<Long, BsonDocument>();
    private static Map<Long, LocalDateTime> cursorTime = new HashMap<Long, LocalDateTime>();
    private static Map<UUID, List<Long>> connCursorMap = new HashMap<UUID, List<Long>>();	
    private static long UniqueCursorID = 0;

    
    public static boolean Cleaning = false;

    private static synchronized void IncrementUniqueCursorID() {
        MongoForwarder.UniqueCursorID++;
    }

    public static long GetUniqueCursorID()
    {
        MongoForwarder.IncrementUniqueCursorID();
        return MongoForwarder.UniqueCursorID;
    }

    public MongoForwarder(MongoConnection inConn, MongoConnection outConn, UUID connId)
    {
        this.incomingConnection = inConn;
        this.outgoingConnection = outConn;
        this.connId = connId;
        connCursorMap.put(this.connId, new ArrayList<Long>());
    }

    public void run() 
    {
        try 
        {
            MongoRequest request = MongoForwarder.ReadRequest(this.incomingConnection);
            MongoRequest outRequest = request;
            boolean shouldIntercept = outRequest.ShouldIntercept();
            if(shouldIntercept)
            {
                outRequest = MongoRequest.TransformRequest(request,  MongoForwarder.cursorMap);
            }

            while(outRequest != null)
            {
                boolean continueLoop = MongoForwarder.WriteRequest(this.outgoingConnection, outRequest, shouldIntercept);
                if(!continueLoop) break;

                if(MongoForwarder.IsResponseRequired(outRequest.StandardHeader.MongoOpCode))
                {
                    //read response only if response is required from previous request
                    MongoResponse response = MongoForwarder.ReadResponse(this.outgoingConnection);
                    if(response == null) break;

                    MongoResponse inResponse = response;
                    if(shouldIntercept)
                    {
                        inResponse = MongoResponse.TransformResponse(
                                this.connId,
                                response, 
                                MongoForwarder.cursorMap,
                                MongoForwarder.connCursorMap,
                                MongoForwarder.cursorTime,
                                request.RequestToCommand);
                    }

                    continueLoop = MongoForwarder.WriteResponse(this.incomingConnection, inResponse, shouldIntercept);
                    if(!continueLoop) break;
                }      		

                if(cursorMap.size() > ProxyClient.maxNumberofCursors && !MongoForwarder.Cleaning)
                {        			
                    Thread cleanupThread = new Thread(new Cleanup());
                    cleanupThread.start();					
                }

                request = MongoForwarder.ReadRequest(this.incomingConnection);
                if(request == null)
                {        			 
                    break;
                }
                outRequest = request;
                shouldIntercept = request.ShouldIntercept();
                if(shouldIntercept)
                {
                    outRequest = MongoRequest.TransformRequest(request, MongoForwarder.cursorMap);
                }
            }        	 
        }
        catch (Exception e) 
        {
            log("Error handling client# " + this.connId + ": " + e);
        } 
        finally 
        {
            try 
            {
                //connection is terminated by Client
                //cleanup
                MongoForwarder.Cleanup(this);
            } 
            catch (Exception e) 
            {
                log("Couldn't close a socket, what's going on?");
            }

            log("Connection with client# " + this.connId + " closed");
        }
    }

    public static MongoRequest ReadRequest(MongoConnection connection)
    {
        try
        {
            byte[] headerBytes = connection.Read(MongoStandardHeader.MongoHeaderSize);
            MongoStandardHeader header = MongoStandardHeader.GetFromBuffer(headerBytes);

            if(header.Length == 0)
            {
                return null;
            }

            byte[] body = connection.Read(header.Length - MongoStandardHeader.MongoHeaderSize);             
            return new MongoRequest(header, headerBytes, body);
        }
        catch (Exception e)
        {
            return null;
        }
    }


    public static MongoResponse ReadResponse(MongoConnection connection)
    {
        try
        {
            byte[] headerBytes = connection.Read(MongoStandardHeader.MongoHeaderSize);
            MongoStandardHeader header = MongoStandardHeader.GetFromBuffer(headerBytes);

            if(header.Length == 0)
            {
                return null;
            }

            byte[] body = connection.Read(header.Length - MongoStandardHeader.MongoHeaderSize);             
            return new MongoResponse(header, headerBytes, body);
        }
        catch (Exception e)
        {
            return null;
        }
    }


    public static boolean WriteRequest(MongoConnection connection, MongoRequest request, boolean serialize)
    {
        try
        {
            if(serialize)
            {
                byte[] requestBytes = request.Serialize();
                connection.Write(requestBytes, 0, requestBytes.length);
            }
            else
            {
                connection.Write(request.Header, 0, request.Header.length);
                connection.Write(request.Body, 0, request.Body.length);
            }

            return true;
        }
        catch(Exception e)
        {
            return false;
        }

    }

    public static boolean WriteResponse(MongoConnection connection, MongoResponse response, boolean serialize)
    {
        try
        {
            if(serialize)
            {
                byte[] responseBytes = response.Serialize();
                connection.Write(responseBytes, 0, responseBytes.length);
            }
            else
            {
                connection.Write(response.Header, 0, response.Header.length);
                connection.Write(response.Body, 0, response.Body.length);
            }
            return true;
        }
        catch(Exception e)
        {
            return false;
        }
    }

    public static void Cleanup(MongoForwarder forwarder)
    {
        //cleanup cursors of this client connection
        List<Long> cursorList = null;
        forwarder.log("Cleaning up client #: "+ forwarder.connId);

        if(MongoForwarder.connCursorMap.containsKey(forwarder.connId))
        {
            cursorList = MongoForwarder.connCursorMap.remove(forwarder.connId);
            for(long cid : cursorList)
            {
                forwarder.log("Cleanup: Removing cursorId #: "+ cid);
                cursorMap.remove(cid);
            }
        }
    }

    private static boolean IsResponseRequired(int opCode)
    {
        return (opCode == 2004 ||
                opCode == 2005);
    }

    private void log(String message) 
    {
        System.out.println(message);
    }

    private class Cleanup implements Runnable
    {
        public Cleanup()
        {

        }

        public void run() {
            // TODO Auto-generated method stub
            MongoForwarder.Cleaning = true;
            System.out.println("Cursor cleanup in progress...");

            LocalDateTime nw = LocalDateTime.now();
            for(Iterator<Entry<Long, LocalDateTime>> it = MongoForwarder.cursorTime.entrySet().iterator(); it.hasNext(); ) 
            {
                Entry<Long, LocalDateTime> entry = it.next();
                LocalDateTime ctime = entry.getValue();
                long cursor = entry.getKey();
                long mins = Duration.between(nw, ctime).toMinutes();
                if(mins>= ProxyClient.cursorExpiryAgeInMins)
                {
                    System.out.println("Removing old cursor... "+ cursor);
                    it.remove();
                    MongoForwarder.cursorMap.remove(cursor);
                }

            }

            System.out.println("Cursor cleanup is complete...");
            MongoForwarder.Cleaning = false;
        }

    }
}
