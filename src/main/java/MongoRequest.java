import java.util.Map;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

public class MongoRequest extends MongoObject {

    public static  String MongoCommandCollectionName = "$cmd";
    public MongoQueryHeader QueryHeader;
    public MongoGetMoreHeader GetMoreHeader;

    public boolean RequestToCommand;

    public MongoRequest(MongoStandardHeader header, byte[] hbytes, byte[] body)     
    {
        super(hbytes, header, body);
        this.QueryHeader = null;
        this.GetMoreHeader = null;
        this.RequestToCommand = false;
        this.Initialize();
    }

    public boolean ShouldIntercept()
    {
        switch(this.StandardHeader.MongoOpCode)
        {
        case 2004: //OP_QUERY
            if(this.QueryHeader.FullCollectionName.contains(MongoRequest.MongoCommandCollectionName))
            {
                //command mode
                if(this.QueryHeader.Query.containsKey("find")) //query in command mode
                {
                    System.out.println("Intercepting Command: "+ this.QueryHeader.Query);
                    return true;
                }
                else if(this.QueryHeader.Query.containsKey("getMore")) //getMore in command mode
                {
                    System.out.println("Intercepting Command: "+ this.QueryHeader.Query);
                    return true;
                }
                else
                {
                    System.out.println("Not Intercepting Command: "+ this.QueryHeader.Query);
                    return false; //dont intercept other commands
                }
            }
            else
            {
                System.out.println("Intercepting Request: OP_QUERY");
                return true; //intercept OP_QUERY
            }								
        case 2005: //OP_GET_MORE
            System.out.println("Intercepting Request: OP_GET_MORE");
            return true; //intercept getMore requests
        default:
            return false; //otherwise no intercept
        }
    }

    public static MongoRequest TransformRequest(
            MongoRequest request, 
            Map<Long, BsonDocument> cursorMap)
    {
        if(request.StandardHeader.MongoOpCode ==  2004) //OP_QUERY
        {
            if(request.QueryHeader.FullCollectionName.contains(MongoRequest.MongoCommandCollectionName))
            {
                if(request.QueryHeader.Query.containsKey("find")) //query in command mode
                {                    
                    //change "find" command to "findWithContinuationToken"
                    BsonString collName = request.QueryHeader.Query.getString("find");
                    request.QueryHeader.Query.append("findWithContinuationToken", collName);
                    request.QueryHeader.Query.remove("find");
                    return request;
                }
                else if(request.QueryHeader.Query.containsKey("getMore")) //getmore in command mode
                {
                    //append the full cursor doc for cursorID from getMore command
                    long cursorId = request.QueryHeader.Query.get("getMore").asInt64().longValue();
                    long orgCursorId = 0;

                    BsonDocument fullcursorDoc = null;
                    if(cursorMap.containsKey(cursorId))
                    {
                        fullcursorDoc = cursorMap.get(cursorId);
                        //DEBUG:
                        //System.out.println("retrived cursor doc: "+ fullcursorDoc);
                        //replace unique cursorid with org cursorid
                        orgCursorId = fullcursorDoc.get("cursorId").asInt64().getValue();
                        fullcursorDoc.replace("cursorId", new BsonInt64(orgCursorId));
                    }
                    else
                    {
                        System.out.println("cursorID not found in cursormap");
                    }

                    request.QueryHeader.Query.append("fullCursor", fullcursorDoc);
                    return request;
                }
            }
            else
            {
                //OP_QUERY request
                //convert to find in command mode
                request.RequestToCommand = true;
                String dbName = MongoRequest.GetDBName(request.QueryHeader.FullCollectionName);
                String collName = MongoRequest.GetCollName(request.QueryHeader.FullCollectionName);
                request.QueryHeader.FullCollectionName = dbName + "." + MongoRequest.MongoCommandCollectionName;
                BsonDocument queryDoc = new BsonDocument();
                queryDoc.append("findWithContinuationToken", new BsonString(collName));
                
                if(request.QueryHeader.Query != null)
                {
                    //filter
                    if(request.QueryHeader.Query.containsKey("query"))
                    {
                        queryDoc.append("filter", request.QueryHeader.Query.get("query"));
                        //sort
                        if(request.QueryHeader.Query.containsKey("orderby"))
                        {
                            queryDoc.append("sort", request.QueryHeader.Query.get("orderby"));
                        }
                    }
                    else if(request.QueryHeader.Query.containsKey("$query"))
                    {
                        queryDoc.append("filter", request.QueryHeader.Query.get("$query"));
                        //sort
                        if(request.QueryHeader.Query.containsKey("$orderby"))
                        {
                            queryDoc.append("sort", request.QueryHeader.Query.get("$orderby"));
                        }
                    }
                    else if(request.QueryHeader.Query.containsKey("$snapshot"))
                    {
                        queryDoc.append("filter", new BsonDocument());
                    }
                    else
                    {
                        queryDoc.append("filter", request.QueryHeader.Query);
                    }
                }
                
                //projection
                if(request.QueryHeader.ReturnFieldsSelector != null)
                {
                    queryDoc.append("projection", request.QueryHeader.ReturnFieldsSelector);
                }
                
                //limit
                queryDoc.append("batchSize", new BsonInt32(Math.abs(request.QueryHeader.NumberToReturn)));
                if(request.QueryHeader.NumberToReturn<0)
                {
                    queryDoc.append("singleBatch", new BsonBoolean(true));
                }
                
                //skip
                queryDoc.append("skip", new BsonInt32(request.QueryHeader.NumberToSkip));                

                request.QueryHeader.Query =  queryDoc;
            }
        }
        else if(request.StandardHeader.MongoOpCode ==  2005) //OP_GET_MORE
        {
            request.RequestToCommand = true;
            //convert to getmore in command mode with full cursor appended
            request.StandardHeader.MongoOpCode = 2004; //change getmore to query
            MongoGetMoreHeader getMoreHeader = request.GetMoreHeader;
            request.GetMoreHeader = null; //erase get more header
            int flags = 0;
            int numReturn = getMoreHeader.NumberToReturn;
            int numSkip = 0;
            String dbName = MongoRequest.GetDBName(getMoreHeader.FullCollectionName);
            String collName = MongoRequest.GetCollName(getMoreHeader.FullCollectionName);
            long cursorId = getMoreHeader.cursorID;
            long orgCursorId = 0;

            //cursor ID
            BsonDocument queryDoc = new BsonDocument();
            queryDoc.append("getMore", new BsonInt64(cursorId));
            
            //collection name
            queryDoc.append("collection", new BsonString(collName));
            
            //batchsize
            queryDoc.append("batchSize", new BsonInt32(getMoreHeader.NumberToReturn));
            
            //append the full cursor doc for cursorID from getMore command
            BsonDocument fullcursorDoc = null;
            if(cursorMap.containsKey(cursorId))
            {
                fullcursorDoc = cursorMap.get(cursorId);
                //replace unique cursorid with org cursorid
                orgCursorId = fullcursorDoc.get("cursorId").asInt64().getValue();
                fullcursorDoc.replace("cursorId", new BsonInt64(orgCursorId));
            }
            else
            {
                System.out.println("cursorID not found in cursormap");
            }

            queryDoc.append("fullCursor", fullcursorDoc);
            request.QueryHeader = new MongoQueryHeader(flags,  dbName+"."+MongoRequest.MongoCommandCollectionName, numSkip, numReturn, queryDoc, null);            

            return request;
        }

        return request;
    }

    public byte[] Serialize()
    {

        if(this.QueryHeader != null)
        {
            this.Body = this.QueryHeader.Serialize();
        }
        else if(this.GetMoreHeader != null)
        {
            this.Body = this.GetMoreHeader.Serialize();
        }

        this.StandardHeader.Length = this.Body.length + MongoStandardHeader.MongoHeaderSize;

        this.Header = this.StandardHeader.Serialize();	

        byte[] destination = new byte[this.Header.length + this.Body.length];
        System.arraycopy(this.Header, 0, destination, 0, this.Header.length);
        System.arraycopy(this.Body, 0, destination,  this.Header.length, this.Body.length);

        return destination;
    }

    private void Initialize()
    {
        switch(this.StandardHeader.MongoOpCode)
        {
        case 2004://OP_QUERY
            this.QueryHeader = new MongoQueryHeader(this.Body);
            break;
        case 2005://OP_GET_MORE
            this.GetMoreHeader = new MongoGetMoreHeader(this.Body);
        }
    }

    private static String GetDBName(String fullName)
    {
        String dbName = null;
        int dot = fullName.indexOf('.');
        dbName = fullName.substring(0, dot);
        return dbName;		
    }

    private static String GetCollName(String fullName)
    {
        String cName = null;
        int dot = fullName.indexOf('.');
        cName = fullName.substring(dot+1, fullName.length());
        return cName;		
    }

}
