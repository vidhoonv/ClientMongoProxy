import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;

public class MongoResponse extends MongoObject {
    public  int OpCode;
    public MongoReplyHeader Reply;

    public MongoResponse(MongoStandardHeader header, byte[] headerBytes, byte[] body)
    {
        super(headerBytes, header, body);
        this.Initialize();
    }

    public static MongoResponse TransformResponse(
            UUID connId,
            MongoResponse response, 
            Map<Long, BsonDocument> cursorMap,
            Map<UUID, List<Long>> connCursorMap,
            Map<Long, LocalDateTime> cursorTime,
            boolean isReqConvertedToCommand)
    {
        if(response.OpCode ==  1)
        {//OP_REPLY
            long uniqueCursorId = 0;
            BsonDocument responseDoc = response.Reply.documents[0];
            if(responseDoc != null)
            {
                BsonDocument fullcursorDoc= null;

                if(responseDoc.containsKey("fullCursor"))
                {
                    fullcursorDoc = responseDoc.getDocument("fullCursor");

                    //remove fullcursor from response
                    responseDoc.remove("fullCursor");

                    uniqueCursorId = MongoForwarder.GetUniqueCursorID();
                    //add to map
                    cursorMap.put(uniqueCursorId, fullcursorDoc);

                    cursorTime.put(uniqueCursorId, LocalDateTime.now());					

                    if(connCursorMap.containsKey(connId))
                    {
                        if(!connCursorMap.get(connId).contains(uniqueCursorId))
                        {
                            connCursorMap.get(connId).add(uniqueCursorId);
                        }
                    }

                    //replace cursor id returned from mongo server
                    //with generated unique cursor id
                    if(responseDoc.containsKey("cursor"))
                    {
                        BsonDocument cursorDoc = responseDoc.getDocument("cursor");
                        if(cursorDoc.containsKey("id"))
                        {							
                            cursorDoc.replace("id", new BsonInt64(uniqueCursorId));
                            responseDoc.replace("cursor", cursorDoc);
                        }
                    }

                    response.Reply.documents[0] = responseDoc;

                }

                if(isReqConvertedToCommand)
                {
                    if(responseDoc.containsKey("cursor"))
                    {
                        BsonDocument cursorDoc = responseDoc.getDocument("cursor");
                        BsonArray docs = null;
                        if(cursorDoc.containsKey("firstBatch"))
                        {
                            docs = cursorDoc.getArray("firstBatch");
                        }
                        else if(cursorDoc.containsKey("nextBatch"))
                        {
                            docs = cursorDoc.getArray("nextBatch");
                        }

                        List<BsonDocument> dlist = new ArrayList<BsonDocument>();
                        int len = docs.size();
                        for(int i=0;i<len;i++)
                        {
                            dlist.add(docs.get(i).asDocument());
                        }

                        response.Reply.documents = new BsonDocument[dlist.size()];
                        for(int j=0;j<len;j++)
                        {
                            response.Reply.documents[j] = dlist.get(j);
                        }
                        response.Reply.numberReturned = len;
                        response.Reply.cursorId = uniqueCursorId;

                    }

                }
            }			
        }

        return response;
    }

    public byte[] Serialize()
    {


        if(this.Reply != null)
        {
            this.Body = this.Reply.Serialize();
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
        this.OpCode =  this.StandardHeader.MongoOpCode;
        switch(this.OpCode)
        {
        case 1: //OP_REPLY
            this.Reply = new MongoReplyHeader(this.Body);
            break;
        }
    }
}
