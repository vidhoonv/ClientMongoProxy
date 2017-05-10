import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MongoStandardHeader {
    public static int MongoHeaderSize = 16;

    public int Length;
    public int RequestId;
    public int ResponseTo;
    public String OpCode;
    public int MongoOpCode;

    private static final Map<Integer,String> opCodeStrMap;
    static
    {
        Map<Integer, String> aMap = new HashMap<Integer, String>();
        aMap.put(0, "Unknown");
        aMap.put(1, "OP_REPLY");
        aMap.put(1000, "OP_MSG");
        aMap.put(2001, "OP_UPDATE");
        aMap.put(2002, "OP_INSERT");
        aMap.put(2003, "Reserved");
        aMap.put(2004, "OP_QUERY");
        aMap.put(2005, "OP_GET_MORE");
        aMap.put(2006, "OP_DELETE");
        aMap.put(2007, "OP_KILL_CURSORS");
        aMap.put(2010, "OP_COMMAND");
        aMap.put(2011, "OP_COMMANDREPLY");
        opCodeStrMap = Collections.unmodifiableMap(aMap);
    }


    public byte[] Serialize()
    {
        int capacity = 4+4+4+4;
        ByteBuffer bb = ByteBuffer.allocate(capacity);
        bb.order(ByteOrder.LITTLE_ENDIAN);

        try {
            bb.putInt(this.Length); 
            bb.putInt(this.RequestId);
            bb.putInt(this.ResponseTo);
            bb.putInt(this.MongoOpCode);

            bb.flip();
            byte[] destination = new byte[bb.remaining()];
            bb.get(destination, 0 , bb.remaining());

            return destination;
        }
        catch(Exception ex)
        {
            System.out.println(ex);
        }

        return null;
    }

    public static MongoStandardHeader GetFromBuffer(byte[] buffer)
    {
        MongoStandardHeader header = new MongoStandardHeader();
        ByteBuffer byteBuf = ByteBuffer.wrap(buffer);
        byteBuf.order( ByteOrder.LITTLE_ENDIAN);

        header.Length = byteBuf.getInt();
        header.RequestId = byteBuf.getInt();
        header.ResponseTo = byteBuf.getInt();
        header.MongoOpCode = byteBuf.getInt();
        header.OpCode = MongoStandardHeader.opCodeStrMap.get(header.MongoOpCode);

        return header;
    }




}
