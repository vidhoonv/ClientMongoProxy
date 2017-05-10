import java.nio.ByteBuffer;

public class MongoObject {

    public MongoStandardHeader StandardHeader;

    public byte[] Header;

    public byte[] Body;

    public MongoObject(byte[] headerbytes, MongoStandardHeader header, byte[] body)
    {
        this.StandardHeader = header;
        this.Header = headerbytes;
        this.Body = body;
    }

    public static String ParseCString(ByteBuffer byteBuf)
    {

        StringBuilder s = new StringBuilder();
        try {
            char nextChar =  (char)byteBuf.get();
            while(nextChar != 0)
            {
                s.append(nextChar);
                nextChar = (char)byteBuf.get();
            }


        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return s.toString(); 
    }

}
