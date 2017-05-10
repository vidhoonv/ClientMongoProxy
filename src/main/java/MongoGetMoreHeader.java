import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class MongoGetMoreHeader {

    public String FullCollectionName;
    public int NumberToReturn;
    public long cursorID;

    public MongoGetMoreHeader(byte[] bodyBytes)
    {
        this.Initialize(bodyBytes);
    }

    public byte[] Serialize()
    {
        int capacity = this.FullCollectionName.length()+1+4+8+4;
        ByteBuffer bb = ByteBuffer.allocate(capacity);


        try {
            bb.putInt(0); //zero reserved field
            int k=0;
            while(k<this.FullCollectionName.length())
            {
                bb.put((byte)this.FullCollectionName.charAt(k));
                k++;
            }

            bb.put((byte)0); //append '\0' char

            bb.putInt(this.NumberToReturn);
            bb.putLong(cursorID);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        bb.flip();
        byte[] dest = new byte[bb.remaining()];
        bb.get(dest, 0, bb.remaining());
        return dest;
    }

    private void Initialize(byte[] buffer)
    {
        try
        {
            ByteBuffer byteBuf = ByteBuffer.wrap(buffer);
            byteBuf.order(ByteOrder.LITTLE_ENDIAN);

            int zeroReserved = byteBuf.getInt();
            this.FullCollectionName = MongoObject.ParseCString(byteBuf);
            this.NumberToReturn = byteBuf.getInt();         
            this.cursorID = byteBuf.getLong();
        }
        catch (Exception e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
