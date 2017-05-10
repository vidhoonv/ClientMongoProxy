import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public class MongoQueryHeader {

    public int Flags;
    public String FullCollectionName;
    public int NumberToSkip;
    public int NumberToReturn;

    public BsonDocument Query;
    public BsonDocument ReturnFieldsSelector;

    public MongoQueryHeader(byte[] bodyBytes)
    {
        this.Query = null;
        this.ReturnFieldsSelector = null;
        this.Initialize(bodyBytes);
    }

    public MongoQueryHeader(int flags, String collName, int numSkip, int numReturn, BsonDocument query, BsonDocument returnFieldsSelector)
    {
        this.Flags = flags;
        this.FullCollectionName = collName;
        this.NumberToSkip = numSkip;
        this.NumberToReturn = numReturn;
        this.Query = query;
        this.ReturnFieldsSelector = returnFieldsSelector;
    }

    public byte[] Serialize()
    {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        int capacity = 4+4+4+this.FullCollectionName.getBytes().length+1;
        ByteBuffer bb = ByteBuffer.allocate(capacity);


        byte[] fields = null;
        try {
            bb.order(ByteOrder.BIG_ENDIAN);
            bb.putInt(this.Flags);
            int k=0;
            while(k<this.FullCollectionName.length())
            {
                bb.put((byte)this.FullCollectionName.charAt(k));
                k++;
            }

            bb.put((byte)0); //append '/0'

            bb.putInt(this.NumberToSkip);
            bb.putInt(this.NumberToReturn);


            bb.flip();
            fields = new byte[bb.remaining()];
            bb.get(fields, 0, bb.remaining());

            BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
            new BsonDocumentCodec().encode(writer, this.Query,  EncoderContext.builder().isEncodingCollectibleDocument(true).build());


            if(this.ReturnFieldsSelector != null)
            {
                new BsonDocumentCodec().encode(writer, this.ReturnFieldsSelector, EncoderContext.builder().build());
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        byte[] docs = outputBuffer.toByteArray();


        byte[] destination = new byte[fields.length + docs.length];

        System.arraycopy(fields, 0, destination,  0, fields.length);
        System.arraycopy(docs, 0, destination, fields.length, docs.length);

        return destination;
    }

    private void Initialize(byte[] buffer)
    {        
        try
        {
            ByteBuffer byteBuf = ByteBuffer.wrap(buffer);
            byteBuf.order(ByteOrder.LITTLE_ENDIAN);
            this.Flags = byteBuf.getInt();

            this.FullCollectionName = MongoObject.ParseCString(byteBuf);

            this.NumberToSkip = byteBuf.getInt();

            this.NumberToReturn = byteBuf.getInt();

            BsonReader breader = new BsonBinaryReader(byteBuf);
            this.Query = new BsonDocumentCodec().decode(breader, DecoderContext.builder().build());

            if (byteBuf.hasRemaining())
            {

                BsonReader breader1 = new BsonBinaryReader(byteBuf);
                this.ReturnFieldsSelector = new BsonDocumentCodec().decode(breader1, DecoderContext.builder().build());
            }
        }
        catch (Exception e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
