import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public class MongoReplyHeader {

    public static int MongoReplyHeaderSize = 4*3 + 8;
    public int reponseFlags;
    public long cursorId;
    public int startingFrom;
    public int numberReturned;
    public BsonDocument[] documents;

    public MongoReplyHeader(byte[] bodyBytes)
    {
        this.Initialize(bodyBytes);
    }

    public byte[] Serialize()
    {
        int capacity = 4+8+4+4;
        ByteBuffer bb = ByteBuffer.allocate(capacity);

        try {

            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.putInt(this.reponseFlags);
            bb.putLong(this.cursorId);
            bb.putInt(this.startingFrom);
            bb.putInt(this.numberReturned);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();


        for(int i=0;i<documents.length;i++)
        {
            BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
            BsonDocumentCodec codec = new BsonDocumentCodec();
            codec.encode(writer, documents[i], EncoderContext.builder().build());
        }

        bb.flip();
        byte[] fields  = new byte[bb.remaining()];
        bb.get(fields, 0, bb.remaining());

        byte[] docs = outputBuffer.toByteArray();


        byte[] destination = new byte[docs.length + fields.length];
        System.arraycopy(fields, 0, destination, 0, fields.length);
        System.arraycopy(docs, 0, destination, fields.length, docs.length);

        return destination;
    }

    private void Initialize(byte[] buffer)
    {
        ByteBuffer byteBuf = ByteBuffer.wrap(buffer);
        byteBuf.order(ByteOrder.LITTLE_ENDIAN);

        this.reponseFlags = byteBuf.getInt();
        this.cursorId = byteBuf.getLong();
        this.startingFrom = byteBuf.getInt();
        this.numberReturned = byteBuf.getInt();

        List<BsonDocument> docList = new ArrayList<BsonDocument>();

        for(int i=0;i<this.numberReturned;i++)
        {
            BsonReader breader = new BsonBinaryReader(byteBuf);
            BsonDocumentCodec codec = new BsonDocumentCodec();
            BsonDocument doc = codec.decode(breader, DecoderContext.builder().build());
            docList.add(doc);
        }

        this.documents = docList.toArray(new BsonDocument[0]);
    }

}
