import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class MongoConnection {
    private Socket socket;
    private InputStream inStream;
    private OutputStream outStream;

    public MongoConnection(Socket socket, InputStream instream, OutputStream outstream)
    {
        this.socket = socket;
        this.inStream = instream;
        this.outStream = outstream;
    }

    public static MongoConnection FromIncomingSocket(Socket socket)
    {
        InputStream instream = null;
        OutputStream outstream = null;
        try 
        {
            instream = socket.getInputStream();
            outstream = socket.getOutputStream();
        } 
        catch (IOException e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new MongoConnection(socket, instream, outstream);
    }

    public static MongoConnection FromOutgoingSocket(Socket socket)
    {
        InputStream instream = null;
        OutputStream outstream = null;
        try 
        {
            instream = socket.getInputStream();
            outstream = socket.getOutputStream();

        } 
        catch (IOException e) 
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new MongoConnection(socket, instream, outstream);
    }

    public byte[] Read(int maxBytesToRead)
    {
        return TryRead(this.inStream, maxBytesToRead);
    }

    private byte[] TryRead(InputStream stream, int maxBytesToRead)
    {
        byte[]  buffer = new byte[maxBytesToRead];
        try 
        {
            int offset = 0;
            int bytesRead = 0;
            do
            {
                bytesRead = stream.read(buffer, offset, maxBytesToRead- offset);
                offset += bytesRead;				
            }while(offset < maxBytesToRead && bytesRead > 0);
        } 
        catch (IOException e) 
        {
            // TODO Auto-generated catch block
            //e.printStackTrace();
        }

        return buffer;
    }

    public void Write(byte[] buffer, int offset, int length)
    {
        TryWrite(this.outStream, buffer, offset, length);
    }

    private void TryWrite(OutputStream stream, byte[] buffer, int offset, int length)
    {
        try 
        {
            stream.write(buffer, offset, length);
        } 
        catch (IOException e) 
        {
            // TODO Auto-generated catch block
            //e.printStackTrace();
        }
    }

}
