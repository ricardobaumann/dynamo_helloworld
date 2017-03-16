package dynamo_helloworld;

import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Component
public class CompressHelper {


    public ByteBuffer compressString(String input) throws IOException {

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream os = new GZIPOutputStream(baos)) {

            os.write(input.getBytes("UTF-8"));
            os.close();
            byte[] compressedBytes = baos.toByteArray();

            ByteBuffer buffer = ByteBuffer.allocate(compressedBytes.length);
            buffer.put(compressedBytes, 0, compressedBytes.length);
            buffer.position(0); // Important: reset the position of the ByteBuffer to the beginning
            return buffer;
        }

    }

    public String uncompressString(ByteBuffer input) throws IOException {
        byte[] bytes = input.array();

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPInputStream is = new GZIPInputStream(bais)) {

            int chunkSize = 1024;
            byte[] buffer = new byte[chunkSize];
            int length = 0;
            while ((length = is.read(buffer, 0, chunkSize)) != -1) {
                baos.write(buffer, 0, length);
            }

            return new String(baos.toByteArray(), "UTF-8");

        }

    }
}
