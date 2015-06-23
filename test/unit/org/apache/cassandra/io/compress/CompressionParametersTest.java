package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.commitlog.EncryptionContextGenerator;
import org.apache.cassandra.io.util.ByteBufferDataInput;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.EncryptionContext;

public class CompressionParametersTest
{
    @Test
    public void serializationRoundTrip_WithChunkLength() throws IOException
    {
        serialize(new CompressionParameters(LZ4Compressor.class.getName(), CompressionParameters.DEFAULT_CHUNK_LENGTH, Collections.emptyMap()));
    }

    private void serialize(CompressionParameters parameters) throws IOException
    {
        DataOutputBuffer outBuffer = new DataOutputBuffer();
        CompressionParameters.serializer.serialize(parameters, outBuffer, MessagingService.current_version);
        ByteBuffer buf = outBuffer.buffer();

        ByteBufferDataInput inBuffer = new ByteBufferDataInput(buf, null, 0, 0);
        CompressionParameters outParams = CompressionParameters.serializer.deserialize(inBuffer, MessagingService.current_version);
        Assert.assertEquals(parameters, outParams);
    }

    @Test
    public void serializationRoundTrip_WithoutChunkLength() throws IOException
    {
        serialize(new CompressionParameters(LZ4Compressor.class.getName(), null, Collections.emptyMap()));
    }

    @Test
    public void serializationRoundTrip_Encryption() throws IOException
    {
        EncryptionContext encryptionContext = EncryptionContextGenerator.createContext(false);
        Map<String, String> encCompressorMap = encryptionContext.toHeaderParameters();
        encCompressorMap.put(EncryptionContext.ENCRYPTION_IV, "this would be an IV in the real world, but a string is good enough for this test");

        serialize(new CompressionParameters(EncryptingCompressor.class.getName(), CompressionParameters.DEFAULT_CHUNK_LENGTH, encCompressorMap));
    }
}
