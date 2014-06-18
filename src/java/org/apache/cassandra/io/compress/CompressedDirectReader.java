package org.apache.cassandra.io.compress;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.io.util.DirectReader;

import java.io.File;
import java.io.FileNotFoundException;

public class CompressedDirectReader extends DirectReader
{

    private final CompressionMetadata metadata;

    protected CompressedDirectReader(File file, int bufferSize, CompressionMetadata metadata, RateLimiter limiter) throws FileNotFoundException
    {
        super(file, bufferSize, limiter);
        this.metadata = metadata;
    }

    public static CompressedDirectReader open(File file, int bufferSize, CompressionMetadata metadata)
    {
        return open(file, bufferSize, metadata, null);
    }

    public static CompressedDirectReader open(File file, int bufferSize, CompressionMetadata metadata, RateLimiter limiter)
    {
        try
        {
            return new CompressedDirectReader(file, bufferSize, metadata, limiter);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }


}
