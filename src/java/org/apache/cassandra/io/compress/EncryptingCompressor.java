package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogEncryptionUtils;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_CIPHER;
import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_IV;
import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_KEY_ALIAS;

/**
 * Custom {@code ICompressor} implementation that performs encryption in addition to compression.
 *
 * Compress then encrypt, or decrypt then decompress - that way you encrypt a smaller payload, instead of
 * compressing the encrypted array.
 */
public class EncryptingCompressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptingCompressor.class);

    private final Cipher cipher;
    private final boolean encrypting; //for paranoia checking
    private final ICompressor compressor;
    private ByteBuffer byteBuffer;
    private byte[] buffer;

    public EncryptingCompressor(Map<String, String> options)
    {
        this(options, DatabaseDescriptor.getEncryptionContext());
    }

    @VisibleForTesting
    EncryptingCompressor(Map<String, String> options, EncryptionContext baseEncryptionContext)
    {
        EncryptionContext encryptionContext = EncryptionContext.createFromMap(options, baseEncryptionContext);
        compressor = encryptionContext.getCompressor();
        String iv = options.get(ENCRYPTION_IV);
        try
        {
            if (iv != null)
            {
                cipher = encryptionContext.getDecryptor(Hex.hexToBytes(iv));
                encrypting = false;
            }
            else
            {
                cipher = encryptionContext.getEncryptor();
                encrypting = true;
            }
        }
        catch (IOException ioe)
        {
            throw new RuntimeException("unable to load cipher", ioe);
        }
    }

    public static EncryptingCompressor create(Map<String, String> compressionOptions)
    {
        return new EncryptingCompressor(compressionOptions);
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        int compressedInitialSize = compressor.initialCompressedBufferLength(chunkLength) + CommitLogEncryptionUtils.COMPRESSED_BLOCK_HEADER_SIZE;
        return cipher.getOutputSize(compressedInitialSize) + CommitLogEncryptionUtils.ENCRYPTED_BLOCK_HEADER_SIZE;
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        // a paranoia check to make sure "we haven't crossed the streams"
        if (!encrypting)
            throw new IllegalStateException("trying to perform action that is opposite to the init'ed cipher: cipher's state is decrypting, but we want to encrypt");

        byteBuffer = CommitLogEncryptionUtils.compress(input, byteBuffer, true, compressor);
        CommitLogEncryptionUtils.encrypt(byteBuffer, output, false, cipher);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        // a paranoia check to make sure "we haven't crossed the streams"
        if (encrypting)
            throw new IllegalStateException("trying to perform action that is opposite to the init'ed cipher: cipher's state is encrypting, but we want to decrypt");

        try
        {
            byteBuffer = CommitLogEncryptionUtils.decrypt(input, byteBuffer, true, cipher);
            CommitLogEncryptionUtils.uncompress(byteBuffer, output, false, compressor);
        }
        catch (BadPaddingException  | ShortBufferException | IllegalBlockSizeException e)
        {
            throw new IOException("failed to decrypt block", e);
        }
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        // a paranoia check to make sure "we haven't crossed the streams"
        if (encrypting)
            throw new IllegalStateException("trying to perform action that is opposite to the init'ed cipher: cipher's state is encrypting, but we want to decrypt");

        //TODO:JEB this is wrong - needs to account for (and read!) the buffer header lengths

        // size the encryption output
        int length = cipher.getOutputSize(inputLength);
        if (buffer == null || buffer.length < length)
            buffer = new byte[length];

        //decrypt the data into the member WrapperArray
        int decryptSize;
        try
        {
            decryptSize = cipher.doFinal(input, inputOffset, inputLength, buffer, 0);
        }
        catch (ShortBufferException sbe)
        {
            //should not happen as we size correctly based on the call to Cipher.getOutputSize()
            throw new IllegalStateException("failed to size byteBuffer correctly, even though we asked the cipher for the correct size");
        }
        catch (IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("problem while decrypting a block", e);
        }

        // hope like hell output byteBuffer is large enough 'cuz we can't resize it
        if (output.length - outputOffset < buffer.length)
        {
            String msg = String.format("buffer to uncompress into is not large enough; buf size = %d, buf offset = %d, target size = %s",
                                       output.length, outputOffset, buffer.length);
            throw new IllegalStateException(msg);
        }
        return compressor.uncompress(buffer, 0, decryptSize, output, outputOffset);
    }

    public Set<String> supportedOptions()
    {
        //not sure if we should expose the compressor's supportedOptions()
        Set<String> opts = new HashSet<>();
        opts.add(ENCRYPTION_CIPHER);
        opts.add(ENCRYPTION_KEY_ALIAS);
        opts.add(ENCRYPTION_IV);
        return opts;
    }

    public Map<String, String> compressionParameters()
    {
        Map<String, String> map = new HashMap<>(1);
        map.put(ENCRYPTION_IV, Hex.bytesToHex(cipher.getIV()));
        return map;
    }

    @VisibleForTesting
    public String getIv()
    {
        return Hex.bytesToHex(cipher.getIV());
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public BufferType preferredBufferType()
    {
        return BufferType.ON_HEAP;
    }
}
