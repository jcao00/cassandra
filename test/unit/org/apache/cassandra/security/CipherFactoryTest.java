package org.apache.cassandra.security;

import java.io.IOException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.TransparentDataEncryptionOptions;

public class CipherFactoryTest
{
    // http://www.gutenberg.org/files/4300/4300-h/4300-h.htm
    static final String ULYSSEUS = "Stately, plump Buck Mulligan came from the stairhead, bearing a bowl of lather on which a mirror and a razor lay crossed. " +
                                   "A yellow dressinggown, ungirdled, was sustained gently behind him on the mild morning air. He held the bowl aloft and intoned: " +
                                   "—Introibo ad altare Dei.";
    TransparentDataEncryptionOptions encryptionOptions;
    CipherFactory cipherFactory;

    @Before
    public void setup()
    {
        encryptionOptions = EncryptionContextGenerator.createEncryptionOptions();
        cipherFactory = new CipherFactory(encryptionOptions);
    }

    @Test
    public void roundTrip() throws IOException, BadPaddingException, IllegalBlockSizeException
    {
        Cipher encryptor = cipherFactory.getEncryptor(encryptionOptions.cipher, encryptionOptions.key_alias);
        byte[] original = ULYSSEUS.getBytes(Charsets.UTF_8);
        byte[] encrypted = encryptor.doFinal(original);

        Cipher decryptor = cipherFactory.getDecryptor(encryptionOptions.cipher, encryptionOptions.key_alias, encryptor.getIV());
        byte[] decrypted = decryptor.doFinal(encrypted);
        Assert.assertEquals(ULYSSEUS, new String(decrypted, Charsets.UTF_8));
    }
}
