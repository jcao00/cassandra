package org.apache.cassandra.gossip.thicket;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.StorageService;

public class ThicketVerbHandler implements IVerbHandler<ThicketMessage>
{
    public void doVerb(MessageIn<ThicketMessage> message, int id) throws IOException
    {
        StorageService.instance.gossipContext.thicketService.receiveMessage(message.payload);
    }
}
