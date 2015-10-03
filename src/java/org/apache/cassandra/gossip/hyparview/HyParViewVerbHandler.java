package org.apache.cassandra.gossip.hyparview;

import java.io.IOException;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.StorageService;

public class HyParViewVerbHandler implements IVerbHandler<HyParViewMessage>
{
    public void doVerb(MessageIn<HyParViewMessage> message, int id) throws IOException
    {
        StorageService.instance.gossipContext.hyparviewService.receiveMessage(message.payload);
    }
}
