package org.apache.cassandra.gms;

import java.util.concurrent.CyclicBarrier;

public class CustomGossiper extends Gossiper
{
    private CyclicBarrier barrier;

    public CustomGossiper()
    {
        super(false);
    }

    public void setBarrier(CyclicBarrier barrier)
    {
        this.barrier = barrier;
    }

    protected void startNextRound()
    {
        try
        {
            barrier.await();
            super.startNextRound();
        }
        catch (Exception e)
        {
            //nop
        }
    }
}
