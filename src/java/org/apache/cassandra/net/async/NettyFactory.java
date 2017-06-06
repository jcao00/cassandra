package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class NettyFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    /**
     * Default seed value for xxhash.
     */
    public static final int XXHASH_DEFAULT_SEED = 0x9747b28c;

    public enum Mode { MESSAGING, STREAMING }

    private static final String SSL_CHANNEL_HANDLER_NAME = "ssl";
    public static final String INBOUND_COMPRESSOR_HANDLER_NAME = "inboundCompressor";
    public static final String OUTBOUND_COMPRESSOR_HANDLER_NAME = "outboundCompressor";
    public static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    public static final String INBOUND_STREAM_HANDLER_NAME = "inboundStreamHandler";

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    private static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private static final boolean DEFAULT_USE_EPOLL = NativeTransportService.useEpoll();
    static
    {
        if (!DEFAULT_USE_EPOLL)
            logger.warn("epoll not availble {}", Epoll.unavailabilityCause());
    }

    /**
     * The size of the receive queue for the outbound channels. As outbound channels do not receive data
     * (outside of the internode messaging protocol's handshake), this value can be relatively small.
     */
    private static final int OUTBOUND_CHANNEL_RECEIVE_BUFFER_SIZE = 1 << 10;

    /**
     * The size of the send queue for the inbound channels. As inbound channels do not send data
     * (outside of the internode messaging protocol's handshake), this value can be relatively small.
     */
    private static final int INBOUND_CHANNEL_SEND_BUFFER_SIZE = 1 << 10;

    /**
     * A factory instance that all normal, runtime code should use. Separate instances should only be used for testing.
     */
    public static final NettyFactory instance = new NettyFactory(DEFAULT_USE_EPOLL);

    private final boolean useEpoll;
    private final EventLoopGroup acceptGroup;

    private final EventLoopGroup inboundGroup;
    private final EventLoopGroup outboundGroup;
    public final EventLoopGroup streamingGroup;

    /**
     * Constructor that allows modifying the {@link NettyFactory#useEpoll} for testing purposes. Otherwise, use the
     * default {@link #instance}.
     */
    @VisibleForTesting
    NettyFactory(boolean useEpoll)
    {
        this.useEpoll = useEpoll;
        acceptGroup = getEventLoopGroup(useEpoll, determineAcceptGroupSize(DatabaseDescriptor.getServerEncryptionOptions().internode_encryption),
                                        "MessagingService-NettyAcceptor-Threads", false);
        inboundGroup = getEventLoopGroup(useEpoll, FBUtilities.getAvailableProcessors(), "MessagingService-NettyInbound-Threads", false);
        outboundGroup = getEventLoopGroup(useEpoll, FBUtilities.getAvailableProcessors(), "MessagingService-NettyOutbound-Threads", true);
        streamingGroup = getEventLoopGroup(useEpoll, FBUtilities.getAvailableProcessors(), "Streaming-Netty-Threads", false);
    }

    /**
     * Determine the number of accept threads we need, which is based upon the number of listening sockets we will have.
     * We'll have either 1 or 2 listen sockets, depending on if we use SSL or not in combination with non-SSL. If we have both,
     * we'll have two sockets, and thus need two threads; else one socket and one thread.
     */
    static int determineAcceptGroupSize(InternodeEncryption internode_encryption)
    {
        return internode_encryption == InternodeEncryption.dc || internode_encryption == InternodeEncryption.rack ? 2 : 1;
    }

    /**
     * Create an {@link EventLoopGroup}, for epoll or nio. The {@code boostIoRatio} flag passes a hint to the netty
     * event loop threads to optimize comsuming all the tasks from the netty channel before checking for IO activity.
     * By default, netty will process some maximum number of tasks off it's queue before it will check for activity on
     * any of the open FDs, which basically amounts to checking for any incoming data. If you have a class of event loops
     * that that do almost *no* inbound activity (like cassandra's outbound channels), then it behooves us to have the
     * outbound netty channel consume as many tasks as it can before making the system calls to check up on the FDs,
     * as we're not expecting any incoming data on those sockets, anyways. Thus, we pass the magic value {@code 100}
     * to achieve the maximum consuption from the netty queue. (for implementation details, as of netty 4.1.8,
     * see {@link io.netty.channel.epoll.EpollEventLoop#run()}.
     */
    static EventLoopGroup getEventLoopGroup(boolean useEpoll, int threadCount, String threadNamePrefix, boolean boostIoRatio)
    {
        if (useEpoll)
        {
            logger.debug("using netty epoll event loop for pool prefix {}", threadNamePrefix);
            EpollEventLoopGroup eventLoopGroup = new EpollEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix));
            if (boostIoRatio)
                eventLoopGroup.setIoRatio(100);
            return eventLoopGroup;
        }

        logger.debug("using netty nio event loop for pool prefix {}", threadNamePrefix);
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix));
        if (boostIoRatio)
            eventLoopGroup.setIoRatio(100);
        return eventLoopGroup;
    }

    /**
     * Create a {@link Channel} that listens on the {@code localAddr}. This method will block while trying to bind to the address,
     * but it does not make a remote call.
     */
    public Channel createInboundChannel(InetSocketAddress localAddr, InboundInitializer initializer, int receiveBufferSize) throws ConfigurationException
    {
        String nic = FBUtilities.getNetworkInterface(localAddr.getAddress());
        logger.info("Starting Messaging Service on {} {}, encryption: {}",
                    localAddr, nic == null ? "" : String.format(" (%s)", nic), encryptionLogStatement(initializer.encryptionOptions));
        Class<? extends ServerChannel> transport = useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        ServerBootstrap bootstrap = new ServerBootstrap().group(acceptGroup, inboundGroup)
                                                         .channel(transport)
                                                         .option(ChannelOption.SO_BACKLOG, 500)
                                                         .childOption(ChannelOption.SO_KEEPALIVE, true)
                                                         .childOption(ChannelOption.TCP_NODELAY, true)
                                                         .childOption(ChannelOption.SO_REUSEADDR, true)
                                                         .childOption(ChannelOption.SO_SNDBUF, INBOUND_CHANNEL_SEND_BUFFER_SIZE)
                                                         .childHandler(initializer);

        if (receiveBufferSize > 0)
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveBufferSize);

        ChannelFuture channelFuture = bootstrap.bind(localAddr);

        if (!channelFuture.awaitUninterruptibly().isSuccess())
        {
            if (channelFuture.channel().isOpen())
                channelFuture.channel().close();

            Throwable failedChannelCause = channelFuture.cause();
            if (failedChannelCause.getMessage().contains("in use"))
            {
                throw new ConfigurationException(localAddr + " is in use by another process.  Change listen_address:storage_port " +
                                                 "in cassandra.yaml to values that do not conflict with other services");
            }
            // looking at the jdk source, solaris/windows bind failue messages both use the phrase "cannot assign requested address".
            // windows message uses "Cannot" (with a capital 'C'), and solaris (a/k/a *nux) doe not. hence we search for "annot" <sigh>
            else if (failedChannelCause.getMessage().contains("annot assign requested address"))
            {
                throw new ConfigurationException("Unable to bind to address " + localAddr
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            }
            else
            {
                throw new ConfigurationException("failed to bind to: " + localAddr, failedChannelCause);
            }
        }

        return channelFuture.channel();
    }

    public static class InboundInitializer extends ChannelInitializer<SocketChannel>
    {
        private final IInternodeAuthenticator authenticator;
        private final ServerEncryptionOptions encryptionOptions;

        public InboundInitializer(IInternodeAuthenticator authenticator, ServerEncryptionOptions encryptionOptions)
        {
            this.authenticator = authenticator;
            this.encryptionOptions = encryptionOptions;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (encryptionOptions != null)
            {
                SslContext sslContext = SSLFactory.getSslContext(encryptionOptions, true, true);
                SslHandler sslHandler = sslContext.newHandler(channel.alloc());
                logger.trace("creating inbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslHandler);            }

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast(HANDSHAKE_HANDLER_NAME, new InboundHandshakeHandler(authenticator));
        }
    }

    private String encryptionLogStatement(ServerEncryptionOptions options)
    {
        if (options == null)
            return "disabled";

        String encryptionType = OpenSsl.isAvailable() ? "openssl" : "jdk";
        return "enabled (" + encryptionType + ')';
    }

    /**
     * Create the {@link Bootstrap} for connecting to a remote peer. This method does <b>not</b> attempt to connect to the peer,
     * and thus does not block.
     */
    @VisibleForTesting
    public Bootstrap createOutboundBootstrap(OutboundConnectionParams params)
    {
        logger.debug("creating outbound bootstrap to peer {}, encryption: {}, coalesce: {}", params.connectionId.connectionAddress(),
                    encryptionLogStatement(params.encryptionOptions),
                     params.coalescingStrategy.isPresent() ? params.coalescingStrategy.get() : CoalescingStrategies.Strategy.DISABLED);
        Class<? extends Channel> transport = useEpoll ? EpollSocketChannel.class : NioSocketChannel.class;
        Bootstrap bootstrap = new Bootstrap().group(params.mode == Mode.MESSAGING ? outboundGroup : streamingGroup)
                              .channel(transport)
                              .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                              .option(ChannelOption.SO_KEEPALIVE, true)
                              .option(ChannelOption.SO_REUSEADDR, true)
                              .option(ChannelOption.SO_SNDBUF, params.sendBufferSize)
                              .option(ChannelOption.SO_RCVBUF, OUTBOUND_CHANNEL_RECEIVE_BUFFER_SIZE)
                              .option(ChannelOption.TCP_NODELAY, params.tcpNoDelay)
                              .option(ChannelOption.WRITE_BUFFER_WATER_MARK, params.waterMark)
                              .handler(new OutboundInitializer(params));
        bootstrap.localAddress(params.connectionId.local(), 0);
        bootstrap.remoteAddress(params.connectionId.connectionAddress());
        return bootstrap;
    }

    public static class OutboundInitializer extends ChannelInitializer<SocketChannel>
    {
        private final OutboundConnectionParams params;

        OutboundInitializer(OutboundConnectionParams params)
        {
            this.params = params;
        }

        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (params.encryptionOptions != null)
            {
                SslContext sslContext = SSLFactory.getSslContext(params.encryptionOptions, true, false);
                SslHandler sslHandler = sslContext.newHandler(channel.alloc());
                logger.trace("creating outbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslHandler);
            }

            if (NettyFactory.WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            pipeline.addLast(HANDSHAKE_HANDLER_NAME, new OutboundHandshakeHandler(params));
        }
    }

    public void close()
    {
        acceptGroup.shutdownGracefully();
        outboundGroup.shutdownGracefully();
        inboundGroup.shutdownGracefully();
        streamingGroup.shutdownGracefully();
    }

    /**
     * Determines if the {@link Channel} is using a secure communications transport (like SSL/TLS)
     */
    public static boolean isSecure(Channel channel)
    {
        return channel.pipeline().get(SslHandler.class) != null;
    }

    public static Lz4FrameEncoder createLz4Encoder()
    {
        return new Lz4FrameEncoder(lz4Factory(), false, 1 << 14, createXXHashChecksum());
    }

    private static Checksum createXXHashChecksum()
    {
        return XXHashFactory.fastestInstance().newStreamingHash32(XXHASH_DEFAULT_SEED).asChecksum();
    }

    public static LZ4Factory lz4Factory()
    {
        return LZ4Factory.fastestInstance();
    }

    public static Lz4FrameDecoder createLz4Decoder()
    {
        return new Lz4FrameDecoder(lz4Factory(), createXXHashChecksum());
    }
}
