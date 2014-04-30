package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Set;

public class JasonsDelegateFileSystem extends FileSystem
{
    private final FileSystemProvider provider;
    private final FileSystem delegate;

    protected JasonsDelegateFileSystem(JasonsDelegateFileSystemProvider provider, FileSystem delegate)
    {
        super();
        this.provider = provider;
        this.delegate = delegate;
    }

    public FileSystemProvider provider()
    {
        return provider;
    }

    public void close() throws IOException
    {
        delegate.close();
    }

    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    public boolean isReadOnly()
    {
        return delegate.isReadOnly();
    }

    public String getSeparator()
    {
        return delegate.getSeparator();
    }

    public Iterable<Path> getRootDirectories()
    {
        return delegate.getRootDirectories();
    }

    public Iterable<FileStore> getFileStores()
    {
        return delegate.getFileStores();
    }

    public Set<String> supportedFileAttributeViews()
    {
        return delegate.supportedFileAttributeViews();
    }

    public Path getPath(String first, String... more)
    {
        return new JasonsDelegatePath(this, delegate.getPath(first, more));
    }

    public PathMatcher getPathMatcher(String syntaxAndPattern)
    {
        return delegate.getPathMatcher(syntaxAndPattern);
    }

    public UserPrincipalLookupService getUserPrincipalLookupService()
    {
        return delegate.getUserPrincipalLookupService();
    }

    public WatchService newWatchService() throws IOException
    {
        return delegate.newWatchService();
    }
}
