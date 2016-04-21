package org.apache.nifi.remote.util;

import java.lang.Math;
import java.io.OutputStream;
import java.io.IOException;

import com.google.common.util.concurrent.RateLimiter;

public class ThrottledOutputStream extends OutputStream {
    private long rate = 0;
    private long bytesWritten = 0;
    private final OutputStream stream;
    private final RateLimiter rateLimiter;
    private final long startTime = System.currentTimeMillis();

    public ThrottledOutputStream(final OutputStream stream, final long rate) {
        this.rate = rate;
        this.stream = stream;
        this.rateLimiter = (rate > 0) ? RateLimiter.create((double) rate) : null;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void write(int b) throws IOException {
      if (rateLimiter != null) {
          rateLimiter.acquire();
      }
      stream.write(b);
      bytesWritten += 1;
    }

    public long getRate() {
        return rate;
    }

    public void setRate(final long rate) {
        this.rate = rate;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public long getBytesPerSecond() {
        final long currentTime = System.currentTimeMillis();
        final long elapsedTime = currentTime - startTime;
        final long bytesPerSecond = Math.round(getBytesWritten() / (elapsedTime / 1000.0));
        return (bytesPerSecond > 0) ? bytesPerSecond : 0;
    }
}
