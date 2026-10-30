package com.berker.db.storage;

/**
 * Every frame of the BufferPool is pinned, so there is no room to bring in another page right
 * now. It may succeed later, once other users unpin their pages.
 *
 * <p>A separate type (instead of a plain IllegalStateException) so that callers who want to
 * wait and retry can catch exactly this case. If it happens with only one user, it usually
 * means a pin leak: a {@link PageGuard} that was never closed.
 */
public final class BufferPoolFullException extends RuntimeException {

    public BufferPoolFullException(int frameCount) {
        super("no available buffer at the moment: all " + frameCount + " frames are pinned");
    }
}
