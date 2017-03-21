package se.kth.infosys.camel;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A minimal FifoCache class built on top of LinkedHashMap. It stores objects up
 * to specified cache size (default 1000). When the size is reached eldest object
 * will be evicted from the cache on subsequent injects. The size can be changed
 * at runtime.
 * 
 * @param <K> key in the cache.
 * @param <V> value in the cache.
 */
@SuppressWarnings("serial")
public class FifoCache<K,V> extends LinkedHashMap<K,V> {
    private int size = 1000;

    /**
     * {@inheritDoc} 
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        return size() > size;
    }

    /**
     * Set the size of the FIFO cache.
     * 
     * @param size the size of the cache.
     */
    protected void setSize(int size) {
        this.size = size;
    }

    /**
     * Get the current size of the FIFO cache.
     * 
     * @return the current size.
     */
    protected int getSize() {
        return size;
    }
}