/*
 * MIT License
 *
 * Copyright (c) 2017 Kungliga Tekniska högskolan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
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