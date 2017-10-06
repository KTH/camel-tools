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

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.util.ExchangeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.infosys.camel.ug.UgMessage;

/**
 * A Camel Processor filter to use in order to prevent out of sequence updates 
 * for UgObjects to pass through, overriding previous updates with older data.
 * 
 * Example of use:
 *
 * <pre>
 * &lt;bean id="ugUpdateOutOfOrderFilter" class="se.kth.infosys.camel.UgVersionOutOfOrderFilter"&gt;
 *   &lt;param name="size" value="10000" /&gt;
 * &lt;/bean&gt;
 * ...
 * &lt;process ref="ugUpdateOutOfOrderFilter"/&gt;
 * </pre>
 */
public class UgVersionOutOfOrderFilter implements Processor {
    private static final Logger LOG = LoggerFactory.getLogger(UgVersionOutOfOrderFilter.class);
    private final FifoCache<String, Long> cache = new FifoCache<>();

    /**
     * {@inheritDoc}
     */
    public void process(Exchange exchange) throws Exception {
        Message in = exchange.getIn();

        final String operation = in.getHeader(UgMessage.Header.Operation, String.class);

        if (operation == null ||
                ! (operation.equals(UgMessage.Operation.Update) ||
                        operation.equals(UgMessage.Operation.Delete))) {
            return;
        }

        final long version = ExchangeHelper.getMandatoryHeader(exchange, UgMessage.Header.Version, Long.class);
        final String kthid = ExchangeHelper.getMandatoryHeader(exchange, UgMessage.Header.Kthid, String.class);

        if (cache.get(kthid) != null && (version < cache.get(kthid))) {
            LOG.warn("Operation {} for {} version {} out of order, dropping.", operation, kthid, version);
            exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
        } else {
            cache.put(kthid, version);
        }
    }

    /**
     * Set number of UG object updates to keep in cache to match updates 
     * against, default 1000.
     *
     * @param size the number of items to keep in cache.
     */
    public void setSize(int size) {
        cache.setSize(size);
    }
}
