package se.kth.infosys.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.util.ExchangeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.infosys.camel.ug.internal.UgMessage;

/**
 * A filter to use in order to prevent out of sequence updates for UgObjects 
 * to pass through, overriding previous updates with older data.
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

        if (cache.get(kthid) != null && (version <= cache.get(kthid))) {
            LOG.warn("Operation {} for {} version {} out of order, dropping.", operation, kthid, version);
            exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
        }

        cache.put(kthid, version);
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
