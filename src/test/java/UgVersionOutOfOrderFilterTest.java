

import org.apache.camel.EndpointInject;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.CamelSpringRunner;
import org.apache.camel.test.spring.CamelTestContextBootstrapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.BootstrapWith;
import org.springframework.test.context.ContextConfiguration;

@RunWith(CamelSpringRunner.class)
@BootstrapWith(CamelTestContextBootstrapper.class)
@ContextConfiguration()
public class UgVersionOutOfOrderFilterTest extends UgCamelTestBase {
    @EndpointInject(uri = "mock:end")
    protected MockEndpoint mockEnd;

    @Test
    public void testReceived() throws Exception {
        mockEnd.assertExchangeReceived(1);
        MockEndpoint.assertIsSatisfied(camelContext);
    }
}
