package au.org.ala.biocache.controller


import au.org.ala.biocache.web.AbstractSecureController
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification
import spock.lang.Unroll

@ContextConfiguration(locations = "classpath:springTest.xml")
class SecureControllerSpec extends Specification {

    AbstractSecureController secureController = new AbstractSecureController()

    @Unroll
    def 'rate limiting request #ip'() {

        setup:
        secureController.excludedNetworks = ['10.0.0.0/8', '192.168.0.0/16' ]
        secureController.includedNetworks = ['10.1.0.0/16' ]

        MockHttpServletRequest request = new MockHttpServletRequest()
        MockHttpServletResponse response = new MockHttpServletResponse()

        when:
        request.addHeader('X-Forwarded-For', ip)

        then:
        secureController.rateLimitRequest(request, response) == rateLimit

        where:
        ip              || rateLimit
        '10.0.0.1'      || false
        '192.168.0.1'   || false
        '172.16.0.1'    || true
        '10.1.0.1'      || true
    }


}
