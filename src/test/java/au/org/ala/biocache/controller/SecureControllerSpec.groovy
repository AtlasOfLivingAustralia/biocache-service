package au.org.ala.biocache.controller

import au.org.ala.biocache.web.AbstractSecureController
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import spock.lang.Specification

class SecureControllerSpec extends Specification {

    AbstractSecureController secureController = new AbstractSecureController()

    def setup() {

    }

    def 'rate limiting request'() {

        setup:
        secureController.acceptNetworks = [ '10.0.0.0/8', '192.168.0.0/16' ]
        secureController.afterPropertiesSet()

        MockHttpServletRequest request = new MockHttpServletRequest()
        MockHttpServletResponse response = new MockHttpServletResponse()

        when:
        request.removeAllParameters()
        request.addParameter('email', 'test@example.com')

        then:
        !secureController.rateLimitRequest(request, response)

        when:
        request.removeAllParameters()
        request.addParameter('apiKey', 'XXXXX')

        then:
        !secureController.rateLimitRequest(request, response)

        when:
        request.removeAllParameters()
        request.addParameter('ip', '10.0.0.1')

        then:
        !secureController.rateLimitRequest(request, response)

        when:
        request.removeAllParameters()
        request.addParameter('ip', '192.168.0.1')

        then:
        !secureController.rateLimitRequest(request, response)

        when:
        request.removeAllParameters()
        request.addParameter('ip', '172.16.0.1')

        then:
        secureController.rateLimitRequest(request, response)
    }


}
