package au.org.ala.biocache.controller

import au.org.ala.biocache.web.AbstractSecureController
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import spock.lang.Specification

class SecureControllerSpec extends Specification {

    AbstractSecureController secureController = new AbstractSecureController()

    def 'rate limiting request'() {

        setup:
        secureController.excludedNetworks = [ '10.0.0.0/8', '192.168.0.0/16' ]
        secureController.includedNetworks = [ '10.1.0.0/16' ]

        MockHttpServletRequest request = new MockHttpServletRequest()
        MockHttpServletResponse response = new MockHttpServletResponse()

        when:
        if (ip) { request.addParameter('ip', ip) }
        if (email) { request.addParameter('email', email) }
        if (apiKey) { request.addParameter('apiKey', apiKey) }

        then:
        secureController.rateLimitRequest(request, response) == rateLimit

        where:
        ip              | email                 | apiKey    || rateLimit
        null            | null                  | null      || true
        null            | 'test@example.com'    | null      || false
        null            | null                  | 'XXXXX'   || false
        '10.0.0.1'      | null                  | null      || false
        '192.168.0.1'   | null                  | null      || false
        '172.16.0.1'    | null                  | null      || true
        '10.1.0.1'      | null                  | null      || true
        '10.1.0.1'      | null                  | 'XXXXX'   || false
    }


}
