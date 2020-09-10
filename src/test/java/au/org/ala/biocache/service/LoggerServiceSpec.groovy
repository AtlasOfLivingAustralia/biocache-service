package au.org.ala.biocache.service

import org.ala.client.model.LogEventVO
import org.springframework.web.client.RestOperations
import spock.lang.Specification

class LoggerServiceSpec extends Specification {

    LoggerService loggerService = new LoggerRestService()

    def 'user-agent blacklist'() {

        setup:
        loggerService.userAgentBlacklist = blacklist
        RestOperations restOperations = Mock()
        loggerService.restTemplate = restOperations

        LogEventVO logEventVO = new LogEventVO()
        logEventVO.userAgent = userAgent

        when:
        loggerService.logEvent(logEventVO)

        then:
        (log ? 1 : 0) * restOperations.postForEntity(*_)

        where:
        blacklist | userAgent || log
        null | 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36' || true
        [ '.* \\(StatusCake\\)' ] | 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/98 Safari/537.4 (StatusCake)' || false
        [ '.* \\(StatusCake\\)' ] | 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36' || true             // Chrome on macOS
        [ '.* \\(StatusCake\\)' ] | 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36 Edg/85.0.564.44' || true    // MS edge on Windows 10
        [ '.* \\(StatusCake\\)' ] | 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; Trident/7.0; rv:11.0) like Gecko' || true                                                            // IE11 on Windows 10
    }

}
