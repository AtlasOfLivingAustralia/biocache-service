package au.org.ala.biocache.service

import org.ala.client.model.LogEventVO
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.client.RestOperations
import spock.lang.Specification

class LoggerServiceSpec extends Specification {

    void 'async log event'() {

        setup:
        LoggerRestService loggerService = new LoggerRestService()
        loggerService.enabled = false
        loggerService.restTemplate = Mock(RestOperations)

        loggerService.init()

        LogEventVO logEvent = new LogEventVO()
        HttpHeaders headers = new HttpHeaders()
        headers.set(HttpHeaders.USER_AGENT, logEvent.getUserAgent())

        when:
        loggerService.logEvent(logEvent)
        Thread.sleep(1000)

        then:
        1 * loggerService.restTemplate.postForEntity(_, new HttpEntity<>(logEvent, headers), Void) >> { new ResponseEntity<Void>(HttpStatus.OK) }
    }

    void 'thottle log events'() {

        setup:
        LoggerRestService loggerService = new LoggerRestService()
        loggerService.enabled = false
        loggerService.restTemplate = Mock(RestOperations)
        loggerService.throttleDelay = 100

        loggerService.init()

        LogEventVO logEvent = new LogEventVO()
        HttpHeaders headers = new HttpHeaders()
        headers.set(HttpHeaders.USER_AGENT, logEvent.getUserAgent())

        when: 'log more then buffer in quick succession'
        10.times {
            loggerService.logEvent(logEvent)
        }

        then:
        0 * loggerService.restTemplate.postForEntity(_, new HttpEntity<>(logEvent, headers), Void) >> { new ResponseEntity<Void>(HttpStatus.OK) }

        when:
        Thread.sleep(10 * loggerService.throttleDelay)

        then: 'expect all events POSTed'
        10 * loggerService.restTemplate.postForEntity(_, new HttpEntity<>(logEvent, headers), Void) >> { new ResponseEntity<Void>(HttpStatus.OK) }
    }
}
