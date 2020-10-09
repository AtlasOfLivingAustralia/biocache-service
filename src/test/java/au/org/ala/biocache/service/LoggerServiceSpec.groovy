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

    void 'block on queue limit'() {

        setup:
        LoggerRestService loggerService = new LoggerRestService()
        loggerService.enabled = false
        loggerService.restTemplate = Stub(RestOperations) {
            postForEntity(_, _, Void) >> { sleep(11); new ResponseEntity<Void>(HttpStatus.OK) }
        }
        loggerService.eventQueueSize = 10
//        loggerService.throttleDelay = 100

        loggerService.init()

        boolean logEventBlocked = true

        when: 'log more then buffer in quick succession'
        10.times {
            LogEventVO logEvent = new LogEventVO()
            logEvent.sourceUrl = it as String

            loggerService.logEvent(logEvent)
        }

        then:
        true
//        0 * loggerService.restTemplate.postForEntity(_, _, Void) >> { sleep(11); new ResponseEntity<Void>(HttpStatus.OK) }

        when:
        Thread.start {

            sleep(100)

            LogEventVO logEvent = new LogEventVO()
            logEvent.sourceUrl = 'blocked 11'

            loggerService.logEvent(logEvent)
            logEventBlocked = false
        }

        then:
        logEventBlocked
//        _ * loggerService.restTemplate.postForEntity(*_) >> { sleep(12); new ResponseEntity<Void>(HttpStatus.OK) }

        when:
        sleep(10000)

        then: 'expect all events POSTed'
        !logEventBlocked
//        _ * loggerService.restTemplate.postForEntity(*_) >> { sleep(12); new ResponseEntity<Void>(HttpStatus.OK) }


//        when:
//        logEventBlocked = true
//        Thread.start({
//            loggerService.logEvent(logEvent)
//            logEventBlocked = false
//        })
//
//        then:
//        logEventBlocked
//        loggerService.eventQueue.size() == 9

//        when:
//        sleep(2000)
//
//        then:
//        10 * loggerService.restTemplate.postForEntity(_, new HttpEntity<>(logEvent, headers), Void) >> { new ResponseEntity<Void>(HttpStatus.OK) }
//        !logEventBlocked

//        then: 'expect all events POSTed'
    }

    void 'thottle log events'() {

        setup:
        LoggerRestService loggerService = new LoggerRestService()
        loggerService.enabled = false
        loggerService.restTemplate = Mock(RestOperations)
        loggerService.throttleDelay = 100

        loggerService.init()

        when: 'log more then buffer in quick succession'
        10.times {
            LogEventVO logEvent = new LogEventVO()
            logEvent.sourceUrl = it as String

            loggerService.logEvent(logEvent)
        }
        
        sleep(10 * loggerService.throttleDelay)

        then: 'expect all events POSTed'
        10 * loggerService.restTemplate.postForEntity(*_) >> { new ResponseEntity<Void>(HttpStatus.OK) }
    }
}
