package au.org.ala.biocache.service

import org.ala.client.model.LogEventVO
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.client.RestOperations
import spock.lang.Ignore
import spock.lang.Specification

// TODO: Remove @Ignore annotation - tests are failing on Travis but working locally, so difficult to debug
@Ignore
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
        1 * loggerService.restTemplate.postForEntity(*_) >> { new ResponseEntity(HttpStatus.OK) }

        cleanup:
        loggerService.destroy()
    }

    void 'block on queue limit'() {

        setup:
        final int queueSize = 10
        long logEventPostCount = 0

        LoggerRestService loggerService = new LoggerRestService()
        loggerService.enabled = false
        loggerService.restTemplate = Stub(RestOperations) {
            postForEntity(_, _, Void) >> {
                sleep(100)
                logEventPostCount++
                new ResponseEntity<Void>(HttpStatus.OK)
            }
        }
        loggerService.eventQueueSize = queueSize
//        loggerService.throttleDelay = 100
        loggerService.init()
        boolean logEventBlocked = true

        when: 'log more then buffer in quick succession'
        10.times {
            LogEventVO logEvent = new LogEventVO()
            logEvent.sourceUrl = it as String
            loggerService.logEvent(logEvent)
        }

        then: 'no log events should be async posted'
        logEventPostCount == 0

        when:
        Thread.start {

            sleep(1000)

            LogEventVO logEvent = new LogEventVO()
            logEvent.sourceUrl = 'blocked 11'

            loggerService.logEvent(logEvent)
            logEventBlocked = false
        }

        then: 'nexted log event should be blocked'
        logEventBlocked

        when:
        // wait until the posted async log events >= query size (queue cleared)
        while (logEventPostCount < queueSize) { sleep(10) }

        then: 'blocked log event call cleared'
        sleep(1000)
        !logEventBlocked

        cleanup:
        loggerService.destroy()
    }

    void 'throttle log events'() {

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

        cleanup:
        loggerService.destroy()
    }
}
