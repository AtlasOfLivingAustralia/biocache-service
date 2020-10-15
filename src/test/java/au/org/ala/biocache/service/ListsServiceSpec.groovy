package au.org.ala.biocache.service

import au.org.ala.biocache.dto.Kvp
import spock.lang.Specification

class ListsServiceSpec extends Specification {

    ListsService listService = new ListsService()

    def 'find Kpv'() {

        setup:
        List<Kvp> kvps = [
                new Kvp(1, 1),
                new Kvp(2, 2),
                new Kvp(3, 3)
        ]


        Kvp lftrgt = new Kvp(4, 4)

        when:
        Kvp result = listService.find(kvps, lftrgt)

        then:
        result == null
    }
}
