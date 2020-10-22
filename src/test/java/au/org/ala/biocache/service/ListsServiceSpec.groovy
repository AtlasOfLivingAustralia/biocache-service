package au.org.ala.biocache.service

import au.org.ala.biocache.dto.Kvp
import spock.lang.Specification
import spock.lang.Unroll

class ListsServiceSpec extends Specification {

    ListsService listService = new ListsService()

    @Unroll
    def 'find Kpv(lft: #lft, rgt: #rgt)'() {

        setup:
        List<Kvp> kvps = [
                new Kvp(10, 10),
                new Kvp(20, 20),
                new Kvp(24, 26),
                new Kvp(30, 30)
        ]


        when:
        Kvp result = listService.find(kvps, new Kvp(lft, rgt))

        then:
        expectedKvp != null || result == null
        expectedKvp == null || result.lft == expectedKvp.lft
        expectedKvp == null || result.rgt == expectedKvp.rgt

        where:
        lft | rgt || expectedKvp
        1   | 1   || null
        10  | 10  || new Kvp(10, 10)
        19  | 21  || null
        24  | 26  || new Kvp(24, 26)
        25  | 25  || new Kvp(24, 26)
        29  | 30  || null
        30  | 30  || new Kvp(30, 30)
        30  | 31  || null
        40  | 40  || null
    }
}
