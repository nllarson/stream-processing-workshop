package test.java.org.improving.workshop.flink;

import spock.lang.Specification

class UseCaseTestTemplateSpec extends Specification {
    def "use case test case"() {
        given: 'some use case test case scenario'
            boolean isPassing = false
        when: 'some action happens'
            isPassing = true
        then: 'the test case scenario should pass'
            assert isPassing
    }
}
