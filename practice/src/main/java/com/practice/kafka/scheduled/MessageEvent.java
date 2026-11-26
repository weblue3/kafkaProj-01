package com.practice.kafka.scheduled;

/*
간결성 : 필드를 정의하면 해당 필드를 기반으로 자동으로 메서드가 생성되어 코드의 양을 줄일 수 있고, 불필요한 보일러플레이트 코드를 줄여 가독성을 높여준다.
메서드 자동 생성 : 필드를 기반으로 equals(), hashCode(), toString() 메서드를 자동으로 생성한다.
생성자 자동 생성 : 필드를 기반으로 자동으로 생성자를 생성한다. (기본 생성자는 제공하지 않으므로 필요한 경우 직접 생성해야한다.)
불변성 : 필드가 한번 설정되면 값을 변경할 수 없다. 이는 데이터의 안정성을 높이는데 도움이 된다.
fianl 선언 생략 : 불변 객체로서 abstract로 선언할 수 없으며 암시적으로 private final로 선언된다.
패턴 매칭 통합 : Java의 Pattern Matching과 함께 사용될 수 있어, 데이터 추출 및 패턴 일치 검사에 효과적으로 사용될 수 있다.
데이터 전달 : record의 주된 목적은 객체 간에 불변 데이터를 전달하는것으로서 DTO를 표현하는데 적합하다.
* */
public record MessageEvent(String key, String value) {
}
