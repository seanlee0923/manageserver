# manageserver

`local-central-client`(온프레미스 클라이언트)와 `local-central-server`(중앙 관리 서버)가 공용으로 쓰는 웹소켓 통신 레이어입니다. 두 프로젝트가 각자 구현해서 관리하던 웹소켓 연결/메시지 포맷 코드를 여기 하나로 모아, 프로토콜이 서로 어긋나는 문제를 없애는 게 목적입니다.

DB 연동, 로깅, 도메인 로직(어떤 데이터를 언제 보낼지)은 전혀 알지 못합니다. 오직 연결 관리 + 메시지 프레이밍 + 요청/응답 매칭만 담당하고, 나머지는 옵션(콜백)으로 호출하는 쪽에서 주입합니다.

## 구성

- `manageserver` (root 패키지) — `Client`, `Server`, `Session`
- `manageserver/protocol` — 실제로 주고받는 메시지 구조체 (`Message`, `LoginReq`, `TickerConfigReq` 등). client/server 양쪽에서 동일하게 사용해야 하는 부분만 여기 있습니다.

## 설치

퍼블릭 저장소([github.com/seanlee0923/manageserver](https://github.com/seanlee0923/manageserver))라 별도 설정 없이 바로 받아 쓸 수 있습니다.

```
go get github.com/seanlee0923/manageserver
```

로컬에서 수정하면서 바로 반영해보고 싶다면 (배포 전 임시로) `go.mod`에 `replace`를 추가합니다.

```
require github.com/seanlee0923/manageserver v0.0.0
replace github.com/seanlee0923/manageserver => ../manageserver
```

## 서버 쪽 사용 예시

```go
import (
    "github.com/seanlee0923/manageserver"
    "github.com/seanlee0923/manageserver/protocol"
)

s, err := manageserver.NewServer(
    manageserver.WithPort("8383"),
    manageserver.WithAuthFunc(func(id string) (any, bool) {
        // id로 DB 조회해서 연결 허용 여부 + 이후 조회에 쓸 값(예: SQL row id) 반환
        var site model.Site
        db.Where("code = ?", id).Find(&site)
        if site.ID == 0 {
            return nil, false
        }
        return site.ID, true
    }),
    manageserver.WithOnDisconnect(func(sess *manageserver.Session) {
        // 연결 종료 처리
    }),
)

s.On("PcStatus", func(sess *manageserver.Session, msg *protocol.Message) any {
    var req protocol.PcStatusReq
    json.Unmarshal(msg.Data, &req)

    siteId := sess.PersistenceID.(int) // WithAuthFunc가 반환한 값
    // ... DB 저장 등 ...

    return protocol.StatusResp{Ok: true}
})

s.Run("/ws/")
```

## 클라이언트 쪽 사용 예시

```go
c, err := manageserver.NewClient(
    manageserver.WithID(siteCode),
    manageserver.WithRootCAFile(caFile), // 자체서명 인증서용, 선택
)

c.On("TickerConfig", handler.ConfigTicker)

err = c.Start("wss://central.example.com:8383", "/ws/")
```

## 주요 옵션

**Server**: `WithPort`, `WithTLS(cert, key)`, `WithAuthFunc`, `WithRequestValidator`, `WithOnConnect`, `WithOnDisconnect`, `WithOnActivity`, `WithOnError`, `WithSendTimeout`

**Client**: `WithID`(필수), `WithTLSConfig`, `WithRootCAFile`, `WithInsecureSkipVerify`, `WithHMACAuth`, `WithConnectHandler`, `WithErrorHandler`, `WithRequestTimeout`

## id만으로는 부족한 인증이 필요할 때 (WithRequestValidator)

`WithAuthFunc`는 연결 id만 보고 허용 여부를 정합니다. `id`(예: 사이트 코드)를 아는 사람이면 누구나 그 이름으로 접속을 시도할 수 있다는 뜻이라, 그걸로는 부족하면 `WithRequestValidator`를 얹어 쓰세요. `WithAuthFunc`와 완전히 독립적인 별도 옵션이라, 안 쓰면 기존 동작은 그대로입니다.

가장 흔한 경우(현장별 공유 secret으로 HMAC 서명)는 바로 쓸 수 있는 헬퍼가 있습니다:

```go
// 서버
manageserver.WithRequestValidator(manageserver.HMACRequestValidator(
    func(id string) (secret string, ok bool) {
        var site model.Site
        db.Where("code = ?", id).Find(&site)
        if site.ID == 0 || site.Secret == "" {
            return "", false
        }
        return site.Secret, true
    },
    0, // 0이면 manageserver.DefaultHMACMaxSkew(5분) 사용
))

// 클라이언트
manageserver.WithHMACAuth(siteSecret)
```

bearer token이나 mTLS 클라이언트 인증서 검사처럼 다른 방식이 필요하면 `HMACRequestValidator` 대신 직접 `func(r *http.Request, id string) bool`을 작성해서 `WithRequestValidator`에 넘기면 됩니다. `VerifyHMACRequest`가 별도로 export돼 있어서, 그 안에서 HMAC 체크를 다른 검증과 조합하는 것도 가능합니다.

> **주의**: `HMACRequestValidator`/`VerifyHMACRequest`는 타임스탬프 스큐(기본 5분)로만 replay를 제한합니다. nonce를 별도로 저장해서 재사용을 완전히 막지는 않습니다(그 저장소를 어디에 둘지는 애플리케이션마다 다르므로 manageserver가 강제하지 않음). 다만 서버가 이미 동일 id의 중복 연결을 거부하므로, 정상 클라이언트가 연결돼 있는 동안은 캡처된 서명을 재생해도 거부됩니다.

## 테스트

```
go test ./... -race
```
