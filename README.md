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

**Server**: `WithPort`, `WithTLS(cert, key)`, `WithAuthFunc`, `WithOnConnect`, `WithOnDisconnect`, `WithOnActivity`, `WithOnError`, `WithSendTimeout`

**Client**: `WithID`(필수), `WithTLSConfig`, `WithRootCAFile`, `WithInsecureSkipVerify`, `WithConnectHandler`, `WithErrorHandler`, `WithRequestTimeout`

## 테스트

```
go test ./... -race
```
