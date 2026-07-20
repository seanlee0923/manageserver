# manageserver

`local-central-client`(온프레미스 클라이언트)와 `local-central-server`(중앙 관리 서버)가 공용으로 쓰는 웹소켓 통신 레이어입니다. 두 프로젝트가 각자 구현해서 관리하던 웹소켓 연결/메시지 포맷 코드를 여기 하나로 모아, 프로토콜이 서로 어긋나는 문제를 없애는 게 목적입니다.

DB 연동, 로깅, 도메인 로직(어떤 데이터를 언제 보낼지)은 전혀 알지 못합니다. 오직 연결 관리 + 메시지 프레이밍 + 요청/응답 매칭만 담당하고, 나머지는 옵션(콜백)으로 호출하는 쪽에서 주입합니다.

## 역할과 책임 범위

`manageserver`가 담당하는 기능:

- WebSocket client 연결과 server listen
- client ID 기반 session 등록 및 중복 연결 거절
- JSON message framing
- UUID message ID를 이용한 요청/응답 matching
- context 취소를 지원하는 `Client.SendContext`/`Session.SendContext`
- ping/pong과 연결 종료 전파
- action별 handler 등록
- 선택적인 TLS, handshake 검증, HMAC 인증
- 연결·활동·종료·오류 callback

호출 애플리케이션이 담당하는 기능:

- 환경변수와 설정 파일 로딩
- DB 조회 및 영속화
- 로그 기록과 metric
- reconnect 정책
- 어떤 action과 DTO를 사용할지에 대한 domain logic
- secret 발급·저장·rotation
- 배포 환경의 TLS 종단과 인증서 관리

## 환경변수

이 라이브러리는 환경변수를 직접 읽지 않습니다. 모든 설정은 `NewClient`와 `NewServer` option으로 주입합니다. 실제 환경변수 이름은 사용하는 애플리케이션의 README를 참고하세요.

| 영역 | 주요 option | 기본값/설명 |
|---|---|---|
| Client identity | `WithID` | 필수 |
| Client HMAC | `WithHMACAuth` | 선택 |
| Client TLS | `WithTLSConfig`, `WithRootCAFile`, `WithInsecureSkipVerify` | 선택 |
| Client timeout | `WithRequestTimeout` | 30분 (Send 응답 대기) |
| Client 연결 상태 | `WithClientReadTimeout`, `WithClientWriteTimeout`, `WithClientReadLimit` | 5분 / 30초 / 4MiB |
| Server address | `WithPort` | `8080` |
| Server TLS | `WithTLS` | 인증서와 key를 함께 지정할 때 활성화 |
| Server 인증 | `WithRequestValidator`, `WithAuthFunc` | 선택 |
| Server timeout | `WithSendTimeout` | 60초 (Send 응답 대기) |
| Server 연결 상태 | `WithReadTimeout`, `WithWriteTimeout`, `WithReadLimit`, `WithPingInterval` | 5분 / 30초 / 4MiB / 2분 |

기본 endpoint path는 `/ws/`이며 client ID가 뒤에 붙어 `/ws/{id}` 형태로 연결됩니다.

## 구성

- `manageserver` (root 패키지) — `Client`, `Server`, `Session`
- `manageserver/protocol` — 실제로 주고받는 메시지 구조체 (`Message`, `LoginReq`, `TickerConfigReq` 등). client/server 양쪽에서 동일하게 사용해야 하는 부분만 여기 있습니다.

파일 전송 계약의 `FileUploadReq`에는 원본 파일명, 파일 크기, MD5 checksum, 일회성 grant가 포함됩니다. checksum 검증과 임시 파일 저장은 도메인 애플리케이션이 담당합니다.

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

응답을 기다리는 요청을 호출자의 취소와 함께 중단하려면 `SendContext`를 사용합니다.

```go
ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
defer cancel()
resp, err := c.SendContext(ctx, "File", request)
```

## 주요 옵션

**Server**: `WithPort`, `WithTLS(cert, key)`, `WithAuthFunc`, `WithRequestValidator`, `WithOnConnect`, `WithOnDisconnect`, `WithOnActivity`, `WithOnPong`, `WithOnError`, `WithSendTimeout`, `WithReadTimeout`, `WithWriteTimeout`, `WithReadLimit`, `WithPingInterval`

**Client**: `WithID`(필수), `WithTLSConfig`, `WithRootCAFile`, `WithInsecureSkipVerify`, `WithHMACAuth`, `WithConnectHandler`, `WithErrorHandler`, `WithPingHandler`, `WithRequestTimeout`, `WithClientReadTimeout`, `WithClientWriteTimeout`, `WithClientReadLimit`

## 연결 상태 감지 (read/write deadline, ping/pong, read limit)

기본값(서버 기준)으로, 서버는 2분마다 각 세션에 ping을 보내고(`WithPingInterval`), 클라이언트의 pong이나 다른 메시지가 5분 이상 오지 않으면 연결을 끊습니다(`WithReadTimeout`). 죽은 피어에 쓰다가 영원히 블록되는 걸 막기 위해 쓰기에도 30초 데드라인이 있습니다(`WithWriteTimeout`). 들어오는 프레임 크기는 기본 4MiB로 제한됩니다(`WithReadLimit`) — `local-central-client`의 파일 업로드 청크 크기(4MB)에 맞춘 값입니다. 클라이언트 쪽도 대칭으로 `WithClientReadTimeout`/`WithClientWriteTimeout`/`WithClientReadLimit`이 있습니다.

`WithOnPong`(서버)과 `WithPingHandler`(클라이언트)는 ping/pong이 오갈 때마다 호출되는 관측용 콜백입니다 — 연결을 살려두는 동작 자체는 이 콜백을 등록하지 않아도 그대로 동작하고, 콜백은 로깅 같은 부가 용도로만 쓰입니다.

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

## 프로토콜 오류 처리

- 등록되지 않은 request action은 `unknown_action` 오류로 즉시 응답한다.
- handler panic은 `handler_panic`으로 응답하고 연결은 유지한다.
- notify 오류는 응답 없이 error hook에 보고한다.
- `Send`는 원격 오류를 `*manageserver.RemoteProtocolError`로 반환한다.
- error hook에는 action과 message/session ID를 가진 `*manageserver.DispatchError`가 전달된다.
