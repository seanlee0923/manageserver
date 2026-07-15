package manageserver

import "github.com/seanlee0923/manageserver/protocol"

// ClientHandler handles an incoming request from the server. The *Client
// argument lets the handler push additional messages asynchronously (e.g.
// after finishing a long-running job) independent of the value it returns.
// Returning nil closes the connection.
type ClientHandler func(*Client, *protocol.Message) any

// ServerHandler handles an incoming request from a connected client. The
// *Session argument identifies which client sent it and lets the handler
// push additional messages back to it asynchronously. Returning nil closes
// the connection.
type ServerHandler func(*Session, *protocol.Message) any
