package manageserver

import "strings"

const defaultPath = "/ws/"

// normalizePath defaults empty to "/ws/" and ensures exactly one trailing
// slash, since http.ServeMux treats a trailing-slash pattern as a subtree
// match and both Client.Start and Server.Run rely on that to route by id.
func normalizePath(path string) string {
	if path == "" {
		path = defaultPath
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}
