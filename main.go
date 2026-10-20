package main

import (
	_ "embed"

	"github.com/PunchPlay/silo-plugin-punchplay/provider"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/runtime"
)

// version is set at build time with -ldflags "-X main.version=...".
var version string

//go:embed manifest.json
var manifestJSON []byte

func main() {
	server := provider.NewServer(nil, version)
	runtime.ServeManifestWithOptions(manifestJSON, version, runtime.CapabilityServers{
		WatchSyncProvider: server,
	}, runtime.WithWatchSyncDeviceAuthorization(server.DeviceAuthorization()))
}
