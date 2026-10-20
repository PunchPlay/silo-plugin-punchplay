package main

import (
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
)

func TestManifestPassesCatalogValidation(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if err := manifest.ValidateCatalogPresentation(parsed, "https://github.com/PunchPlay/silo-plugin-punchplay"); err != nil {
		t.Fatalf("catalog presentation: %v", err)
	}
	capabilities := parsed.GetCapabilities()
	if len(capabilities) != 1 || capabilities[0].GetType() != "watch_sync_provider.v1" || capabilities[0].GetId() != "punchplay" {
		t.Fatalf("capabilities = %v", capabilities)
	}
}

func TestManifestAdvertisesTheSpecsCapabilities(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	d := parsed.GetCapabilities()[0].GetWatchSyncProvider()
	if len(d.GetAuthMethods()) != 1 || d.GetAuthMethods()[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_DEVICE_CODE {
		t.Fatalf("auth methods = %v", d.GetAuthMethods())
	}
	if !d.GetImportWatched() || !d.GetExportWatched() || !d.GetExportUnwatched() || !d.GetImportProgress() ||
		!d.GetImportFavorites() || !d.GetExportFavorites() || !d.GetRemoveFavorites() ||
		!d.GetImportWatchlist() || !d.GetExportWatchlist() || !d.GetRemoveWatchlist() ||
		!d.GetImportRatings() || !d.GetExportRatings() || !d.GetSyncDropped() || !d.GetScrobblePlayback() {
		t.Fatalf("descriptor = %v", d)
	}
	// Rating on PunchPlay never logs a watch, and list items have no manual
	// order yet.
	if len(d.GetRatingExportRequiresWatched()) != 0 || d.GetProvidesWatchlistOrder() {
		t.Fatalf("descriptor = %v", d)
	}
	// Every rewatch is a play on PunchPlay, so there is no rewatch switch.
	if len(d.GetConnectionSettings()) != 0 {
		t.Fatalf("connection settings = %v", d.GetConnectionSettings())
	}
	if d.GetMaxBatchSize() != 100 {
		t.Fatalf("max batch size = %d, want PunchPlay's bulk limit", d.GetMaxBatchSize())
	}
}

func TestManifestNeedsNoAdminSetup(t *testing.T) {
	t.Parallel()
	parsed, err := manifest.Load(manifestJSON)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	for _, schema := range parsed.GetGlobalConfigSchema() {
		if schema.GetRequired() {
			t.Fatalf("config %q is required; the plugin must work with no settings", schema.GetKey())
		}
		for _, field := range schema.GetAdminForm().GetFields() {
			if field.GetRequired() || field.GetSecret() {
				t.Fatalf("field %q must be optional and public", field.GetKey())
			}
		}
	}
}
