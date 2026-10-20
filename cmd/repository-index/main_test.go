package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const download = "https://github.com/PunchPlay/silo-plugin-punchplay/releases/download/v0.1.0/"

func fullRelease() release {
	return release{
		TagName: "v0.1.0",
		Assets: []asset{
			{Name: "plugin-linux-amd64", BrowserDownloadURL: download + "plugin-linux-amd64"},
			{Name: "plugin-linux-arm64", BrowserDownloadURL: download + "plugin-linux-arm64"},
			{Name: "plugin-darwin-arm64", BrowserDownloadURL: download + "plugin-darwin-arm64"},
			{Name: "checksums.txt", BrowserDownloadURL: download + "checksums.txt"},
		},
	}
}

func repoManifest(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestIndexMatchesWhatSiloReads(t *testing.T) {
	index, err := buildIndex(repoManifest(t), fullRelease())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}

	// Silo decodes the index with encoding/json into its own structs.
	var silo struct {
		Plugins []struct {
			Manifest     *pluginv1.PluginManifest `json:"manifest"`
			RepoURL      string                   `json:"repo_url"`
			ChecksumsURL string                   `json:"checksums_url"`
			Binaries     map[string]struct {
				URL string `json:"url"`
			} `json:"binaries"`
		} `json:"plugins"`
	}
	if err := json.Unmarshal(encoded, &silo); err != nil {
		t.Fatalf("Silo could not decode the index: %v", err)
	}
	if len(silo.Plugins) != 1 {
		t.Fatalf("plugins = %d", len(silo.Plugins))
	}
	pkg := silo.Plugins[0]
	if pkg.Manifest.GetPluginId() != "tv.punchplay.watchprovider" || pkg.Manifest.GetVersion() != "0.1.0" ||
		pkg.Manifest.GetSiloApiVersion() != "v1" || pkg.Manifest.GetChecksum() != "" {
		t.Fatalf("manifest = %v", pkg.Manifest)
	}
	descriptor := pkg.Manifest.GetCapabilities()[0].GetWatchSyncProvider()
	if len(descriptor.GetAuthMethods()) != 1 || descriptor.GetAuthMethods()[0] != pluginv1.WatchSyncAuthMethod_WATCH_SYNC_AUTH_METHOD_DEVICE_CODE {
		t.Fatalf("auth methods = %v", descriptor.GetAuthMethods())
	}
	if pkg.RepoURL != "https://github.com/PunchPlay/silo-plugin-punchplay" || pkg.ChecksumsURL != download+"checksums.txt" {
		t.Fatalf("package = %+v", pkg)
	}
	if len(pkg.Binaries) != 3 || pkg.Binaries["linux/arm64"].URL != download+"plugin-linux-arm64" {
		t.Fatalf("binaries = %v", pkg.Binaries)
	}
	// The source manifest's placeholder checksum must not leak.
	if strings.Contains(string(encoded), "__CHECKSUM__") {
		t.Fatal("index carries the checksum placeholder")
	}
}

func TestVersionComesFromTheTag(t *testing.T) {
	rel := fullRelease()
	rel.TagName = "v0.1.7"
	index, err := buildIndex(repoManifest(t), rel)
	if err != nil {
		t.Fatal(err)
	}
	if got := index.Plugins[0].Manifest.GetVersion(); got != "0.1.7" {
		t.Fatalf("version = %q", got)
	}
}

func TestIncompleteReleasesAreRefused(t *testing.T) {
	noChecksums := fullRelease()
	noChecksums.Assets = noChecksums.Assets[:3]
	missingPlatform := fullRelease()
	missingPlatform.Assets = missingPlatform.Assets[1:]
	draft := fullRelease()
	draft.Draft = true
	for name, rel := range map[string]release{"no checksums": noChecksums, "missing platform": missingPlatform, "draft": draft} {
		if _, err := buildIndex(repoManifest(t), rel); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}
