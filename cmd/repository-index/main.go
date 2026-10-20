// Command repository-index writes the plugin repository index Silo admins add
// under Admin > Integrations > Plugins > Repositories. It lists the latest
// release in the same shape Silo's own catalogs use, so Silo can install the
// plugin and offer its updates.
//
//	gh api repos/PunchPlay/silo-plugin-punchplay/releases/tags/v0.1.0 > release.json
//	go run ./cmd/repository-index -manifest manifest.json -release release.json -out repository.json
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	pluginmanifest "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginsdk/manifest"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const repository = "PunchPlay/silo-plugin-punchplay"

type asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

type release struct {
	TagName    string  `json:"tag_name"`
	Draft      bool    `json:"draft"`
	Prerelease bool    `json:"prerelease"`
	Assets     []asset `json:"assets"`
}

type platformBinary struct {
	URL string `json:"url"`
}

// catalogPackage and repositoryIndex match the JSON Silo reads
// (internal/plugins/catalog_service.go in silo-server). The manifest is
// encoded with encoding/json, as Silo decodes it, so enums are numbers.
type catalogPackage struct {
	Manifest     *pluginv1.PluginManifest  `json:"manifest"`
	RepoURL      string                    `json:"repo_url,omitempty"`
	ChecksumsURL string                    `json:"checksums_url,omitempty"`
	Binaries     map[string]platformBinary `json:"binaries,omitempty"`
}

type repositoryIndex struct {
	Plugins []catalogPackage `json:"plugins"`
}

func main() {
	manifestPath := flag.String("manifest", "manifest.json", "plugin manifest")
	releasePath := flag.String("release", "", "GitHub release JSON (gh api repos/OWNER/REPO/releases/tags/TAG)")
	outPath := flag.String("out", "repository.json", "index to write")
	flag.Parse()

	if err := run(*manifestPath, *releasePath, *outPath); err != nil {
		fmt.Fprintln(os.Stderr, "repository-index:", err)
		os.Exit(1)
	}
}

func run(manifestPath, releasePath, outPath string) error {
	if releasePath == "" {
		return errors.New("-release is required")
	}
	manifestJSON, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	releaseJSON, err := os.ReadFile(releasePath)
	if err != nil {
		return err
	}
	var rel release
	if err := json.Unmarshal(releaseJSON, &rel); err != nil {
		return fmt.Errorf("decode release: %w", err)
	}
	index, err := buildIndex(manifestJSON, rel)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(outPath, append(encoded, '\n'), 0o644)
}

func buildIndex(manifestJSON []byte, rel release) (repositoryIndex, error) {
	if rel.Draft || rel.Prerelease {
		return repositoryIndex{}, fmt.Errorf("release %q is a draft or prerelease", rel.TagName)
	}
	var source pluginv1.PluginManifest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(manifestJSON, &source); err != nil {
		return repositoryIndex{}, fmt.Errorf("decode manifest: %w", err)
	}
	repoURL := "https://github.com/" + repository
	if err := pluginmanifest.ValidateCatalogPresentation(&source, repoURL); err != nil {
		return repositoryIndex{}, fmt.Errorf("manifest presentation: %w", err)
	}

	version := strings.TrimPrefix(strings.TrimSpace(rel.TagName), "v")
	if version == "" {
		return repositoryIndex{}, errors.New("release has no tag")
	}

	checksumsURL := ""
	binaries := map[string]platformBinary{}
	for _, a := range rel.Assets {
		switch {
		case a.Name == "checksums.txt":
			checksumsURL = a.BrowserDownloadURL
		case strings.HasPrefix(a.Name, "plugin-"):
			parts := strings.Split(a.Name, "-")
			if len(parts) == 3 && parts[1] != "" && parts[2] != "" {
				binaries[parts[1]+"/"+parts[2]] = platformBinary{URL: a.BrowserDownloadURL}
			}
		}
	}
	if checksumsURL == "" {
		return repositoryIndex{}, fmt.Errorf("release %q has no checksums.txt", rel.TagName)
	}
	// Every platform the manifest promises must be downloadable.
	for _, platform := range source.GetSupportedPlatforms() {
		key := platform.GetOs() + "/" + platform.GetArch()
		if _, ok := binaries[key]; !ok {
			return repositoryIndex{}, fmt.Errorf("release %q has no binary for %s", rel.TagName, key)
		}
	}

	manifest := proto.Clone(&source).(*pluginv1.PluginManifest)
	manifest.Version = version
	// The checksum differs per platform; Silo reads it from checksums.txt.
	manifest.Checksum = ""
	if err := pluginmanifest.Validate(manifest); err != nil {
		return repositoryIndex{}, fmt.Errorf("manifest: %w", err)
	}

	return repositoryIndex{Plugins: []catalogPackage{{
		Manifest:     manifest,
		RepoURL:      repoURL,
		ChecksumsURL: checksumsURL,
		Binaries:     binaries,
	}}}, nil
}
