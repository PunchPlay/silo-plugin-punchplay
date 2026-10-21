package provider

import (
	"regexp"
	"strconv"
	"strings"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

var imdbPattern = regexp.MustCompile(`^tt\d+$`)

// ids are the identifiers PunchPlay accepts on a write. TMDB is preferred:
// PunchPlay matches it without a lookup. The others are resolved on
// PunchPlay's side, and an item that cannot be resolved yet is kept there and
// retried.
type ids struct {
	tmdb int
	imdb string
	tvdb int
}

func (i ids) empty() bool { return i.tmdb == 0 && i.imdb == "" && i.tvdb == 0 }

// fields returns the ids as bulk-item fields.
func (i ids) fields() map[string]any {
	out := map[string]any{}
	if i.tmdb > 0 {
		out["tmdb_id"] = i.tmdb
	}
	if i.imdb != "" {
		out["imdb_id"] = i.imdb
	}
	if i.tvdb > 0 {
		out["tvdb_id"] = i.tvdb
	}
	return out
}

// idsFrom reads Silo external IDs, ignoring values PunchPlay would reject.
func idsFrom(external map[string]string) ids {
	var out ids
	for namespace, value := range external {
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(namespace)) {
		case "tmdb":
			out.tmdb = positive(value)
		case "imdb":
			if imdbPattern.MatchString(value) {
				out.imdb = value
			}
		case "tvdb":
			out.tvdb = positive(value)
		}
	}
	return out
}

func positive(value string) int {
	number, err := strconv.Atoi(value)
	if err != nil || number <= 0 {
		return 0
	}
	return number
}

// titleIDs are the IDs that identify a movie or series for a write: the
// item's own IDs, or for an episode its series' IDs.
func titleIDs(media *pluginv1.WatchSyncMedia) ids {
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
		return idsFrom(media.GetSeriesExternalIds())
	}
	return idsFrom(media.GetExternalIds())
}

// titleKind is PunchPlay's kind for the title a media item belongs to.
func titleKind(media *pluginv1.WatchSyncMedia) string {
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE {
		return "movie"
	}
	return "show"
}

func titleName(media *pluginv1.WatchSyncMedia) string {
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
		return strings.TrimSpace(media.GetSeriesTitle())
	}
	return strings.TrimSpace(media.GetTitle())
}

func titleYear(media *pluginv1.WatchSyncMedia) int32 {
	year := media.GetYear()
	if media.GetMediaType() == pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE {
		year = media.GetSeriesYear()
	}
	if year < 1888 || year > 2100 {
		return 0
	}
	return year
}

// record is one PunchPlay sync record, as the snapshot and changes feeds
// return it. Only the fields the plugin reads are listed.
type record struct {
	ID           *int64   `json:"id,omitempty"`
	Type         *string  `json:"type,omitempty"`
	Kind         *string  `json:"kind,omitempty"`
	Scope        *string  `json:"scope,omitempty"`
	MediaSource  *string  `json:"mediaSource,omitempty"`
	SourceID     *int64   `json:"sourceId,omitempty"`
	TmdbID       *int64   `json:"tmdbId,omitempty"`
	ShowSourceID *int64   `json:"showSourceId,omitempty"`
	ShowTmdbID   *int64   `json:"showTmdbId,omitempty"`
	Title        *string  `json:"title,omitempty"`
	Year         *int64   `json:"year,omitempty"`
	Season       *int64   `json:"season,omitempty"`
	Episode      *int64   `json:"episode,omitempty"`
	EpisodeTitle *string  `json:"episodeTitle,omitempty"`
	WatchedAt    *string  `json:"watchedAt,omitempty"`
	UpdatedAt    *string  `json:"updatedAt,omitempty"`
	AddedAt      *string  `json:"addedAt,omitempty"`
	ClientItemID *string  `json:"clientItemId,omitempty"`
	Rating       *float64 `json:"rating,omitempty"`
	IsFavourite  *bool    `json:"isFavourite,omitempty"`
	ShowStatus   *string  `json:"showStatus,omitempty"`
	ListID       *int64   `json:"listId,omitempty"`
	IsWatchlist  *bool    `json:"isWatchlist,omitempty"`
	// Playback only.
	ProgressPercent *float64 `json:"progressPercent,omitempty"`
}

func str(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func num(value *int64) int64 {
	if value == nil || *value <= 0 {
		return 0
	}
	return *value
}

// titleExternalIDs returns the Silo external IDs of the movie or show a
// record belongs to. Anime is stored under its TVDB series ID, with the TMDB
// ID alongside when PunchPlay knows it.
func titleExternalIDs(source string, sourceID, tmdbID int64) map[string]string {
	out := map[string]string{}
	if tmdbID > 0 {
		out["tmdb"] = strconv.FormatInt(tmdbID, 10)
	}
	switch source {
	case "tvdb":
		if sourceID > 0 {
			out["tvdb"] = strconv.FormatInt(sourceID, 10)
		}
	case "tmdb", "":
		if tmdbID == 0 && sourceID > 0 {
			out["tmdb"] = strconv.FormatInt(sourceID, 10)
		}
	}
	return out
}

// watchMedia describes a history or playback record: a movie, or an episode
// of a show. It returns nil for a record without usable identity.
func watchMedia(r record) *pluginv1.WatchSyncMedia {
	source := strings.ToLower(str(r.MediaSource))
	if str(r.Type) == "episode" {
		series := titleExternalIDs(source, num(r.ShowSourceID), num(r.ShowTmdbID))
		if len(series) == 0 || r.Season == nil || *r.Season < 0 || num(r.Episode) == 0 {
			return nil
		}
		return &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			Title:             str(r.EpisodeTitle),
			SeriesTitle:       str(r.Title),
			SeriesYear:        int32(num(r.Year)),
			SeriesExternalIds: series,
			SeasonNumber:      int32(*r.Season),
			EpisodeNumber:     int32(num(r.Episode)),
		}
	}
	external := titleExternalIDs(source, num(r.SourceID), num(r.TmdbID))
	if len(external) == 0 {
		return nil
	}
	return &pluginv1.WatchSyncMedia{
		MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
		Title:       str(r.Title),
		Year:        int32(num(r.Year)),
		ExternalIds: external,
	}
}

// interactionMedia describes an interaction or list record: a movie, a
// series, or (for interactions) an episode. Season-scoped interactions have
// no Silo counterpart and return nil.
func interactionMedia(r record) *pluginv1.WatchSyncMedia {
	external := titleExternalIDs(strings.ToLower(str(r.MediaSource)), num(r.SourceID), num(r.TmdbID))
	if len(external) == 0 {
		return nil
	}
	if str(r.Kind) == "movie" {
		if scope := str(r.Scope); scope != "" && scope != "title" {
			return nil
		}
		return &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_MOVIE,
			Title:       str(r.Title),
			Year:        int32(num(r.Year)),
			ExternalIds: external,
		}
	}
	if str(r.Kind) != "show" {
		return nil
	}
	switch str(r.Scope) {
	case "", "title", "series":
		return &pluginv1.WatchSyncMedia{
			MediaType:   pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_SERIES,
			Title:       str(r.Title),
			Year:        int32(num(r.Year)),
			ExternalIds: external,
		}
	case "episode":
		if r.Season == nil || *r.Season < 0 || num(r.Episode) == 0 {
			return nil
		}
		return &pluginv1.WatchSyncMedia{
			MediaType:         pluginv1.WatchSyncMediaType_WATCH_SYNC_MEDIA_TYPE_EPISODE,
			SeriesExternalIds: external,
			SeasonNumber:      int32(*r.Season),
			EpisodeNumber:     int32(num(r.Episode)),
		}
	default:
		return nil
	}
}
