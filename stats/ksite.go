package stats

import (
    "encoding/json"
    "math"
    "os"
    "path/filepath"
    "sync"
    "time"

    log "github.com/sirupsen/logrus"

    "github.com/sagan/ptool/config"
)

// Lightweight per-site, per time-bucket k_site (bytes/s per unit demand L/(S+1)) estimator.
// Persisted as a small JSON file under config dir.

const (
    ksiteFilename          = "ptool_ksite.json"
    ksiteDefaultK          = int64(100 * 1024) // default 100KiB per unit demand
    ksiteTimeBucketHours   = 4                  // default 4-hour buckets => 6 buckets per day
    ksiteEwmaAlpha         = 0.3                // default EWMA alpha
    ksiteMinSamplesForRead = 3
)

type ksiteEntry struct {
    Site    string  `json:"site"`
    Bucket  int     `json:"bucket"`
    K       float64 `json:"k"`
    N       int64   `json:"n"`
    Updated int64   `json:"updated"`
}

type ksiteModel struct {
    Entries []ksiteEntry `json:"entries"`
}

var (
    ksiteMu sync.Mutex
    ksite   = &ksiteModel{}
    loaded  = false
)

func ksitePath() string {
    return filepath.Join(config.ConfigDir, ksiteFilename)
}

func loadKSite() {
    if loaded {
        return
    }
    loaded = true
    path := ksitePath()
    f, err := os.Open(path)
    if err != nil {
        return
    }
    defer f.Close()
    dec := json.NewDecoder(f)
    _ = dec.Decode(ksite)
}

func saveKSite() {
    path := ksitePath()
    tmp := path + ".tmp"
    f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
    if err != nil {
        log.Debugf("saveKSite open error: %v", err)
        return
    }
    enc := json.NewEncoder(f)
    enc.SetIndent("", "  ")
    _ = enc.Encode(ksite)
    f.Close()
    _ = os.Rename(tmp, path)
}

func getBucketHours() int {
    v := config.Get().BrushKSiteBucketHours
    if v <= 0 || v > 24 {
        return ksiteTimeBucketHours
    }
    return int(v)
}

func getEwmaAlpha() float64 {
    v := config.Get().BrushKSiteEwmaAlpha
    if v > 0 && v < 1 {
        return v
    }
    return ksiteEwmaAlpha
}

func timeBucket(ts int64) int {
    if ts <= 0 {
        ts = time.Now().Unix()
    }
    t := time.Unix(ts, 0)
    h := t.Hour()
    bh := getBucketHours()
    if bh <= 0 {
        bh = ksiteTimeBucketHours
    }
    // number of buckets per day
    buckets := int(math.Ceil(24.0 / float64(bh)))
    if buckets <= 0 {
        buckets = 6
    }
    b := int(float64(h) / float64(bh))
    if b < 0 {
        b = 0
    }
    if b >= buckets {
        b = buckets - 1
    }
    return b
}

// EstimateKSite returns learned k_site (bytes/s per unit demand) for site at ts bucket.
// Falls back to default when insufficient samples.
func EstimateKSite(site string, ts int64) int64 {
    if site == "" {
        return ksiteDefaultK
    }
    ksiteMu.Lock()
    defer ksiteMu.Unlock()
    loadKSite()
    bucket := timeBucket(ts)
    for _, e := range ksite.Entries {
        if e.Site == site && e.Bucket == bucket && e.N >= ksiteMinSamplesForRead && e.K > 0 {
            return int64(math.Round(e.K))
        }
    }
    return ksiteDefaultK
}

// LearnKSite updates k_site using a new sample.
// demandMilli: demand scaled by 1000 (i.e., 1000 * L/(S+1)).
// avgUploadSpeed: bytes/s average upload speed of the torrent lifespan.
func LearnKSite(site string, atime int64, demandMilli int64, avgUploadSpeed int64) {
    if site == "" || demandMilli <= 0 || avgUploadSpeed <= 0 {
        return
    }
    ksiteMu.Lock()
    defer ksiteMu.Unlock()
    loadKSite()
    bucket := timeBucket(atime)
    sample := float64(avgUploadSpeed) * 1000.0 / float64(demandMilli)
    // clamp sample to a sane range: [1KiB, 100MiB]
    if sample < 1024 {
        sample = 1024
    }
    if sample > 100*1024*1024 {
        sample = 100 * 1024 * 1024
    }
    // find or create entry
    idx := -1
    for i, e := range ksite.Entries {
        if e.Site == site && e.Bucket == bucket {
            idx = i
            break
        }
    }
    if idx == -1 {
        ksite.Entries = append(ksite.Entries, ksiteEntry{
            Site:    site,
            Bucket:  bucket,
            K:       sample,
            N:       1,
            Updated: time.Now().Unix(),
        })
    } else {
        e := &ksite.Entries[idx]
        alpha := getEwmaAlpha()
        if e.N < 5 {
            // Use higher alpha when low samples to converge faster
            alpha = 0.5
        }
        e.K = e.K*(1.0-alpha) + sample*alpha
        e.N++
        e.Updated = time.Now().Unix()
    }
    saveKSite()
}
