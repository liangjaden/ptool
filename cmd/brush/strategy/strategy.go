package strategy

import (
    "fmt"
    "math"
    "sort"
    "strings"

    log "github.com/sirupsen/logrus"

    "github.com/sagan/ptool/client"
    "github.com/sagan/ptool/site"
    "github.com/sagan/ptool/util"
    "github.com/sagan/ptool/stats"
)

const (
	// new torrents timespan during which will NOT be examined at all
	NEW_TORRENTS_TIMESPAN = int64(15 * 60)
	// new torrents timespan during which will NOT be stalled
	NEW_TORRENTS_STALL_EXEMPTION_TIMESPAN = int64(30 * 60)
	NO_PROCESS_TORRENT_DELETEION_TIMESPAN = int64(30 * 60)
	STALL_DOWNLOAD_SPEED                  = int64(10 * 1024)
	SLOW_UPLOAD_SPEED                     = int64(100 * 1024)
	RATIO_CHECK_MIN_DOWNLOAD_SPEED        = int64(100 * 1024)
	SLOW_TORRENTS_CHECK_TIMESPAN          = int64(15 * 60)
	// stalled torrent will be deleted after this time passed
	STALL_TORRENT_DELETEION_TIMESPAN     = int64(30 * 60)
	BANDWIDTH_FULL_PERCENT               = float64(0.8)
    DELETE_TORRENT_IMMEDIATELY_SCORE     = float64(99999)
    RESUME_TORRENTS_FREE_DISK_SPACE_TIER = int64(5 * 1024 * 1024 * 1024)  // 5GB
    DELETE_TORRENTS_FREE_DISK_SPACE_TIER = int64(10 * 1024 * 1024 * 1024) // 10GB
)

// --- Brush scoring v2 defaults (can be tuned later via config, for now constants) ---
const (
    // demand factor: min(Dmax, L/(S+1)^alpha)/Dmax
    SCORE_DEMAND_ALPHA   = 0.7
    SCORE_DEMAND_MAX     = 50.0

    // age factor: exp(- max(0, (now-time)-t0) / tau). Units: seconds
    SCORE_AGE_T0_SECONDS        = 20 * 60        // 20min
    SCORE_AGE_TAU_SECONDS       = 3 * 3600       // 3h
    SCORE_AGE_TAU_ANYFREE       = 3 * 3600       // AcceptAnyFree=true 时同样默认 3h
    SCORE_AGE_TAU_STRICT_FREE   = 2 * 3600       // AcceptAnyFree=false 时更严格

    // size factor breakpoints (GiB)
    SIZE_BP_SMALL_GIB    = 1.0
    SIZE_BP_IDEAL_MIN_GIB = 2.0
    SIZE_BP_IDEAL_MAX_GIB = 15.0
    SIZE_BP_MED_MAX_GIB  = 20.0
    SIZE_BP_LARGE_MAX_GIB = 60.0

    // size factor weights at key points
    SIZE_SCORE_TINY   = 0.6
    SIZE_SCORE_IDEAL  = 1.0
    SIZE_SCORE_MED    = 0.8
    SIZE_SCORE_LARGE  = 0.5
    SIZE_SCORE_HUGE   = 0.2

    // extra boost for huge packs with very high leechers
    HUGE_L_BOOST_1 = 100
    HUGE_L_BOOST_2 = 500
    HUGE_L_BOOST_3 = 1000
    HUGE_BOOST_1   = 2.0
    HUGE_BOOST_2   = 5.0
    HUGE_BOOST_3   = 10.0

    // discount factor window (seconds)
    SCORE_DISCOUNT_WINDOW = 4 * 3600

    // non-free penalty
    SCORE_NONFREE_PENALTY = 0.6

    // snatched smoothing param
    SCORE_SNATCH_K = 200.0

    // composition exponents
    SCORE_W_DEMAND   = 0.7
    SCORE_W_AGE      = 0.5
    SCORE_W_SIZE     = 0.3
    SCORE_W_DISCOUNT = 0.5

    // additive snatch weight
    SCORE_SNATCH_ADD = 0.1
)

type BrushSiteOptionStruct struct {
    AllowNoneFree           bool
    AllowPaid               bool
    AllowHr                 bool
    AllowZeroSeeders        bool
    TorrentUploadSpeedLimit int64
    TorrentMinSizeLimit     int64
    TorrentMaxSizeLimit     int64
    Now                     int64
    Excludes                []string
    ExcludeTags             []string
    AllowAddTorrents        int64
    AcceptAnyFree           bool
    // Per-site quota support
    SiteName                string
    SiteQuotaLimit          int64 // 0 or negative => no limit
    // Scoring control
    ScoreVersion            string
    ScoreV2                 *BrushScoreV2Params
}

// BrushScoreV2Params groups tunables for v2 scoring. When nil, v2 is disabled.
type BrushScoreV2Params struct {
    DemandAlpha  float64
    DemandMax    float64
    AgeT0Seconds int64
    AgeTauSeconds int64
    AgeTauStrictSeconds int64

    SizeBpSmallGiB     float64
    SizeBpIdealMinGiB  float64
    SizeBpIdealMaxGiB  float64
    SizeBpMedMaxGiB    float64
    SizeBpLargeMaxGiB  float64
    SizeScoreTiny      float64
    SizeScoreIdeal     float64
    SizeScoreMed       float64
    SizeScoreLarge     float64
    SizeScoreHuge      float64

    HugeLBoost1 int64
    HugeLBoost2 int64
    HugeLBoost3 int64
    HugeBoost1  float64
    HugeBoost2  float64
    HugeBoost3  float64

    DiscountWindowSeconds int64
    NonfreePenalty        float64
    SnatchK               float64
    SnatchAdd             float64
    WDemand               float64
    WAge                  float64
    WSize                 float64
    WDiscount             float64
}

type BrushClientOptionStruct struct {
	MinDiskSpace            int64
	SlowUploadSpeedTier     int64
	MaxDownloadingTorrents  int64
	MaxTorrents             int64
	MinRatio                float64
	DefaultUploadSpeedLimit int64
}

type AlgorithmAddTorrent struct {
	DownloadUrl string
	Name        string
	Meta        map[string]int64
	Msg         string
}

type AlgorithmModifyTorrent struct {
	InfoHash string
	Name     string
	Meta     map[string]int64
	Msg      string
}

type AlgorithmOperationTorrent struct {
	InfoHash string
	Name     string
	Msg      string
}

type AlgorithmResult struct {
	DeleteTorrents  []AlgorithmOperationTorrent // torrents that will be removed from client
	StallTorrents   []AlgorithmModifyTorrent    // torrents that will stop downloading but still uploading
	ResumeTorrents  []AlgorithmOperationTorrent // resume paused / errored torrents
	ModifyTorrents  []AlgorithmModifyTorrent    // modify meta info of these torrents
	AddTorrents     []AlgorithmAddTorrent       // new torrents that will be added to client
	CanAddMore      bool                        // client is able to add more torrents
	FreeSpaceChange int64                       // estimated free space change after apply above operations
	Msg             string
}

type candidateTorrentStruct struct {
	Name                  string
	DownloadUrl           string
	Size                  int64
	PredictionUploadSpeed int64
	Score                 float64
	Meta                  map[string]int64
}

type candidateClientTorrentStruct struct {
	InfoHash    string
	Score       float64
	FutureValue int64 // 预期的该种子未来的刷流上传价值
	Msg         string
}

type clientTorrentInfoStruct struct {
	Torrent             *client.Torrent
	ModifyFlag          bool
	StallFlag           bool
	ResumeFlag          bool
	DeleteCandidateFlag bool
	DeleteFlag          bool
}

func countAsDownloading(torrent *client.Torrent, now int64) bool {
	return !torrent.IsComplete() && torrent.Meta["stt"] == 0 &&
		(torrent.DownloadSpeed >= STALL_DOWNLOAD_SPEED || now-torrent.Atime <= NEW_TORRENTS_TIMESPAN)
}

func canStallTorrent(torrent *client.Torrent) bool {
	return torrent.State == "downloading" && torrent.Meta["stt"] == 0
}

func isTorrentStalled(torrent *client.Torrent) bool {
	return !torrent.IsComplete() && torrent.Meta["stt"] > 0
}

/*
 * @todo : this function requires a major rework. It's a mess right now.
 *
 * Strategy (Desired)
 * Delete a torrent from client when (any of the the follow criterion matches):
 *   a. Tt's uploading speed become SLOW enough AND free disk space insufficient
 *   b. It's consuming too much downloading bandwidth and uploading / downloading speed ratio is too low
 *   c. It's incomplete and been totally stalled (no uploading or downloading activity) for some time
 *   d. It's incomplete and the free discount expired (or will soon expire)
 * Stall ALL incomplete torrent of client (limit download speed to 1B/s, so upload only)
 *   when free disk space insufficient
 *   * This's somwwhat broken in qBittorrent for now (See https://github.com/qbittorrent/qBittorrent/issues/2185 ).
 *   * Simply limiting downloading speed (to a very low tier) will also drop uploading speed to the same level
 *   * Consider removing this behavior
 * Add new torrents to client when server uploading and downloading bandwidth is somewhat idle AND
 *   there is SOME free disk space
 * Also：
 *   * Use the current seeders / leechers info of torrent when make decisions
 */
func Decide(clientStatus *client.Status, clientTorrents []*client.Torrent, siteTorrents []*site.Torrent,
    siteOption *BrushSiteOptionStruct, clientOption *BrushClientOptionStruct) (result *AlgorithmResult) {
    result = &AlgorithmResult{}

	cntTorrents := int64(len(clientTorrents))
	cntDownloadingTorrents := int64(0)
	freespace := clientStatus.FreeSpaceOnDisk
	freespaceChange := int64(0)
	freespaceTarget := min(clientOption.MinDiskSpace*2, clientOption.MinDiskSpace+DELETE_TORRENTS_FREE_DISK_SPACE_TIER)
	estimateUploadSpeed := clientStatus.UploadSpeed

    var candidateTorrents []candidateTorrentStruct
    var modifyTorrents []AlgorithmModifyTorrent
    var stallTorrents []AlgorithmModifyTorrent
    var resumeTorrents []AlgorithmOperationTorrent
    var deleteCandidateTorrents []candidateClientTorrentStruct
    clientTorrentsMap := map[string]*clientTorrentInfoStruct{}
    siteTorrentsMap := map[string]*site.Torrent{}

	targetUploadSpeed := clientStatus.UploadSpeedLimit
	if targetUploadSpeed <= 0 {
		targetUploadSpeed = clientOption.DefaultUploadSpeedLimit
	}

	for i, torrent := range clientTorrents {
		clientTorrentsMap[torrent.InfoHash] = &clientTorrentInfoStruct{
			Torrent: clientTorrents[i],
		}
	}
    for i, siteTorrent := range siteTorrents {
        siteTorrentsMap[siteTorrent.InfoHash] = siteTorrents[i]
    }

    // Per-site quota tracking
    siteQuotaLimit := siteOption.SiteQuotaLimit
    siteName := siteOption.SiteName
    var siteQuotaUsed int64 = 0
    var siteQuotaDelta int64 = 0 // negative when deleting; positive when adding
    if siteQuotaLimit > 0 && siteName != "" {
        for _, torrent := range clientTorrents {
            if torrent.GetSiteFromTag() == siteName {
                siteQuotaUsed += torrent.Size
            }
        }
    }

    for _, siteTorrent := range siteTorrents {
        score, predictionUploadSpeed, _ := RateSiteTorrent(siteTorrent, siteOption)
        if score > 0 {
            candidateTorrent := candidateTorrentStruct{
                Name:                  siteTorrent.Name,
                Size:                  siteTorrent.Size,
                DownloadUrl:           siteTorrent.DownloadUrl,
                PredictionUploadSpeed: predictionUploadSpeed,
                Score:                 score,
                Meta:                  map[string]int64{},
            }
            if siteTorrent.DiscountEndTime > 0 {
                candidateTorrent.Meta["dcet"] = siteTorrent.DiscountEndTime
            }
            // Record selection-time S/L features for later k_site learning
            candidateTorrent.Meta["bl"] = siteTorrent.Leechers
            candidateTorrent.Meta["bs"] = siteTorrent.Seeders
            // demand milli = round(1000 * L/(S+1))
            denom := siteTorrent.Seeders + 1
            if denom <= 0 {
                denom = 1
            }
            dm := (float64(siteTorrent.Leechers) / float64(denom)) * 1000.0
            if dm < 0 {
                dm = 0
            }
            candidateTorrent.Meta["bdm"] = int64(math.Round(dm))
            candidateTorrents = append(candidateTorrents, candidateTorrent)
        }
    }
	sort.SliceStable(candidateTorrents, func(i, j int) bool {
		return candidateTorrents[i].Score > candidateTorrents[j].Score
	})

	// mark torrents
	for _, torrent := range clientTorrents {
		if countAsDownloading(torrent, siteOption.Now) {
			cntDownloadingTorrents++
		}

		// mark torrents that discount time ends as stall
		if torrent.Meta["dcet"] > 0 && torrent.Meta["dcet"]-siteOption.Now <= 3600 && torrent.Ctime <= 0 {
			if canStallTorrent(torrent) {
				meta := util.CopyMap(torrent.Meta, true)
				meta["stt"] = siteOption.Now
				stallTorrents = append(stallTorrents, AlgorithmModifyTorrent{
					InfoHash: torrent.InfoHash,
					Name:     torrent.Name,
					Msg:      "discount time ends",
					Meta:     meta,
				})
				clientTorrentsMap[torrent.InfoHash].StallFlag = true
			}
		}

		// skip new added torrents
		if siteOption.Now-torrent.Atime <= NEW_TORRENTS_TIMESPAN {
			continue
		}

		if torrent.State == "error" && (torrent.UploadSpeed < clientOption.SlowUploadSpeedTier ||
			torrent.UploadSpeed < clientOption.SlowUploadSpeedTier*2 && freespace == 0) &&
			len(candidateTorrents) > 0 {
			deleteCandidateTorrents = append(deleteCandidateTorrents, candidateClientTorrentStruct{
				InfoHash:    torrent.InfoHash,
				Score:       DELETE_TORRENT_IMMEDIATELY_SCORE,
				FutureValue: 0,
				Msg:         "torrent in error state",
			})
			clientTorrentsMap[torrent.InfoHash].DeleteCandidateFlag = true
		} else if torrent.DownloadSpeed == 0 && torrent.SizeCompleted == 0 {
			if siteOption.Now-torrent.Atime > NO_PROCESS_TORRENT_DELETEION_TIMESPAN {
				deleteCandidateTorrents = append(deleteCandidateTorrents, candidateClientTorrentStruct{
					InfoHash:    torrent.InfoHash,
					Score:       DELETE_TORRENT_IMMEDIATELY_SCORE,
					FutureValue: 0,
					Msg:         "torrent has no download proccess",
				})
				clientTorrentsMap[torrent.InfoHash].DeleteCandidateFlag = true
			}
		} else if torrent.UploadSpeed < clientOption.SlowUploadSpeedTier {
			// check slow torrents, add it to watch list first time and mark as deleteCandidate second time
			if torrent.Meta["sct"] > 0 { // second encounter on slow torrent
				if siteOption.Now-torrent.Meta["sct"] >= SLOW_TORRENTS_CHECK_TIMESPAN {
					averageUploadSpeedSinceSct := (torrent.Uploaded - torrent.Meta["sctu"]) /
						(siteOption.Now - torrent.Meta["sct"])
					if averageUploadSpeedSinceSct < clientOption.SlowUploadSpeedTier {
						if canStallTorrent(torrent) &&
							torrent.DownloadSpeed >= RATIO_CHECK_MIN_DOWNLOAD_SPEED &&
							float64(torrent.UploadSpeed)/float64(torrent.DownloadSpeed) < clientOption.MinRatio &&
							siteOption.Now-torrent.Atime >= NEW_TORRENTS_STALL_EXEMPTION_TIMESPAN {
							meta := util.CopyMap(torrent.Meta, true)
							meta["stt"] = siteOption.Now
							stallTorrents = append(stallTorrents, AlgorithmModifyTorrent{
								InfoHash: torrent.InfoHash,
								Name:     torrent.Name,
								Msg:      "low upload / download ratio",
								Meta:     meta,
							})
							clientTorrentsMap[torrent.InfoHash].StallFlag = true
						}
						score := -float64(torrent.UploadSpeed)
						if torrent.Ctime <= 0 {
							if torrent.Meta["stt"] > 0 {
								score += float64(siteOption.Now) - float64(torrent.Meta["stt"])
							}
						} else {
							score += math.Min(float64(siteOption.Now-torrent.Ctime), 86400)
						}
						deleteCandidateTorrents = append(deleteCandidateTorrents, candidateClientTorrentStruct{
							InfoHash:    torrent.InfoHash,
							Score:       score,
							FutureValue: torrent.UploadSpeed,
							Msg:         "slow uploading speed",
						})
						clientTorrentsMap[torrent.InfoHash].DeleteCandidateFlag = true
					} else {
						meta := util.CopyMap(torrent.Meta, true)
						meta["sct"] = siteOption.Now
						meta["sctu"] = torrent.Uploaded
						modifyTorrents = append(modifyTorrents, AlgorithmModifyTorrent{
							InfoHash: torrent.InfoHash,
							Name:     torrent.Name,
							Msg:      "reset slow check time mark",
							Meta:     meta,
						})
						clientTorrentsMap[torrent.InfoHash].ModifyFlag = true
					}
				}
			} else { // first encounter on slow torrent
				meta := util.CopyMap(torrent.Meta, true)
				meta["sct"] = siteOption.Now
				meta["sctu"] = torrent.Uploaded
				modifyTorrents = append(modifyTorrents, AlgorithmModifyTorrent{
					InfoHash: torrent.InfoHash,
					Name:     torrent.Name,
					Msg:      "set slow check time mark",
					Meta:     meta,
				})
				clientTorrentsMap[torrent.InfoHash].ModifyFlag = true
			}
		} else if torrent.Meta["sct"] > 0 { // remove mark on no-longer slow torrents
			meta := util.CopyMap(torrent.Meta, true)
			delete(meta, "sct")
			delete(meta, "sctu")
			modifyTorrents = append(modifyTorrents, AlgorithmModifyTorrent{
				InfoHash: torrent.InfoHash,
				Name:     torrent.Name,
				Msg:      "remove slow check time mark",
				Meta:     meta,
			})
			clientTorrentsMap[torrent.InfoHash].ModifyFlag = true
		}
	}
	sort.SliceStable(deleteCandidateTorrents, func(i, j int) bool {
		return deleteCandidateTorrents[i].Score > deleteCandidateTorrents[j].Score
	})

	// @todo: use Dynamic Programming to better find torrents suitable for delete
	// delete torrents
    for _, deleteTorrent := range deleteCandidateTorrents {
        torrent := clientTorrentsMap[deleteTorrent.InfoHash].Torrent
        shouldDelete := false
        if deleteTorrent.Score >= DELETE_TORRENT_IMMEDIATELY_SCORE ||
            (freespace >= 0 && freespace <= clientOption.MinDiskSpace && freespace+freespaceChange <= freespaceTarget) {
            shouldDelete = true
        } else if torrent.Ctime <= 0 &&
            torrent.Meta["stt"] > 0 &&
            siteOption.Now-torrent.Meta["stt"] >= STALL_TORRENT_DELETEION_TIMESPAN {
            shouldDelete = true
        }
        if !shouldDelete {
            continue
        }
        result.DeleteTorrents = append(result.DeleteTorrents, AlgorithmOperationTorrent{
            InfoHash: torrent.InfoHash,
            Name:     torrent.Name,
            Msg:      deleteTorrent.Msg,
        })
        freespaceChange += torrent.SizeCompleted
        estimateUploadSpeed -= torrent.UploadSpeed
        clientTorrentsMap[torrent.InfoHash].DeleteFlag = true
        if siteQuotaLimit > 0 && siteName != "" && torrent.GetSiteFromTag() == siteName {
            siteQuotaDelta -= torrent.Size
        }
        if countAsDownloading(torrent, siteOption.Now) {
            cntDownloadingTorrents--
        }
        cntTorrents--
    }

	// if still not enough free space, delete ALL stalled incomplete torrents
    if freespace >= 0 && freespace <= clientOption.MinDiskSpace && freespace+freespaceChange <= freespaceTarget {
        for _, torrent := range clientTorrents {
            if clientTorrentsMap[torrent.InfoHash].DeleteFlag || !isTorrentStalled(torrent) {
                continue
            }
            result.DeleteTorrents = append(result.DeleteTorrents, AlgorithmOperationTorrent{
                InfoHash: torrent.InfoHash,
                Name:     torrent.Name,
                Msg:      "delete stalled incomplete torrents due to insufficient disk space",
            })
            freespaceChange += torrent.SizeCompleted
            estimateUploadSpeed -= torrent.UploadSpeed
            clientTorrentsMap[torrent.InfoHash].DeleteFlag = true
            if siteQuotaLimit > 0 && siteName != "" && torrent.GetSiteFromTag() == siteName {
                siteQuotaDelta -= torrent.Size
            }
            if countAsDownloading(torrent, siteOption.Now) {
                cntDownloadingTorrents--
            }
            cntTorrents--
        }
    }

	// delete torrents due to max brush torrents limit
	cntDeleteDueToMaxTorrents := max(cntTorrents-clientOption.MaxTorrents, -siteOption.AllowAddTorrents)
	if cntDeleteDueToMaxTorrents > 0 && len(candidateTorrents) > 0 {
		for _, deleteTorrent := range deleteCandidateTorrents {
			torrent := clientTorrentsMap[deleteTorrent.InfoHash].Torrent
			if clientTorrentsMap[torrent.InfoHash].DeleteFlag {
				continue
			}
			result.DeleteTorrents = append(result.DeleteTorrents, AlgorithmOperationTorrent{
				InfoHash: torrent.InfoHash,
				Name:     torrent.Name,
				Msg:      deleteTorrent.Msg + " (delete due to max torrents limit)",
			})
			freespaceChange += torrent.SizeCompleted
			estimateUploadSpeed -= torrent.UploadSpeed
			clientTorrentsMap[torrent.InfoHash].DeleteFlag = true
			if countAsDownloading(torrent, siteOption.Now) {
				cntDownloadingTorrents--
			}
			cntTorrents--
			cntDeleteDueToMaxTorrents--
			if cntDeleteDueToMaxTorrents == 0 {
				break
			}
		}
	}

	// if still not enough free space, mark ALL torrents as stall
	if freespace >= 0 && freespace+freespaceChange < clientOption.MinDiskSpace {
		for _, torrent := range clientTorrents {
			if clientTorrentsMap[torrent.InfoHash].DeleteFlag || clientTorrentsMap[torrent.InfoHash].StallFlag {
				continue
			}
			if canStallTorrent(torrent) {
				meta := util.CopyMap(torrent.Meta, true)
				meta["stt"] = siteOption.Now
				stallTorrents = append(stallTorrents, AlgorithmModifyTorrent{
					InfoHash: torrent.InfoHash,
					Name:     torrent.Name,
					Msg:      "stall all torrents due to insufficient free disk space",
					Meta:     meta,
				})
				clientTorrentsMap[torrent.InfoHash].StallFlag = true
			}
		}
	}

	// mark torrents as resume
	if freespace+freespaceChange >= max(clientOption.MinDiskSpace, RESUME_TORRENTS_FREE_DISK_SPACE_TIER) {
		for _, torrent := range clientTorrents {
			if torrent.State != "error" || torrent.UploadSpeed < clientOption.SlowUploadSpeedTier*4 ||
				isTorrentStalled(torrent) || clientTorrentsMap[torrent.InfoHash].ResumeFlag {
				continue
			}
			resumeTorrents = append(resumeTorrents, AlgorithmOperationTorrent{
				InfoHash: torrent.InfoHash,
				Name:     torrent.Name,
				Msg:      "resume fast uploading errored torrent",
			})
			clientTorrentsMap[torrent.InfoHash].ResumeFlag = true
		}
	}

	// stall torrents
	for _, stallTorrent := range stallTorrents {
		if clientTorrentsMap[stallTorrent.InfoHash].DeleteFlag {
			continue
		}
		result.StallTorrents = append(result.StallTorrents, stallTorrent)
		if countAsDownloading(clientTorrentsMap[stallTorrent.InfoHash].Torrent, siteOption.Now) {
			cntDownloadingTorrents--
		}
	}

	// resume torrents
	for _, resumeTorrent := range resumeTorrents {
		if clientTorrentsMap[resumeTorrent.InfoHash].DeleteFlag ||
			clientTorrentsMap[resumeTorrent.InfoHash].StallFlag {
			continue
		}
		result.ResumeTorrents = append(result.ResumeTorrents, resumeTorrent)
		if !countAsDownloading(clientTorrentsMap[resumeTorrent.InfoHash].Torrent, siteOption.Now) {
			cntDownloadingTorrents++
		}
	}

	// modify torrents
	for _, modifyTorrent := range modifyTorrents {
		if clientTorrentsMap[modifyTorrent.InfoHash].DeleteFlag || clientTorrentsMap[modifyTorrent.InfoHash].StallFlag {
			continue
		}
		result.ModifyTorrents = append(result.ModifyTorrents, modifyTorrent)
	}

	// add new torrents
    if (freespace == -1 || freespace+freespaceChange > clientOption.MinDiskSpace) &&
        cntTorrents <= clientOption.MaxTorrents {
        var added int64
        for cntDownloadingTorrents < clientOption.MaxDownloadingTorrents &&
            estimateUploadSpeed <= targetUploadSpeed*2 && len(candidateTorrents) > 0 &&
            added < siteOption.AllowAddTorrents {
            candidateTorrent := candidateTorrents[0]
            candidateTorrents = candidateTorrents[1:]
            // Enforce per-site quota by deleting slow torrents of the same site when needed
            if siteQuotaLimit > 0 && siteName != "" {
                // current available capacity for this site
                siteAvailable := siteQuotaLimit - (siteQuotaUsed + siteQuotaDelta)
                if candidateTorrent.Size > siteAvailable {
                    required := candidateTorrent.Size - siteAvailable
                    // Try to free space by deleting slow/stalled torrents of this site
                    for _, del := range deleteCandidateTorrents {
                        if required <= 0 {
                            break
                        }
                        info := clientTorrentsMap[del.InfoHash]
                        if info == nil || info.DeleteFlag {
                            continue
                        }
                        t := info.Torrent
                        if t.GetSiteFromTag() != siteName {
                            continue
                        }
                        // delete
                        result.DeleteTorrents = append(result.DeleteTorrents, AlgorithmOperationTorrent{
                            InfoHash: t.InfoHash,
                            Name:     t.Name,
                            Msg:      del.Msg + " (delete due to site quota)",
                        })
                        freespaceChange += t.SizeCompleted
                        estimateUploadSpeed -= t.UploadSpeed
                        info.DeleteFlag = true
                        if countAsDownloading(t, siteOption.Now) {
                            cntDownloadingTorrents--
                        }
                        cntTorrents--
                        // update site quota tracking
                        siteQuotaDelta -= t.Size
                        required -= t.Size
                    }
                    // If still not enough, try delete stalled incomplete torrents of this site
                    if required > 0 {
                        for _, info := range clientTorrentsMap {
                            if required <= 0 {
                                break
                            }
                            if info == nil || info.DeleteFlag {
                                continue
                            }
                            t := info.Torrent
                            if t.GetSiteFromTag() != siteName || !isTorrentStalled(t) {
                                continue
                            }
                            result.DeleteTorrents = append(result.DeleteTorrents, AlgorithmOperationTorrent{
                                InfoHash: t.InfoHash,
                                Name:     t.Name,
                                Msg:      "delete stalled torrent due to site quota",
                            })
                            freespaceChange += t.SizeCompleted
                            estimateUploadSpeed -= t.UploadSpeed
                            info.DeleteFlag = true
                            if countAsDownloading(t, siteOption.Now) {
                                cntDownloadingTorrents--
                            }
                            cntTorrents--
                            siteQuotaDelta -= t.Size
                            required -= t.Size
                        }
                    }
                    // If still not enough, skip this candidate and try next one
                    siteAvailable = siteQuotaLimit - (siteQuotaUsed + siteQuotaDelta)
                    if candidateTorrent.Size > siteAvailable {
                        // cannot free enough space for this candidate; skip it
                        continue
                    }
                }
            }
            result.AddTorrents = append(result.AddTorrents, AlgorithmAddTorrent{
                DownloadUrl: candidateTorrent.DownloadUrl,
                Name:        candidateTorrent.Name,
                Meta:        candidateTorrent.Meta,
                Msg:         fmt.Sprintf("new torrrent of score %.0f", candidateTorrent.Score),
            })
            added++
            cntTorrents++
            cntDownloadingTorrents++
            estimateUploadSpeed += candidateTorrent.PredictionUploadSpeed
            if siteQuotaLimit > 0 && siteName != "" {
                siteQuotaDelta += candidateTorrent.Size
            }
        }
    }

	result.FreeSpaceChange = freespaceChange

	if cntTorrents <= clientOption.MaxTorrents &&
		cntDownloadingTorrents < clientOption.MaxDownloadingTorrents &&
		estimateUploadSpeed <= targetUploadSpeed*2 &&
		(freespace == -1 || freespace+freespaceChange > clientOption.MinDiskSpace) {
		result.CanAddMore = true
	}

	return
}

func RateSiteTorrent(siteTorrent *site.Torrent, siteOption *BrushSiteOptionStruct) (
    score float64, predictionUploadSpeed int64, note string) {
    // Dispatch by score version; default to v1 when not explicitly "v2"
    if strings.EqualFold(siteOption.ScoreVersion, "v2") && siteOption.ScoreV2 != nil {
        return rateSiteTorrentV2(siteTorrent, siteOption)
    }
    return rateSiteTorrentV1(siteTorrent, siteOption)
}

// Legacy V1 scoring (original behavior, default)
func rateSiteTorrentV1(siteTorrent *site.Torrent, siteOption *BrushSiteOptionStruct) (
    score float64, predictionUploadSpeed int64, note string) {
    if log.GetLevel() >= log.TraceLevel {
        defer func() {
            log.Tracef("rateSiteTorrent[v1] score=%0.0f name=%s, free=%t, rtime=%d, seeders=%d, leechers=%d, note=%s",
                score,
                siteTorrent.Name,
                siteTorrent.DownloadMultiplier == 0,
                siteOption.Now-siteTorrent.Time,
                siteTorrent.Seeders,
                siteTorrent.Leechers,
                note,
            )
        }()
    }
    if siteTorrent.IsActive || siteTorrent.UploadMultiplier == 0 ||
        (!siteOption.AllowHr && siteTorrent.HasHnR) ||
        (!siteOption.AllowNoneFree && siteTorrent.DownloadMultiplier != 0) ||
        (!siteOption.AllowPaid && siteTorrent.Paid && !siteTorrent.Bought) ||
        siteTorrent.Size < siteOption.TorrentMinSizeLimit ||
        siteTorrent.Size > siteOption.TorrentMaxSizeLimit ||
        (siteTorrent.DiscountEndTime > 0 && siteTorrent.DiscountEndTime-siteOption.Now < 3600) ||
        (!siteOption.AllowZeroSeeders && siteTorrent.Seeders == 0) ||
        ((!siteOption.AcceptAnyFree || siteTorrent.DownloadMultiplier != 0) && siteTorrent.Leechers <= siteTorrent.Seeders) {
        score = 0
        return
    }
    if siteTorrent.MatchFiltersOr(siteOption.Excludes) {
        score = 0
        note = "brush excludes match"
        return
    }
    if siteTorrent.HasAnyTag(siteOption.ExcludeTags) {
        score = 0
        note = "brush exclude tags match"
        return
    }
    // 部分站点定期将旧种重新置顶免费。这类种子仍然可以获得很好的上传速度。
    if !siteOption.AcceptAnyFree && siteOption.Now-siteTorrent.Time <= 86400*30 {
        if siteOption.Now-siteTorrent.Time >= 86400 {
            score = 0
            return
        } else if siteOption.Now-siteTorrent.Time >= 7200 {
            if siteTorrent.Leechers < 500 {
                score = 0
                return
            }
        }
    }

    predictionUploadSpeed = siteTorrent.Leechers * 100 * 1024
    if predictionUploadSpeed > siteOption.TorrentUploadSpeedLimit {
        predictionUploadSpeed = siteOption.TorrentUploadSpeedLimit
    }

    if siteTorrent.Seeders <= 1 {
        score = 50
    } else if siteTorrent.Seeders <= 3 {
        score = 30
    } else {
        score = 10
    }
    score += float64(siteTorrent.Leechers)

    score *= siteTorrent.UploadMultiplier
    if siteTorrent.DownloadMultiplier != 0 {
        score *= 0.5
    }

    if siteTorrent.Size <= 1024*1024*1024 {
        score *= 10
    } else if siteTorrent.Size <= 1024*1024*1024*10 {
        score *= 2
    } else if siteTorrent.Size <= 1024*1024*1024*20 {
        score *= 1
    } else if siteTorrent.Size <= 1024*1024*1024*50 {
        score *= 0.5
    } else if siteTorrent.Size <= 1024*1024*1024*100 {
        score *= 0.1
    } else {
        // 大包特殊处理
        if siteTorrent.Leechers >= 1000 {
            score *= 100
        } else if siteTorrent.Leechers >= 500 {
            score *= 50
        } else if siteTorrent.Leechers >= 100 {
            score *= 10
        } else {
            score *= 0
        }
    }
    return
}

// V2 scoring implementation
func rateSiteTorrentV2(siteTorrent *site.Torrent, siteOption *BrushSiteOptionStruct) (
    score float64, predictionUploadSpeed int64, note string) {
    if log.GetLevel() >= log.TraceLevel {
        defer func() {
            log.Tracef("rateSiteTorrent[v2] score=%0.2f name=%s, free=%t, rtime=%d, seeders=%d, leechers=%d, note=%s",
                score,
                siteTorrent.Name,
                siteTorrent.DownloadMultiplier == 0,
                siteOption.Now-siteTorrent.Time,
                siteTorrent.Seeders,
                siteTorrent.Leechers,
                note,
            )
        }()
    }

    p := siteOption.ScoreV2
    if p == nil {
        // should not happen; fallback to v1
        return rateSiteTorrentV1(siteTorrent, siteOption)
    }

    // 1) 硬过滤（保守安全）
    if siteTorrent.IsActive || siteTorrent.UploadMultiplier == 0 ||
        (!siteOption.AllowHr && siteTorrent.HasHnR) ||
        (!siteOption.AllowPaid && siteTorrent.Paid && !siteTorrent.Bought) ||
        siteTorrent.Size < siteOption.TorrentMinSizeLimit ||
        siteTorrent.Size > siteOption.TorrentMaxSizeLimit ||
        (!siteOption.AllowZeroSeeders && siteTorrent.Seeders == 0) ||
        ((!siteOption.AcceptAnyFree || siteTorrent.DownloadMultiplier != 0) && siteTorrent.Leechers <= siteTorrent.Seeders) {
        score = 0
        return
    }
    if !siteOption.AllowNoneFree && siteTorrent.DownloadMultiplier != 0 {
        score = 0
        note = "nonfree not allowed"
        return
    }
    if siteTorrent.MatchFiltersOr(siteOption.Excludes) {
        score = 0
        note = "brush excludes match"
        return
    }
    if siteTorrent.HasAnyTag(siteOption.ExcludeTags) {
        score = 0
        note = "brush exclude tags match"
        return
    }
    // 折扣将尽的极端情况：10 分钟内结束，避免踩线
    if siteTorrent.DiscountEndTime > 0 && siteTorrent.DiscountEndTime-siteOption.Now <= 10*60 {
        score = 0
        note = "discount ends too soon"
        return
    }

    // 2) 因子计算
    l := float64(siteTorrent.Leechers)
    s := float64(siteTorrent.Seeders)
    demand := l / math.Pow(s+1.0, firstNonZeroFloat(p.DemandAlpha, SCORE_DEMAND_ALPHA))
    dmax := firstNonZeroFloat(p.DemandMax, SCORE_DEMAND_MAX)
    if demand > dmax {
        demand = dmax
    }
    fDemand := demand / dmax

    // age factor
    age := float64(siteOption.Now - siteTorrent.Time)
    if age < 0 {
        age = 0
    }
    t0 := float64(firstNonZeroInt(p.AgeT0Seconds, SCORE_AGE_T0_SECONDS))
    tau := float64(firstNonZeroInt(p.AgeTauSeconds, SCORE_AGE_TAU_SECONDS))
    if !siteOption.AcceptAnyFree {
        tau = float64(firstNonZeroInt(p.AgeTauStrictSeconds, SCORE_AGE_TAU_STRICT_FREE))
    }
    fAge := math.Exp(-math.Max(0, age-t0)/tau)

    // size factor
    sizeGiB := float64(siteTorrent.Size) / (1024.0 * 1024.0 * 1024.0)
    bpSmall := firstNonZeroFloat(p.SizeBpSmallGiB, SIZE_BP_SMALL_GIB)
    bpIdealMin := firstNonZeroFloat(p.SizeBpIdealMinGiB, SIZE_BP_IDEAL_MIN_GIB)
    bpIdealMax := firstNonZeroFloat(p.SizeBpIdealMaxGiB, SIZE_BP_IDEAL_MAX_GIB)
    bpMedMax := firstNonZeroFloat(p.SizeBpMedMaxGiB, SIZE_BP_MED_MAX_GIB)
    bpLargeMax := firstNonZeroFloat(p.SizeBpLargeMaxGiB, SIZE_BP_LARGE_MAX_GIB)
    scTiny := firstNonZeroFloat(p.SizeScoreTiny, SIZE_SCORE_TINY)
    scIdeal := firstNonZeroFloat(p.SizeScoreIdeal, SIZE_SCORE_IDEAL)
    scMed := firstNonZeroFloat(p.SizeScoreMed, SIZE_SCORE_MED)
    scLarge := firstNonZeroFloat(p.SizeScoreLarge, SIZE_SCORE_LARGE)
    scHuge := firstNonZeroFloat(p.SizeScoreHuge, SIZE_SCORE_HUGE)

    fSize := func() float64 {
        if sizeGiB <= bpSmall {
            return scTiny
        }
        if sizeGiB <= bpIdealMin {
            ratio := (sizeGiB - bpSmall) / (bpIdealMin - bpSmall)
            return scTiny + ratio*(scIdeal-scTiny)
        }
        if sizeGiB <= bpIdealMax {
            return scIdeal
        }
        if sizeGiB <= bpMedMax {
            ratio := (sizeGiB - bpIdealMax) / (bpMedMax - bpIdealMax)
            return scIdeal + ratio*(scMed-scIdeal)
        }
        if sizeGiB <= bpLargeMax {
            return scLarge
        }
        base := scHuge
        if siteTorrent.Leechers >= firstNonZeroInt(p.HugeLBoost3, HUGE_L_BOOST_3) {
            base *= firstNonZeroFloat(p.HugeBoost3, HUGE_BOOST_3)
        } else if siteTorrent.Leechers >= firstNonZeroInt(p.HugeLBoost2, HUGE_L_BOOST_2) {
            base *= firstNonZeroFloat(p.HugeBoost2, HUGE_BOOST_2)
        } else if siteTorrent.Leechers >= firstNonZeroInt(p.HugeLBoost1, HUGE_L_BOOST_1) {
            base *= firstNonZeroFloat(p.HugeBoost1, HUGE_BOOST_1)
        }
        return base
    }()

    // discount factor
    fDiscount := 1.0
    if siteTorrent.DiscountEndTime > 0 {
        left := float64(siteTorrent.DiscountEndTime - siteOption.Now)
        if left <= 0 {
            fDiscount = 0
        } else {
            win := float64(firstNonZeroInt(p.DiscountWindowSeconds, SCORE_DISCOUNT_WINDOW))
            fDiscount = math.Min(1.0, left/win)
        }
    }

    // multiplier: upload multiplier (2x/3x) and non-free penalty
    mul := siteTorrent.UploadMultiplier
    if mul <= 0 {
        mul = 1
    }
    penalty := 1.0
    if siteTorrent.DownloadMultiplier != 0 { // non-free
        penalty = firstNonZeroFloat(p.NonfreePenalty, SCORE_NONFREE_PENALTY)
    }

    // snatched factor (soft addend)
    fSnatch := 1.0 - math.Exp(-float64(siteTorrent.Snatched)/firstNonZeroFloat(p.SnatchK, SCORE_SNATCH_K))

    // 3) 组合
    core := math.Pow(fDemand, firstNonZeroFloat(p.WDemand, SCORE_W_DEMAND)) *
        math.Pow(fAge, firstNonZeroFloat(p.WAge, SCORE_W_AGE)) *
        math.Pow(fSize, firstNonZeroFloat(p.WSize, SCORE_W_SIZE)) *
        math.Pow(fDiscount, firstNonZeroFloat(p.WDiscount, SCORE_W_DISCOUNT))

    score = core * float64(mul) * penalty
    score += firstNonZeroFloat(p.SnatchAdd, SCORE_SNATCH_ADD) * fSnatch

    // 4) 预测上传速度：按站点/时段统计估计 (k_site)
    // demand = L/(S+1)
    denom := siteTorrent.Seeders + 1
    if denom <= 0 { denom = 1 }
    demand := float64(siteTorrent.Leechers) / float64(denom)
    k := stats.EstimateKSite(siteOption.SiteName, siteOption.Now)
    if k <= 0 { k = 100 * 1024 } // fallback
    predictionUploadSpeed = int64(math.Round(float64(k) * demand))
    if predictionUploadSpeed > siteOption.TorrentUploadSpeedLimit {
        predictionUploadSpeed = siteOption.TorrentUploadSpeedLimit
    }
    return
}

func firstNonZeroFloat(v float64, def float64) float64 {
    if v == 0 {
        return def
    }
    return v
}

func firstNonZeroInt(v int64, def int64) int64 {
    if v == 0 {
        return def
    }
    return v
}

func GetBrushSiteOptions(siteInstance site.Site, ts int64) *BrushSiteOptionStruct {
    sc := siteInstance.GetSiteConfig()
    opt := &BrushSiteOptionStruct{
        TorrentMinSizeLimit:     sc.BrushTorrentMinSizeLimitValue,
        TorrentMaxSizeLimit:     sc.BrushTorrentMaxSizeLimitValue,
        TorrentUploadSpeedLimit: sc.TorrentUploadSpeedLimitValue,
        AllowNoneFree:           sc.BrushAllowNoneFree,
        AcceptAnyFree:           sc.BrushAcceptAnyFree,
        AllowPaid:               sc.BrushAllowPaid,
        AllowHr:                 sc.BrushAllowHr,
        AllowZeroSeeders:        sc.BrushAllowZeroSeeders,
        Excludes:                sc.BrushExcludes,
        ExcludeTags:             sc.BrushExcludeTags,
        Now:                     ts,
        SiteName:                siteInstance.GetName(),
        SiteQuotaLimit:          sc.BrushMaxDiskSizeValue,
        ScoreVersion:            sc.BrushScoreVersion,
    }
    if strings.EqualFold(opt.ScoreVersion, "v2") {
        opt.ScoreV2 = &BrushScoreV2Params{
            DemandAlpha:  sc.BrushScoreDemandAlpha,
            DemandMax:    sc.BrushScoreDemandMax,
            AgeT0Seconds: sc.BrushScoreAgeT0Seconds,
            AgeTauSeconds: sc.BrushScoreAgeTauSeconds,
            AgeTauStrictSeconds: sc.BrushScoreAgeTauStrictSeconds,

            SizeBpSmallGiB:    sc.BrushScoreSizeBpSmallGiB,
            SizeBpIdealMinGiB: sc.BrushScoreSizeBpIdealMinGiB,
            SizeBpIdealMaxGiB: sc.BrushScoreSizeBpIdealMaxGiB,
            SizeBpMedMaxGiB:   sc.BrushScoreSizeBpMedMaxGiB,
            SizeBpLargeMaxGiB: sc.BrushScoreSizeBpLargeMaxGiB,
            SizeScoreTiny:     sc.BrushScoreSizeScoreTiny,
            SizeScoreIdeal:    sc.BrushScoreSizeScoreIdeal,
            SizeScoreMed:      sc.BrushScoreSizeScoreMed,
            SizeScoreLarge:    sc.BrushScoreSizeScoreLarge,
            SizeScoreHuge:     sc.BrushScoreSizeScoreHuge,

            HugeLBoost1: sc.BrushScoreHugeLBoost1,
            HugeLBoost2: sc.BrushScoreHugeLBoost2,
            HugeLBoost3: sc.BrushScoreHugeLBoost3,
            HugeBoost1:  sc.BrushScoreHugeBoost1,
            HugeBoost2:  sc.BrushScoreHugeBoost2,
            HugeBoost3:  sc.BrushScoreHugeBoost3,

            DiscountWindowSeconds: sc.BrushScoreDiscountWindowSeconds,
            NonfreePenalty:        sc.BrushScoreNonfreePenalty,
            SnatchK:               sc.BrushScoreSnatchK,
            SnatchAdd:             sc.BrushScoreSnatchAdd,
            WDemand:               sc.BrushScoreWeightsDemand,
            WAge:                  sc.BrushScoreWeightsAge,
            WSize:                 sc.BrushScoreWeightsSize,
            WDiscount:             sc.BrushScoreWeightsDiscount,
        }
    }
    return opt
}

func GetBrushClientOptions(clientInstance client.Client) *BrushClientOptionStruct {
	return &BrushClientOptionStruct{
		MinDiskSpace:            clientInstance.GetClientConfig().BrushMinDiskSpaceValue,
		SlowUploadSpeedTier:     clientInstance.GetClientConfig().BrushSlowUploadSpeedTierValue,
		MaxDownloadingTorrents:  clientInstance.GetClientConfig().BrushMaxDownloadingTorrents,
		MaxTorrents:             clientInstance.GetClientConfig().BrushMaxTorrents,
		MinRatio:                clientInstance.GetClientConfig().BrushMinRatio,
		DefaultUploadSpeedLimit: clientInstance.GetClientConfig().BrushDefaultUploadSpeedLimitValue,
	}
}
