package officialseeding

import (
    "fmt"
    "io"
    "math/rand"
    "os"
    "slices"
    "sort"

    log "github.com/sirupsen/logrus"

    "github.com/sagan/ptool/client"
    "github.com/sagan/ptool/cmd/common"
    "github.com/sagan/ptool/config"
    "github.com/sagan/ptool/constants"
    "github.com/sagan/ptool/site"
    "github.com/sagan/ptool/util"
)

type TrackersStatus int

const (
    TRACKER_UNKNOWN TrackersStatus = iota
    TRACKER_OK
    TRACKER_INVALID
)

// 删除保护阈值：当前做种人数 < 4 的官方保种种子绝不删除
const OFFICIAL_MIN_SEEDERS = 4
// 默认站点“需保种”的做种人数上限（≤该值才算官方保种候选）
const OFFICIAL_MEMBER_DEFAULT = 5

// 客户端下载相关参数
const MIN_SIZE = 10 * 1024 * 1024 * 1024 // 最小 officialSeedingSize 要求。10GiB
const NEW_TORRENT_TIMESPAN = 3600
const MAX_PARALLEL_DOWNLOAD = 3
const INACTIVITY_TIMESPAN = 3600 * 3 // 未完成种长时间无活动则视为失败

type Result struct {
    OverflowSpace     int64 // 当前类别种子总体超出上限的体积
    Timestamp         int64
    Sitename          string
    Size              int64
    DeleteTorrents    []*client.Torrent
    AddTorrents       []*site.Torrent
    AddTorrentsOption *client.TorrentOption
    Msg               string
    Log               string
}

func (result *Result) Print(output io.Writer) {
    fmt.Fprintf(output, "official-seeding of %q site at %s\n", result.Sitename, util.FormatTime(result.Timestamp))
    fmt.Fprintf(output, "Use at most %s of disk to official-seeding\n", util.BytesSize(float64(result.Size)))
    fmt.Fprintf(output, "Message: %s\n", result.Msg)
    if len(result.DeleteTorrents) > 0 {
        fmt.Fprintf(output, "\nDelete %d torents from client:\n", len(result.DeleteTorrents))
        client.PrintTorrents(os.Stdout, result.DeleteTorrents, "", 1, false)
    }
    if len(result.AddTorrents) > 0 {
        fmt.Fprintf(output, "\nAdd %d torents to client:\n", len(result.AddTorrents))
        site.PrintTorrents(os.Stdout, result.AddTorrents, "", result.Timestamp, false, false, nil)
    }
    fmt.Fprintf(output, "\nLog:\n%s\n", result.Log)
}

func doOfficialSeeding(clientInstance client.Client, siteInstance site.Site, ignores []string) (
    result *Result, err error) {
    timestamp := util.Now()
    if siteInstance.GetSiteConfig().OfficialSeedingSizeValue <= MIN_SIZE {
        return nil, fmt.Errorf("officialSeedingSize insufficient. Current value: %s. At least %s is required",
            util.BytesSizeAround(float64(siteInstance.GetSiteConfig().OfficialSeedingSizeValue)),
            util.BytesSizeAround(float64(MIN_SIZE)))
    }
    if siteInstance.GetSiteConfig().GlobalHnR {
        return nil, fmt.Errorf("site that enforces global H&R policy is not supported at this time")
    }
    officialSeedingCat := config.OFFICIAL_SEEDING_CAT_PREFIX + siteInstance.GetName()
    officialSeedingTag := client.GenerateTorrentTagFromSite(siteInstance.GetName())
    clientStatus, err := clientInstance.GetStatus()
    if err != nil {
        return nil, fmt.Errorf("failed to get client status: %w", err)
    }
    result = &Result{
        Timestamp: timestamp,
        Sitename:  siteInstance.GetName(),
        Size:      siteInstance.GetSiteConfig().OfficialSeedingSizeValue,
        AddTorrentsOption: &client.TorrentOption{
            Category: officialSeedingCat,
            Tags:     []string{officialSeedingTag},
        },
    }
    if clientStatus.NoAdd {
        result.Msg = fmt.Sprintf("Client has %q tag. Exit", config.NOADD_TAG)
        return
    }
    downloadingSpeedLimit := clientStatus.DownloadSpeedLimit
    if downloadingSpeedLimit <= 0 {
        downloadingSpeedLimit = constants.CLIENT_DEFAULT_DOWNLOADING_SPEED_LIMIT
    }
    if float64(clientStatus.DownloadSpeed/downloadingSpeedLimit) >= 0.8 {
        result.Msg = fmt.Sprintf("Client incoming bandwidth is full (spd/lmt): %s / %s. Exit",
            util.BytesSize(float64(clientStatus.DownloadSpeed)), util.BytesSize(float64(downloadingSpeedLimit)))
        return
    }
    clientTorrents, err := clientInstance.GetTorrents("", officialSeedingCat, true)
    if err != nil {
        return nil, fmt.Errorf("failed to get client current official seeding torrents: %w", err)
    }
    // 按“原始做种人数多 -> 优先删除”排序（若无原始人数，按当前做种人数）；其次按体积从小到大
    sort.Slice(clientTorrents, func(i, j int) bool {
        oi := clientTorrents[i].GetMetadataFromTags()["oseeders"]
        oj := clientTorrents[j].GetMetadataFromTags()["oseeders"]
        if oi == 0 {
            oi = clientTorrents[i].Seeders
        }
        if oj == 0 {
            oj = clientTorrents[j].Seeders
        }
        if oi != oj {
            return oi > oj
        }
        return clientTorrents[i].Size < clientTorrents[j].Size
    })
    fmt.Fprintf(os.Stderr, "client category %q torrents:\n", officialSeedingCat)
    client.PrintTorrents(os.Stderr, clientTorrents, "", 1, false)

    // 分类当前客户端种子
    clientTorrentsMap := map[string]*client.Torrent{}
    var invalidTorrents []string
    var stalledTorrents []string
    var downloadingTorrents []string
    var deletableTorrents []string // 可删集合（不含 nodel；当前做种人数>=OFFICIAL_MIN_SEEDERS）
    var protectedTorrents []string // 保护集合（nodel 或 当前做种人数<4）
    var otherTorrents []string

    for _, torrent := range clientTorrents {
        clientTorrentsMap[torrent.InfoHash] = torrent
    }

    for _, torrent := range clientTorrents {
        if !torrent.HasTag(officialSeedingTag) {
            otherTorrents = append(otherTorrents, torrent.InfoHash)
            continue
        }
        var trackerStatus TrackersStatus
        if torrent.Seeders == 0 && torrent.Leechers == 0 {
            if trackers, err := clientInstance.GetTorrentTrackers(torrent.InfoHash); err != nil {
                trackerStatus = TRACKER_UNKNOWN
            } else if valitidy := trackers.SpeculateTrackerValidity(); valitidy > 0 {
                trackerStatus = TRACKER_INVALID
            } else {
                trackerStatus = TRACKER_OK
            }
        }
        if torrent.HasTag(config.TORRENT_NODEL_TAG) {
            protectedTorrents = append(protectedTorrents, torrent.InfoHash)
        } else if !torrent.IsComplete() {
            if trackerStatus == TRACKER_INVALID && timestamp-torrent.Atime > NEW_TORRENT_TIMESPAN {
                invalidTorrents = append(invalidTorrents, torrent.InfoHash)
            } else if (timestamp - torrent.ActivityTime) >= INACTIVITY_TIMESPAN {
                stalledTorrents = append(stalledTorrents, torrent.InfoHash)
            } else {
                downloadingTorrents = append(downloadingTorrents, torrent.InfoHash)
            }
        } else if torrent.State != "seeding" {
            otherTorrents = append(otherTorrents, torrent.InfoHash)
        } else if trackerStatus == TRACKER_INVALID {
            invalidTorrents = append(invalidTorrents, torrent.InfoHash)
        } else if torrent.Seeders < OFFICIAL_MIN_SEEDERS {
            protectedTorrents = append(protectedTorrents, torrent.InfoHash)
        } else {
            deletableTorrents = append(deletableTorrents, torrent.InfoHash)
        }
    }

    availableSlots := MAX_PARALLEL_DOWNLOAD - len(downloadingTorrents)
    statistics := common.NewTorrentsStatistics()
    for _, ih := range protectedTorrents {
        statistics.UpdateClientTorrent(common.TORRENT_SUCCESS, clientTorrentsMap[ih])
    }
    for _, ih := range deletableTorrents {
        statistics.UpdateClientTorrent(common.TORRENT_FAILURE, clientTorrentsMap[ih])
    }
    for _, ih := range stalledTorrents {
        statistics.UpdateClientTorrent(common.TORRENT_FAILURE, clientTorrentsMap[ih])
    }
    for _, ih := range invalidTorrents {
        statistics.UpdateClientTorrent(common.TORRENT_INVALID, clientTorrentsMap[ih])
    }

    availableSpace := siteInstance.GetSiteConfig().OfficialSeedingSizeValue - statistics.SuccessSize
    if !clientStatus.NoDel {
        availableSpace += statistics.FailureSize
    }
    if statistics.SuccessSize+statistics.FailureSize > siteInstance.GetSiteConfig().OfficialSeedingSizeValue {
        result.OverflowSpace = statistics.SuccessSize + statistics.FailureSize -
            siteInstance.GetSiteConfig().OfficialSeedingSizeValue
    }
    result.Log += fmt.Sprintf("Client torrents: others %d / invalid %d / stalled %d / downloading %d / deletable %d / protected %d\n",
        len(otherTorrents), len(invalidTorrents), len(stalledTorrents), len(downloadingTorrents), len(deletableTorrents), len(protectedTorrents))
    result.Log += fmt.Sprintf("CapSpace/ProtectedSize/DeletableSize/AvailableSpace: %s / %s / %s /%s",
        util.BytesSizeAround(float64(siteInstance.GetSiteConfig().OfficialSeedingSizeValue)),
        util.BytesSizeAround(float64(statistics.SuccessSize)),
        util.BytesSizeAround(float64(statistics.FailureSize)),
        util.BytesSizeAround(float64(availableSpace)))

    if len(downloadingTorrents) >= MAX_PARALLEL_DOWNLOAD {
        result.Msg = "Already currently downloading enough torrents. Exit"
        return
    }
    if availableSpace < min(siteInstance.GetSiteConfig().OfficialSeedingSizeValue/10, MIN_SIZE) {
        result.Msg = "Insufficient official seeding storage space in client. Exit"
        return
    }

    // 获取站点官方保种页面的种子列表，优先做种人数少的
    officialUrl := siteInstance.GetSiteConfig().OfficialSeedingTorrentsUrl
    // 默认使用站点的 rescue 列表。如果未配置，则尝试常见路径。
    if officialUrl == "" {
        officialUrl = "rescue.php"
    }
    // 按做种数排序扫描（与 dynamicseeding 一致）；NexusPHP 支持 seeders_begin=1
    if siteInstance.GetSiteConfig().Type == "nexusphp" {
        officialUrl = util.AppendUrlQueryString(officialUrl, "seeders_begin=1")
    }
    // 按做种数排序扫描（与 dynamicseeding 一致）
    var siteTorrents []*site.Torrent
    var siteTorrentsSize int64
    var scannedTorrents int64
    marker := ""
    maxScan := int64(1000)
    member := siteInstance.GetSiteConfig().OfficialSeedingMember
    if member <= 0 {
        member = OFFICIAL_MEMBER_DEFAULT
    }
site_outer:
    for {
        torrents, nextPageMarker, err := siteInstance.GetAllTorrents("seeders", false, marker, officialUrl)
        if err != nil {
            result.Log += fmt.Sprintf("failed to get site torrents: %v\n", err)
            break
        }
        rand.Shuffle(len(torrents), func(i, j int) { torrents[i], torrents[j] = torrents[j], torrents[i] })
        slices.SortStableFunc(torrents, func(a, b *site.Torrent) int { return int(a.Seeders - b.Seeders) })
        for _, torrent := range torrents {
            if torrent.Id != "" && slices.Contains(ignores, torrent.ID()) {
                log.Debugf("Ignore site torrent %s (%s) which is recently deleted from client", torrent.Name, torrent.Id)
                continue
            }
            if torrent.Seeders < 1 || torrent.IsCurrentActive {
                continue
            }
            if torrent.Seeders > member {
                // 仅跳过该条，继续扫描其余候选（部分站点排序未必严格按做种数升序）
                continue
            }
            scannedTorrents++
            if maxScan > 0 && scannedTorrents > maxScan {
                break site_outer
            }
            onlyFree := siteInstance.GetSiteConfig().OfficialSeedingOnlyFree
            if torrent.HasHnR || (torrent.Paid && !torrent.Bought) || (onlyFree && torrent.DownloadMultiplier != 0) {
                continue
            }
            if siteInstance.GetSiteConfig().OfficialSeedingTorrentMaxSizeValue > 0 &&
                torrent.Size > siteInstance.GetSiteConfig().OfficialSeedingTorrentMaxSizeValue ||
                siteInstance.GetSiteConfig().OfficialSeedingTorrentMinSizeValue > 0 &&
                    torrent.Size < siteInstance.GetSiteConfig().OfficialSeedingTorrentMinSizeValue {
                continue
            }
            if torrent.Size > availableSpace-siteTorrentsSize {
                continue
            }
            siteTorrents = append(siteTorrents, torrent)
            siteTorrentsSize += torrent.Size
        }
        if len(siteTorrents) >= availableSlots {
            break
        }
        if nextPageMarker == "" {
            break
        }
        marker = nextPageMarker
    }
    if len(siteTorrents) == 0 {
        result.Msg = "No candidate site official seeding torrents found"
        return
    }
    fmt.Fprintf(os.Stderr, "site candidate torrents:\n")
    site.PrintTorrents(os.Stderr, siteTorrents, "", timestamp, false, false, nil)

    availableSpace = siteInstance.GetSiteConfig().OfficialSeedingSizeValue -
        statistics.SuccessSize - statistics.FailureSize
    for _, torrent := range siteTorrents {
        var deleteTorrents []string
        var logstr string
        if !clientStatus.NoDel {
            for availableSpace < torrent.Size {
                if len(invalidTorrents) > 0 {
                    availableSpace += clientTorrentsMap[invalidTorrents[0]].Size
                    logstr += fmt.Sprintf("Delete client invalid torrent %s\n", clientTorrentsMap[invalidTorrents[0]].Name)
                    deleteTorrents = append(deleteTorrents, invalidTorrents[0])
                    invalidTorrents = invalidTorrents[1:]
                } else if len(stalledTorrents) > 0 {
                    availableSpace += clientTorrentsMap[stalledTorrents[0]].Size
                    logstr += fmt.Sprintf("Delete client stalled torrent %s\n", clientTorrentsMap[stalledTorrents[0]].Name)
                    deleteTorrents = append(deleteTorrents, stalledTorrents[0])
                    stalledTorrents = stalledTorrents[1:]
                } else if len(deletableTorrents) > 0 {
                    // 在可删集合中，优先删除“原始做种人数”高的
                    ih := deletableTorrents[0]
                    deletableTorrents = deletableTorrents[1:]
                    availableSpace += clientTorrentsMap[ih].Size
                    logstr += fmt.Sprintf("Delete client official torrent %s (prefer high original seeders)\n", clientTorrentsMap[ih].Name)
                    deleteTorrents = append(deleteTorrents, ih)
                } else {
                    break
                }
            }
        }
        if availableSpace < torrent.Size {
            break
        }
        availableSpace -= torrent.Size
        // 记录添加时的做种人数
        torrent.SeedersAtAdd = torrent.Seeders
        result.AddTorrents = append(result.AddTorrents, torrent)
        result.Log += logstr
        result.Log += fmt.Sprintf("Add site torrent %s\n", torrent.Name)
        for _, ih := range deleteTorrents {
            result.DeleteTorrents = append(result.DeleteTorrents, clientTorrentsMap[ih])
        }
    }

    return
}

func min(a, b int64) int64 {
    if a < b {
        return a
    }
    return b
}
