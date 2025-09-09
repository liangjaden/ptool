// 官方保种：自动从站点下载官方保种列表（做种人数≤阈值）的种子并做种。
// 配置：在 ptool.toml 对应站点下设置
//   officialSeedingSize = '5TiB'
//   officialSeedingTorrentMaxSize = '20GiB'
//   officialSeedingTorrentsUrl = 'https://example.com/rescue.php'
//   officialSeedingMember = 5
// 使用：ptool officialseeding <client> <site>
package officialseeding

import (
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"

    log "github.com/sirupsen/logrus"
    "github.com/spf13/cobra"

    "github.com/sagan/ptool/client"
    "github.com/sagan/ptool/client/transmission"
    "github.com/sagan/ptool/cmd"
    "github.com/sagan/ptool/config"
    "github.com/sagan/ptool/constants"
    "github.com/sagan/ptool/site"
    "github.com/sagan/ptool/util"
    "github.com/sagan/ptool/util/torrentutil"
)

const IGNORE_FILE_SIZE = 100

var command = &cobra.Command{
    Use:         "officialseeding {client} {site}",
    Annotations: map[string]string{"cobra-prompt-dynamic-suggestions": "officialseeding"},
    Short:       "Official seeding torrents of sites.",
    Long:        `Official seeding torrents of sites.`,
    Args:        cobra.MatchAll(cobra.ExactArgs(2), cobra.OnlyValidArgs),
    RunE:        officialseeding,
}

var (
    dryRun = false
)

func init() {
    command.Flags().BoolVarP(&dryRun, "dry-run", "d", false,
        "Dry run. Do NOT actually add or delete torrent to / from client")
    cmd.RootCmd.AddCommand(command)
}

func officialseeding(cmd *cobra.Command, args []string) (err error) {
    clientName := args[0]
    sitename := args[1]
    clientInstance, err := client.CreateClient(clientName)
    if err != nil {
        return fmt.Errorf("failed to create client: %w", err)
    }
    lock, err := config.LockConfigDirFile(fmt.Sprintf(config.CLIENT_LOCK_FILE, clientName))
    if err != nil {
        return err
    }
    defer lock.Unlock()
    siteInstance, err := site.CreateSite(sitename)
    if err != nil {
        return fmt.Errorf("failed to create site: %w", err)
    }
    ignoreFile, err := os.OpenFile(filepath.Join(config.ConfigDir,
        fmt.Sprintf("official-seeding-%s.ignore.txt", sitename)),
        os.O_CREATE|os.O_RDWR, constants.PERM)
    if err != nil {
        return fmt.Errorf("failed to open ignore file: %w", err)
    }
    defer ignoreFile.Close()
    var ignores []string
    if contents, err := io.ReadAll(ignoreFile); err != nil {
        return fmt.Errorf("failed to read ignore file: %w", err)
    } else {
        ignores = strings.Split(string(contents), "\n")
    }
    // Transmission 性能优化：批量同步
    if trClient, ok := clientInstance.(*transmission.Client); ok {
        trClient.Sync(true)
    }
    result, err := doOfficialSeeding(clientInstance, siteInstance, ignores)
    if err != nil {
        return err
    }
    result.Print(os.Stdout)
    if dryRun {
        log.Warnf("Dry-run. Exit")
        return nil
    }
    errorCnt := int64(0)
    // add
    addedSize := int64(0)
    tags := result.AddTorrentsOption.Tags
    for len(result.AddTorrents) > 0 {
        torrent := result.AddTorrents[0].Id
        if torrent == "" {
            torrent = result.AddTorrents[0].DownloadUrl
        }
        if contents, _, _, err := siteInstance.DownloadTorrent(torrent); err != nil {
            log.Errorf("Failed to download site torrent %s", torrent)
            errorCnt++
        } else if tinfo, err := torrentutil.ParseTorrent(contents); err != nil {
            log.Errorf("Failed to download site torrent %s: is not a valid torrent: %v", torrent, err)
            errorCnt++
        } else {
            var _tags []string
            _tags = append(_tags, tags...)
            if tinfo.IsPrivate() {
                _tags = append(_tags, config.PRIVATE_TAG)
            } else {
                _tags = append(_tags, config.PUBLIC_TAG)
            }
            // 记录原始做种人数，便于后续淘汰时优先删除“原始做种人数多”的种子
            if result.AddTorrents[0].SeedersAtAdd > 0 {
                _tags = append(_tags, client.GenerateTorrentTagFromMetadata("oseeders", result.AddTorrents[0].SeedersAtAdd))
            }
            result.AddTorrentsOption.Name = result.AddTorrents[0].Name
            result.AddTorrentsOption.Tags = _tags
            meta := map[string]int64{}
            if result.AddTorrents[0].Id != "" {
                if id := util.ParseInt(result.AddTorrents[0].ID()); id != 0 {
                    meta["id"] = id
                }
            }
            if err := clientInstance.AddTorrent(contents, result.AddTorrentsOption, meta); err != nil {
                log.Errorf("Failed to add site torrent %s to client: %v", torrent, err)
                errorCnt++
            } else {
                addedSize += result.AddTorrents[0].Size
            }
        }
        result.AddTorrents = result.AddTorrents[1:]
    }
    // delete
    deleteSize := int64(0)
    var deleteInfoHashes []string
    var deleteIds []string
    log.Infof("Delete torrents:")
    for len(result.DeleteTorrents) > 0 {
        if deleteSize >= addedSize+result.OverflowSpace {
            break
        }
        deleteSize += result.DeleteTorrents[0].Size
        if result.DeleteTorrents[0].Meta["id"] > 0 {
            deleteIds = append(deleteIds, fmt.Sprint(result.DeleteTorrents[0].Meta["id"]))
        }
        deleteInfoHashes = append(deleteInfoHashes, result.DeleteTorrents[0].InfoHash)
        log.Infof("Torrent %s (%s)", result.DeleteTorrents[0].Name, result.DeleteTorrents[0].InfoHash)
        result.DeleteTorrents = result.DeleteTorrents[1:]
    }
    if len(deleteInfoHashes) > 0 {
        err := client.DeleteTorrentsAuto(clientInstance, deleteInfoHashes)
        log.Infof("Delete torrents result: %v", err)
        if err != nil {
            errorCnt++
        } else if len(deleteIds) > 0 {
            ignores = append(ignores, deleteIds...)
            if len(ignores) > IGNORE_FILE_SIZE {
                ignores = ignores[len(ignores)-IGNORE_FILE_SIZE:]
            }
            ignoreFile.Truncate(0)
            ignoreFile.Seek(0, 0)
            ignoreFile.WriteString(strings.Join(ignores, "\n"))
        }
    }
    if errorCnt > 0 {
        return fmt.Errorf("%d errors", errorCnt)
    }
    return nil
}

