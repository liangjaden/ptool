package officialseeding

import (
    prompt "github.com/c-bata/go-prompt"
    "github.com/sagan/ptool/cmd"
    "github.com/sagan/ptool/config"
)

func init() {
    cmd.AddShellCompletion("officialseeding", func(document *prompt.Document) []prompt.Suggest {
        text := document.GetWordBeforeCursorWithSpace()
        words := cmd.SplitAndTrim(text)
        if len(words) >= 3 { // cmd + client + site
            return []prompt.Suggest{}
        }
        suggests := []prompt.Suggest{}
        if len(words) < 2 {
            for _, client := range config.Get().ClientsEnabled {
                suggests = append(suggests, prompt.Suggest{Text: client.Name, Description: client.Url})
            }
        } else {
            for _, site := range config.Get().SitesEnabled {
                suggests = append(suggests, prompt.Suggest{Text: site.GetName(), Description: site.Url})
            }
        }
        return cmd.FilterByPrefixOrContains(suggests, text)
    })
}

