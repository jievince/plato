package main

import (
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"strings"
    "fmt"
)

type Configs struct {
    Hdfs    string `json:"hdfs"`
    Wnum    int `json:"wnum"`
    Wcores  int `json:"wcores"`
    Algo    string `json:"algo"`
    Args    map[string]interface{} `json:"args"`
}

func main() {
    if len(os.Args) < 2 {
        log.Print("plato [jsonStr] [platoHome]")
        os.Exit(1)
    }
    platoHome := "/home/vesoft-cm/jie/plato/"
    jsonStr := os.Args[1]
    // jsonStr := "{\"hdfs\": \"hdfs://192.168.8.149:9000/\",\"wnum\": 4,\"wcores\": 4,\"algo\": \"pagerank\",\"args\": {\"maxIter\": 10,\"damping\": 0.85,\"is_directed\":false}}"
    if len(os.Args) > 2 {
        platoHome = os.Args[2]
    }
    log.Println("jsonStr: ", jsonStr)
    log.Println("platoHome: ", platoHome)

    var configs Configs
    json.Unmarshal([]byte(jsonStr), &configs)
    log.Printf("[configs]value: %v\ntype: %T\n", configs, configs)

    command := platoHome  + `./scripts/run_algo.sh`
    switch strings.ToLower(configs.Algo) {
        case "louvain":
            is_directed, ok := configs.Args["is_directed"]
            if !ok {
                log.Println("Warning: Argument is_directed is missed")
                os.Exit(51)
            }
            maxIter, ok := configs.Args["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                os.Exit(51)
            }
            internalIter, ok := configs.Args["internalIter"]
            if !ok {
                log.Println("Warning: Argument internalIter is missed")
                os.Exit(51)
            }
            command += fmt.Sprintf(" -a fast_unfolding_simple -o %v -i %v -d %v", maxIter, internalIter, is_directed)
            //tol := configs.Args["tol"]
        case "kcore":
            is_directed, ok := configs.Args["is_directed"]
            if !ok {
                log.Println("Warning: Argument is_directed is missed")
                os.Exit(51)
            }
            k, ok := configs.Args["k"]
            if !ok {
                log.Println("Warning: Argument k is missed")
                os.Exit(51)
            }
            //maxIter := configs.Args["maxIter"]
            command += fmt.Sprintf(" -a kcore_simple -k %v -d %v", k, is_directed)
        case "lpa":
            is_directed, ok := configs.Args["is_directed"]
            if !ok {
                log.Println("Warning: Argument is_directed is missed")
                os.Exit(51)
            }
            maxIter, ok := configs.Args["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                os.Exit(51)
            }
            command += fmt.Sprintf(" -a lpa -r %v -d %v", maxIter, is_directed)
        case "hanp":
            is_directed, ok := configs.Args["is_directed"]
            if !ok {
                log.Println("Warning: Argument is_directed is missed")
                os.Exit(51)
            }
            maxIter, ok := configs.Args["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                os.Exit(51)
            }
            preference, ok := configs.Args["preference"]
            if !ok {
                log.Println("Warning: Argument preference is missed")
                os.Exit(51)
            }
            hop_att, ok := configs.Args["hop_att"]
            if !ok {
                log.Println("Warning: Argument hop_att is missed")
                os.Exit(51)
            }
            command += fmt.Sprintf(" -a hanp -r %v -p %v -t %v -d %v", maxIter, preference, hop_att, is_directed)
        case "pagerank":
            is_directed, ok := configs.Args["is_directed"]
            if !ok {
                log.Println("Warning: Argument is_directed is missed")
                os.Exit(51)
            }
            maxIter, ok := configs.Args["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                os.Exit(51)
            }
            damping, ok := configs.Args["damping"]
            if !ok {
                log.Println("Warning: Argument damping is missed")
                os.Exit(51)
            }
            command += fmt.Sprintf(" -a pagerank -r %v -m %v -d %v", maxIter, damping, is_directed)
        default:
            log.Println("No such algorighm: ", configs.Algo)
            os.Exit(52)
    }
    keys := make([]string, 0, len(configs.Args))
    for k := range configs.Args {
        keys = append(keys, k)
    }
    if len(configs.Hdfs) == 0 {
        log.Println("hdfs is needed...")
        os.Exit(53)
    }
    command += " -f " + configs.Hdfs
    if configs.Wnum == 0 {
        command += fmt.Sprintf(" -n %v", configs.Wnum)
    }
    if configs.Wcores == 0 {
        command += fmt.Sprintf(" -c %v", configs.Wcores)
    }
    log.Println("command: ", command)
    cmd := exec.Command("/bin/bash", "-c", command)
    out, err := cmd.CombinedOutput()
	if err != nil {
        if exitError, ok := err.(*exec.ExitError); ok {
            log.Printf("Output of run_algo.sh:\n%s\n", string(out))
            os.Exit(exitError.ExitCode())
        }
	}
    log.Printf("Output of run_algo.sh:\n%s\n", string(out))
}
