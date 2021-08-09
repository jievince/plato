package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
    "log"
    "time"
    "os/user"

	"fmt"
)

type Configs struct {
    Hdfs    string `json:"hdfs"`
    Wnum    int `json:"WNUM"`
    Wcores  int `json:"WCORES"`
    Algo    string `json:"algoName"`
    Louvain  map[string]interface{} `json:"louvain"`
    Kcore    map[string]interface{} `json:"kcore"`
    Lpa      map[string]interface{} `json:"lpa"`
    Hanp     map[string]interface{} `json:"hanp"`
    Pagerank map[string]interface{} `json:"pagerank"`
    Degree   map[string]interface{} `json:"degree"`
    Cc       map[string]interface{} `json:"cc"`
    CustomedAlgo string `json:"customedAlgo"`
}
var (
	pid      = os.Getpid()
	program  = filepath.Base(os.Args[0])
	host     = "unknownhost"
	userName = "unknownuser"
)

func init() {
	h, err := os.Hostname()
	if err == nil {
		host = shortHostname(h)
	}

	current, err := user.Current()
	if err == nil {
		userName = current.Username
	}

	// Sanitize userName since it may contain filepath separators on Windows.
	userName = strings.Replace(userName, `\`, "_", -1)
}

// shortHostname returns its argument, truncating at the first period.
// For instance, given "www.google.com" it returns "www".
func shortHostname(hostname string) string {
	if i := strings.Index(hostname, "."); i >= 0 {
		return hostname[:i]
	}
	return hostname
}

// logName returns a new log file name containing tag, with start time t, and
// the name for the symlink for tag.
func logName(t time.Time) (name string) {
	name = fmt.Sprintf("%s.%s.%s.log.%04d%02d%02d-%02d%02d%02d.%d",
		program,
		host,
		userName,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		pid)
	return name
}

func main() {
    if len(os.Args) < 2 {
        log.Println("plato [jsonStr] [platoHome] [logDirOfRunPlato]")
        os.Exit(120)
    }
    platoHome := ""
    logDirOfRunPlato := ""
    jsonStr := os.Args[1]
    // jsonStr := "{\"hdfs\": \"hdfs://192.168.8.149:9000/\",\"wnum\": 4,\"wcores\": 4,\"algo\": \"pagerank\",\"args\": {\"maxIter\": 10,\"damping\": 0.85,\"is_directed\":false}}"
    log.Println("args num", len(os.Args))
    if len(os.Args) > 2 {
        platoHome = os.Args[2]
    }
    if len(os.Args) > 3 {
        logDirOfRunPlato = os.Args[3]
    }
    if platoHome == "" {
		ex, err := os.Executable()
		if err != nil {
			log.Panicf("Get run_plato executable failed: %s", err.Error())
		}
		platoHome = filepath.Dir(ex) // Set to executable folder
    }
    if logDirOfRunPlato == "" {
        logDirOfRunPlato = "/home/vesoft-cm/graph/logs/run_plato"
    }

    if _, err := os.Stat(logDirOfRunPlato); os.IsNotExist(err) {
        //Create a folder/directory at a full qualified path
        err := os.MkdirAll(logDirOfRunPlato, 0777)
        if err != nil {
            log.Panicf("create log_dir failed: %s", err.Error())
        }
    }

    f, err := os.OpenFile(logDirOfRunPlato+"/"+logName(time.Now()), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
    if err != nil {
        log.Panicf("error opening file: %v", err)
    }

    //defer f.Close()

    log.Println("jsonStr: ", jsonStr)
    log.Println("platoHome: ", platoHome)
    log.Println("logDirOfRunPlato: ", logDirOfRunPlato)

    log.SetOutput(f)
    log.Println("This is a test log entry")

    var configs Configs
    json.Unmarshal([]byte(jsonStr), &configs)
    log.Println("[configs]value: %v\ntype: %T\n", configs, configs)

    command := platoHome  + `/scripts/run_algo.sh`
    switch strings.ToLower(configs.Algo) {
        case "louvain":
            is_directed, ok := configs.Louvain["isDirected"]
            if !ok {
                log.Println("Warning: Argument isDirected is missed")
                f.Close()
                os.Exit(122)
            }
            maxIter, ok := configs.Louvain["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                f.Close()
                os.Exit(122)
            }
            internalIter, ok := configs.Louvain["internalIter"]
            if !ok {
                log.Println("Warning: Argument internalIter is missed")
                f.Close()
                os.Exit(122)
            }
            command += fmt.Sprintf(" -a fast_unfolding_simple -o %v -i %v -d %v", maxIter, internalIter, is_directed)
            //tol := configs.Louvain["tol"]
        case "kcore":
            is_directed, ok := configs.Kcore["isDirected"]
            if !ok {
                log.Println("Warning: Argument isDirected is missed")
                f.Close()
                os.Exit(122)
            }
            k, ok := configs.Kcore["k"]
            if !ok {
                log.Println("Warning: Argument k is missed")
                f.Close()
                os.Exit(122)
            }
            //maxIter := configs.Kcore["maxIter"]
            command += fmt.Sprintf(" -a kcore_simple -k %v -d %v", k, is_directed)
        case "lpa":
            is_directed, ok := configs.Lpa["isDirected"]
            if !ok {
                log.Println("Warning: Argument isDirected is missed")
                f.Close()
                os.Exit(122)
            }
            maxIter, ok := configs.Lpa["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                f.Close()
                os.Exit(122)
            }
            command += fmt.Sprintf(" -a lpa -r %v -d %v", maxIter, is_directed)
        case "hanp":
            is_directed, ok := configs.Hanp["isDirected"]
            if !ok {
                log.Println("Warning: Argument isDirected is missed")
                f.Close()
                os.Exit(122)
            }
            maxIter, ok := configs.Hanp["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                f.Close()
                os.Exit(122)
            }
            preference, ok := configs.Hanp["preference"]
            if !ok {
                log.Println("Warning: Argument preference is missed")
                f.Close()
                os.Exit(122)
            }
            hop_att, ok := configs.Hanp["hop_att"]
            if !ok {
                log.Println("Warning: Argument hop_att is missed")
                f.Close()
                os.Exit(122)
            }
            command += fmt.Sprintf(" -a hanp -r %v -p %v -t %v -d %v", maxIter, preference, hop_att, is_directed)
        case "pagerank":
            is_directed, ok := configs.Pagerank["isDirected"]
            if !ok {
                log.Println("Warning: Argument isDirected is missed")
                f.Close()
                os.Exit(122)
            }
            maxIter, ok := configs.Pagerank["maxIter"]
            if !ok {
                log.Println("Warning: Argument maxIter is missed")
                f.Close()
                os.Exit(122)
            }
            damping, ok := configs.Pagerank["damping"]
            if !ok {
                log.Println("Warning: Argument damping is missed")
                f.Close()
                os.Exit(122)
            }
            command += fmt.Sprintf(" -a pagerank -r %v -m %v -d %v", maxIter, damping, is_directed)
        case "degree":
            // is_directed, ok := configs.Degree["isDirected"]
            // if !ok {
            //     log.Println("Warning: Argument isDirected is missed")
            //     f.Close()
            //     os.Exit(122)
            // }
            command += fmt.Sprintf(" -a nstepdegrees -d %v", true)
        case "cc":
            is_directed, ok := configs.Cc["isDirected"]
            if !ok {
                log.Println("Warning: Argument isDirected is missed")
                f.Close()
                os.Exit(122)
            }
            // maxIter, ok := configs.Cc["maxIter"]
            // if !ok {
            //     log.Println("Warning: Argument maxIter is missed")
            //     f.Close()
            //     os.Exit(122)
            // }
            command += fmt.Sprintf(" -a cgm_simple -d %v", is_directed)
        default:
            log.Println("Customized algorighm: ", configs.Algo)
            parameterStr := configs.CustomedAlgo
            command += fmt.Sprintf(" -a %v -x \"%v\"", configs.Algo, parameterStr)
    }
    // keys := make([]string, 0, len(configs.Args))
    // for k := range configs.Args {
    //     keys = append(keys, k)
    // }
    if len(configs.Hdfs) == 0 {
        log.Println("hdfs is needed...")
        os.Exit(124)
    }
    command += " -f " + configs.Hdfs
    if configs.Wnum != 0 {
        command += fmt.Sprintf(" -n %v", configs.Wnum)
    }
    if configs.Wcores != 0 {
        command += fmt.Sprintf(" -c %v", configs.Wcores)
    }
    log.Println("command: ", command)
    cmd := exec.Command("/bin/bash", "-c", command)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Panicf("cmd.StdoutPipe: %s", err.Error())
	}

    cmd.Stderr = cmd.Stdout
	
	cmd.Start()

	reader := bufio.NewReader(stdout)

	//实时循环读取输出流中的一行内容
	for {
		line, err2 := reader.ReadString('\n')
		if err2 != nil || io.EOF == err2 {
			break
		}
	    log.Println(line)
	}

	cmd.Wait()
    f.Close()
}

