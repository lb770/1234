package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zip"
	tls "github.com/refraction-networking/utls"
	"golang.org/x/net/proxy"
)

var (
	outputFile  string
	threadNum   int
	defaultPort string
	timeoutSec  int
	ipPortRegex = regexp.MustCompile(`^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?:[:,\s]+(\d{1,5}))?`)
)

func init() {
	flag.StringVar(&outputFile, "output", "list.txt", "输出文件路径")
	flag.IntVar(&threadNum, "thread", 16, "最大并发线程数")
	flag.StringVar(&defaultPort, "port", "443", "默认端口(当输入未指定端口时使用)")
	flag.IntVar(&timeoutSec, "timeout", 5, "超时时间(秒)")
	flag.Parse()
}

func parseTarget(line string) string {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return ""
	}
	matches := ipPortRegex.FindStringSubmatch(line)
	if len(matches) < 2 || matches[1] == "" {
		return ""
	}
	ip := matches[1]
	port := defaultPort
	if len(matches) > 2 && matches[2] != "" {
		port = matches[2]
	}
	return net.JoinHostPort(ip, port)
}

func readTargets(input string) ([]string, error) {
	var reader io.Reader

	if strings.HasPrefix(input, "http://") || strings.HasPrefix(input, "https://") {
		log.Printf("[+] 正在从网络获取 IP 列表: %s\n", input)
		resp, err := http.Get(input)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		reader = resp.Body
		return parseLines(reader), nil
	}

	ext := strings.ToLower(filepath.Ext(input))
	if ext == ".zip" {
		zipReader, err := zip.OpenReader(input)
		if err != nil {
			return nil, err
		}
		defer zipReader.Close()
		var targets []string
		for _, f := range zipReader.File {
			rc, err := f.Open()
			if err != nil {
				continue
			}
			targets = append(targets, parseLines(rc)...)
			rc.Close()
		}
		return targets, nil
	}

	file, err := os.Open(input)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseLines(file), nil
}

func parseLines(r io.Reader) []string {
	var targets []string
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		target := parseTarget(scanner.Text())
		if target != "" && !seen[target] {
			seen[target] = true
			targets = append(targets, target)
		}
	}
	return targets
}

func testWorker(dialer proxy.Dialer, target string) (string, time.Duration, bool) {
	start := time.Now()
	tr := &http.Transport{
		DialTLSContext: func(_ context.Context, network, _ string) (net.Conn, error) {
			conn, err := dialer.Dial(network, target)
			if err != nil {
				return nil, err
			}
			return tls.UClient(conn, &tls.Config{
				ServerName: "speed.cloudflare.com",
			}, tls.HelloChrome_Auto), nil
		},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Duration(timeoutSec) * time.Second,
	}

	req, err := http.NewRequest(http.MethodGet, "https://speed.cloudflare.com/cdn-cgi/trace", nil)
	if err != nil {
		return "", 0, false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36")

	resp, err := client.Do(req)
	if err != nil {
		return "", 0, false
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, false
	}

	latency := time.Since(start)
	if strings.Contains(string(data), "h=speed.cloudflare.com") {
		return target, latency, true
	}
	return "", 0, false
}

func main() {
	input := flag.Arg(0)
	if input == "" {
		log.Fatal("错误: 请提供输入文件路径或 URL URL/CSV/TXT/ZIP\n用法: ./cf-tester [选项] <文件路径或URL>")
	}

	targets, err := readTargets(input)
	if err != nil {
		log.Fatalf("读取目标失败: %v", err)
	}
	log.Printf("[+] 成功加载 %d 个待测目标，开始测试 (线程: %d)...\n", len(targets), threadNum)

	out, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("创建输出文件失败: %v", err)
	}
	defer out.Close()

	dialer := proxy.FromEnvironment()
	sem := make(chan struct{}, threadNum)
	var wg sync.WaitGroup
	var lock sync.Mutex

	validCount := 0
	for _, target := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(t string) {
			defer func() {
				<-sem
				wg.Done()
			}()
			if res, latency, ok := testWorker(dialer, t); ok {
				lock.Lock()
				validCount++
				fmt.Printf("[可用] %-21s 延迟: %v\n", res, latency)
				out.WriteString(res + "\n")
				lock.Unlock()
			}
		}(target)
	}

	wg.Wait()
	log.Printf("[+] 测试完成！有效节点: %d/%d，结果已保存至: %s\n", validCount, len(targets), outputFile)
}
