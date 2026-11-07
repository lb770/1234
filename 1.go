package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zip"
	tls "github.com/refraction-networking/utls"
	"golang.org/x/net/proxy"
)

var (
	outputFile string
	threadNum  int
)

func init() {
	flag.StringVar(&outputFile, "output", "list.txt", "Set output file path")
	flag.IntVar(&threadNum, "thread", 4, "Set max thread number")
	flag.Parse()
}

func writeContent(writer io.WriteCloser, inputChan <-chan string) {
	defer writer.Close()
	for ip := range inputChan {
		if _, err := writer.Write([]byte(ip + "\n")); err != nil {
			log.Fatal(err)
		}
	}
}

func testWorker(wg *sync.WaitGroup, limitChan chan struct{}, dialer proxy.Dialer, ip string, inputChan chan<- string) {
	limitChan <- struct{}{}
	defer func() { wg.Done(); <-limitChan }()
	tr := &http.Transport{
		DialTLSContext: func(_ context.Context, network, _ string) (net.Conn, error) {
			conn, err := dialer.Dial(network, ip)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.UClient(conn, &tls.Config{
				ServerName: "speed.cloudflare.com",
			}, tls.HelloChrome_Auto)
			return tlsConn, nil
		},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}
	req, err := http.NewRequest(http.MethodGet, "https://speed.cloudflare.com/cdn-cgi/trace", nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64; en-US) AppleWebKit/536.48 (KHTML, like Gecko) Chrome/49.0.3874.325 Safari/603")
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	for e := range strings.SplitSeq(string(data), "\n") {
		if e == "h=speed.cloudflare.com" {
			inputChan <- ip
			return
		}
	}
}

func start(zipFile, outputFile string) error {
	dialer := proxy.FromEnvironment()

	zipReader, err := zip.OpenReader(zipFile)
	if err != nil {
		return err
	}
	defer zipReader.Close()
	if len(zipReader.File) == 0 {
		return errors.New("empty zip file")
	}

	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	inputChan := make(chan string, 128)
	defer close(inputChan)

	// start write content goroutine
	go writeContent(output, inputChan)

	wg := new(sync.WaitGroup)
	limitChan := make(chan struct{}, threadNum)
	for _, f := range zipReader.File {
		rc, err := f.Open()
		if err != nil {
			log.Println("error:", err)
			continue
		}
		splited := strings.Split(f.Name[:len(f.Name)-4], "-")
		port := splited[len(splited)-1]

		defer rc.Close()
		scanner := bufio.NewScanner(rc)
		for scanner.Scan() {
			ip := scanner.Text()
			wg.Add(1)
			go testWorker(wg, limitChan, dialer, ip+":"+port, inputChan)
		}
	}
	wg.Wait()
	return nil
}

func main() {
	zipFile := flag.Arg(0)
	if err := start(zipFile, outputFile); err != nil {
		log.Fatal(err)
	}
}
