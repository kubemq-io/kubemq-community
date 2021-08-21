package utils

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/kubemq-io/kubemq-community/config"
	"regexp"
	"strings"
	"text/tabwriter"

	"os"
	"time"
)

var retries = 10

func runWebsocketClientReaderWriter(ctx context.Context, uri string, chRead chan string, chWrite chan string, ready chan struct{}, errCh chan error) {
	var c *websocket.Conn
	for i := 0; i < retries; i++ {
		conn, res, err := websocket.DefaultDialer.Dial(uri, nil)
		if err != nil {
			buf := make([]byte, 1024)
			if res != nil {
				n, _ := res.Body.Read(buf)
				//	errCh <- errors.New(string(buf[:n]))
				Println(string(buf[:n]))
			} else {
				Printlnf("error: attach web socket connection, %s", err.Error())

			}
			time.Sleep(1 * time.Second)
		} else {
			c = conn
			break
		}
	}
	if c == nil {
		os.Exit(1)
	} else {
		defer c.Close()
	}

	ready <- struct{}{}
	go func() {
		for {
			select {
			case msg := <-chWrite:
				err := c.WriteMessage(1, []byte(msg))
				if err != nil {
					Printlnf("error: attach web socket writing, %s", err.Error())
					errCh <- err
					return
				}
			case <-ctx.Done():
				c.Close()
				return
			}

		}

	}()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			Printlnf("error: attach web socket reading, %s", err.Error())
			os.Exit(0)
			errCh <- err
			return
		} else {
			chRead <- string(message)
		}
	}

}

type Message struct {
	Resource string
	Message  string
}

func Run(ctx context.Context, cfg *config.Config, resources []string, include []string, exclude []string) error {
	for _, rsc := range resources {
		pair := strings.Split(rsc, "/")
		if len(pair) != 2 {
			return fmt.Errorf("invalid resource, %s", rsc)
		}
		go runner(ctx, cfg, pair[0], pair[1], include, exclude)
	}
	return nil
}

func runner(ctx context.Context, cfg *config.Config, resType, resChannel string, include []string, exclude []string) {
	var exc []*regexp.Regexp
	var inc []*regexp.Regexp
	for _, ex := range exclude {
		rex, err := regexp.Compile(ex)
		if err != nil {
			continue
		}
		exc = append(exc, rex)
	}
	for _, in := range include {
		rin, err := regexp.Compile(in)
		if err != nil {
			continue
		}
		inc = append(inc, rin)
	}

	uri := fmt.Sprintf("%s/v1/stats/attach?channel=%s&kind=%s", cfg.Client.ApiAddress, resChannel, resType)

	rxChan := make(chan string, 10)
	txChan := make(chan string, 10)
	ready := make(chan struct{})
	errCh := make(chan error, 10)
	go runWebsocketClientReaderWriter(ctx, uri, rxChan, txChan, ready, errCh)
	<-ready
	txChan <- "start"
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.TabIndent)

OUTER:
	for {
		select {
		case msg := <-rxChan:
			for _, rex := range exc {
				if rex.MatchString(msg) {
					continue OUTER
				}
			}

			if len(inc) != 0 {
				matches := false
				for _, rin := range inc {
					if rin.MatchString(msg) {
						matches = true
						break
					}
				}
				if !matches {
					continue OUTER
				}
			}
			msg = strings.Replace(msg, "\n", "", -1)
			msg = strings.Replace(msg, "\t", " ", -1)
			fmt.Fprintf(w, "[%s]\t[%s]\t%s\n", resType, resChannel, decodeBase64(msg))
			w.Flush()
		case <-ctx.Done():
			return
		case <-errCh:
			return
		default:
			time.Sleep(1 * time.Millisecond)
		}

	}
}

func decodeBase64(in string) string {
	// base64 string cannot contain space so this is indication of base64 string
	if !strings.Contains(in, " ") {
		sDec, err := base64.StdEncoding.DecodeString(in)
		if err != nil {
			return in
		}
		return string(sDec)
	}
	return in
}
