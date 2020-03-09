package logsort

import (
	"io/ioutil"
	"os"
	"testing"
)

const (
	EXPECTED1 = `2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
`
)

func doSort(srcFile, dstFile string, getTime TimeHandler) (string, error) {
	err := Sort(srcFile, dstFile, getTime)
	if err != nil {
		return "", nil
	}

	dstFd, err := os.Open(dstFile)
	if err != nil {
		return "", nil
	}

	defer os.Remove(dstFile)
	defer dstFd.Close()

	content, err := ioutil.ReadAll(dstFd)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func TestBaseSort(t *testing.T) {
	srcFile := "./testdata/base1.log"
	dstFile := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")

	res, err := doSort(srcFile, dstFile, getTime)
	if err != nil {
		t.Errorf("Sort file error: %s", err.Error())
		return
	}

	if string(res) != EXPECTED1 {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), EXPECTED1)
	}
}

func TestEmptySort(t *testing.T) {
	srcFile := "./testdata/empty1.log"
	dstFile := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")

	res, err := doSort(srcFile, dstFile, getTime)
	if err != nil {
		t.Errorf("Sort file error: %s", err.Error())
		return
	}

	if string(res) != "" {
		t.Errorf("Different content, merge failed\n%s\n%s", string(res), "")
	}
}

func TestGzipSort(t *testing.T) {
	srcFile := "./testdata/base1.log.gz"
	dstFile := "./testdata/output.log"

	getTime := TimeStartHandler("2006/01/02 15:04:05")

	err := SortByOption(Option{SrcFile: srcFile, DstFile: dstFile,
		SrcGzip: true, GetTime: getTime})

	if err != nil {
		t.Errorf("Sort file error: %s", err.Error())
		return
	}

	content, err := ioutil.ReadFile(dstFile)
	if err != nil {
		t.Errorf("Read dst file error: %s", err.Error())
		return
	}

	defer os.Remove(dstFile)

	if string(content) != EXPECTED1 {
		t.Errorf("Different content, merge failed\n%s\n%s", string(content), EXPECTED1)
	}
}
