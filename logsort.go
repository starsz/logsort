package logsort

import (
	"bufio"
	"compress/gzip"
	"errors"
	"os"
	"sort"
)

type Action int

const (
	NOP = iota
	SKIP
	STOP
)

var (
	ErrNeedTimeHandler = errors.New("logsort: need time handler")
	ErrSameSRCAndDST   = errors.New("logsort: same src file and dst file")
)

type lineUnit struct {
	offset    int64
	length    int
	timestamp int64
}

type Option struct {
	SrcFile string
	DstFile string
	SrcGzip bool
	DstGzip bool
	GetTime TimeHandler
}

type TimeHandler = func([]byte) (int64, Action, error)

type linesSort []*lineUnit

func (l linesSort) Len() int { return len(l) }

func (l linesSort) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

func (l linesSort) Less(i, j int) bool { return l[i].timestamp < l[j].timestamp }

func SortByOption(option Option) error {
	if option.GetTime == nil {
		return ErrNeedTimeHandler
	}

	if option.SrcFile == option.DstFile {
		return ErrSameSRCAndDST
	}

	var lines linesSort

	srcFd, err := os.Open(option.SrcFile)
	if err != nil {
		return err
	}

	defer srcFd.Close()

	var scanner *bufio.Scanner
	if option.SrcGzip {
		gzFd, err := gzip.NewReader(srcFd)
		if err != nil {
			return err
		}

		defer gzFd.Close()

		scanner = bufio.NewScanner(gzFd)
	} else {
		scanner = bufio.NewScanner(srcFd)
	}

	offset := int64(0)
	for {
		if ok := scanner.Scan(); !ok {
			if err = scanner.Err(); err != nil {
				return err
			}

			// EOF
			break
		}

		line := scanner.Bytes()
		tm, action, err := option.GetTime(line)
		if action == SKIP {
			offset += int64(len(line))
			continue
		} else if action == STOP {
			return err
		}

		l := &lineUnit{
			offset:    offset,
			length:    len(line),
			timestamp: tm,
		}

		// +1 for "\n"
		offset += int64(len(line)) + 1
		lines = append(lines, l)
	}

	sort.Sort(lines)

	dstFd, err := os.Create(option.DstFile)
	if err != nil {
		return err
	}

	defer dstFd.Close()

	var writer *bufio.Writer
	if option.DstGzip {
		gzFd := gzip.NewWriter(dstFd)

		defer gzFd.Close()

		writer = bufio.NewWriter(gzFd)
	} else {
		writer = bufio.NewWriter(dstFd)
	}

	for _, l := range lines {
		line := make([]byte, l.length+1)
		if _, err := srcFd.ReadAt(line, l.offset); err != nil {
			return err
		}

		if _, err := writer.Write(line); err != nil {
			return err
		}

		if err = writer.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func Sort(srcFile, dstFile string, getTime TimeHandler) error {
	option := Option{
		SrcFile: srcFile,
		DstFile: dstFile,
		GetTime: getTime,
	}

	return SortByOption(option)
}
