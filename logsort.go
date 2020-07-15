/*
Package logsort provides a method to sort log file by timestamp.

Example:

    srcFile := "disorder.log"
	dstFile := "order.log"
    getTime := func(line []byte) (int64, logmerge.Action, error) {
        tm, err := time.Parse("20060102150405", string(line[:14]))
        if err != nil {
            return 0, logmerge.SKIP, nil
        }

        return tm.Unix(), logmerge.NOP, nil
    }

    err := logMerge.Sort(srcFile, dstFile, getTime)
*/
package logsort

import (
	"bufio"
	"compress/gzip"
	"os"
	"sort"

	"github.com/pkg/errors"
)

// Action defined the read line behaviour.
type Action int

const (
	// NOP: no extra optioin
	NOP = iota
	// SKIP: skip this line
	SKIP
	// STOP: stop file merging
	STOP
)

/*
	TimeHandler defined handlers for getting timestamp from each line.
*/
type TimeHandler = func([]byte) (int64, Action, error)

var (
	// ErrNeedTimeHandler returned when the getTime function is nil.
	ErrNeedTimeHandler = errors.New("logsort: need time handler")
	// ErrSameSRCAndDST returned when the srcfile is same as dstfile.
	ErrSameSRCAndDST = errors.New("logsort: same src file and dst file")
)

type lineUnit struct {
	offset    int64
	length    int
	timestamp int64
	content   []byte
}

/*
	Option defined some option can set for sorting.
*/
type Option struct {
	SrcFile string      // Need sort file path
	DstFile string      // The output file path
	SrcGzip bool        // if srcGzip, logsort will read whole file into RAM
	DstGzip bool        // Output file in gzip format
	GetTime TimeHandler // The function to getTime from each line
}

type linesSort []*lineUnit

func (l linesSort) Len() int { return len(l) }

func (l linesSort) Swap(i, j int) { l[i], l[j] = l[j], l[i] }

func (l linesSort) Less(i, j int) bool { return l[i].timestamp < l[j].timestamp }

/*
	Sort src file, and output to dst file.
	Use getTime function to get timestamp.
*/
func Sort(srcFile, dstFile string, getTime TimeHandler) error {
	option := Option{
		SrcFile: srcFile,
		DstFile: dstFile,
		GetTime: getTime,
	}

	return SortByOption(option)
}

/*
	Use option to control sort behaviour.
	Be careful of using srcGzip, because logsort will read whole file into RAM.
*/
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
			return errors.Wrap(err, "new reader")
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
				return errors.Wrap(err, "scanner err")
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

		if option.SrcGzip {
			temp := string(line)
			l.content = []byte(temp)
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
		var line []byte

		if l.content != nil {
			line = append(l.content, '\n')
		} else {
			line = make([]byte, l.length+1)
			if _, err := srcFd.ReadAt(line, l.offset); err != nil {
				return errors.Wrapf(err, "fd %s read offset %d", option.SrcFile, l.offset)
			}
		}

		if _, err := writer.Write(line); err != nil {
			return errors.Wrapf(err, "writer %s write", option.SrcFile)
		}

		if err = writer.Flush(); err != nil {
			return errors.Wrapf(err, "writer %s flush", option.SrcFile)
		}
	}

	return nil
}
