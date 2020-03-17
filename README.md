logsort
===========

This is a library that sort log files based on timestamp. Logsort provides a customizable function to get the timestamp from each line, and only store offset and timestamp while reading a file, so it doesn't consume to much RAM. Also, it's used to sort nginx access log for me.

For complete documentation, check out the [Godoc][1].


Feature
===========

- [x] Less RAM
- [x] Support gzip format


Usage
===========

First, define the getTime handler or use TimeStartHandler in library.


```go
func getTime([]byte) (int64, logsort.Action, error) {
            // do parse time in this
}
```

Second, start to merge.

```go
srcFile := "./testdata/base1.log"
dstFile := "./testdata/output.log"
getTime := logsort.TimeStartHandler("2006/01/02 15:04:05")

err := logsort.Sort(srcFile, dstFile, getTime)
```

Example
=========

SrcFile:

```
2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
```

DstFile:

```
2020/01/18 12:20:30 [error] 177003#0: *1004128358 recv() failed (104: Connection reset by peer)
2020/01/18 12:21:55 [error] 177004#0: *1004127283 recv() failed (104: Connection reset by peer)
2020/01/18 12:24:38 [error] 176995#0: *1004136348 [lua] heartbeat.lua:107: cb_heartbeat(): failed to connect: 127.0.0.1:403, timeout, context: ngx.timer
2020/01/18 12:31:05 [error] 177004#0: *1004144640 recv() failed (104: Connection reset by peer)
```

See Also
=========

* logmerge: https://github.com/starsz/logmerge

[1]: https://godoc.org/github.com/starsz/logsort
