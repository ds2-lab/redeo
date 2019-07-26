package resp

import (
	"fmt"
	"github.com/ScottMansfield/nanolog"
)

var (
	isPrint          = true
	LogServer2Client nanolog.Handle
	LogServer        nanolog.Handle
	LogServerBufio   nanolog.Handle
	LogProxy         nanolog.Handle
)

func MyPrint(a ...interface{}) {
	if isPrint {
		fmt.Println(a)
	}
}

func init() {
	LogServer2Client = nanolog.AddLogger("Server AppendInt time is %s, AppendBulk time is %s, Server Flush time is %s, Chunk body len is %i")
	//LogServer = nanolog.AddLogger("KEY is %s, IN %s, ReqID is %s, ConnID is %i, ChunkID is %i64, LambdaStoreID is %i64")
	LogServerBufio = nanolog.AddLogger("ReadBulk ReadLen time is %s, ReadBulk Require time is %s, ReadBulk Append time is %s")
	LogProxy = nanolog.AddLogger("ConnId is %i, " +
		"ChunkId is %i " +
		"Sever PeekType clientId time is %s, " +
		"Sever read field0 clientId time is %s, " +
		"Sever PeekType chunkId time is %s, " +
		"Sever read field1 chunkId time is %s, " +
		"Sever PeekType objBody time is %s, " +
		"Sever read field2 chunkBody time is %s")
}
