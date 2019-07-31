package resp

import (
	"fmt"
	"github.com/ScottMansfield/nanolog"
)

var (
	isPrint          = true
	LogServer2Client nanolog.Handle
	//LogServer        nanolog.Handle
	//LogServerBufio   nanolog.Handle
	LogProxy nanolog.Handle
	LogData  nanolog.Handle
	LogStart nanolog.Handle
)

func MyPrint(a ...interface{}) {
	if isPrint {
		fmt.Println(a)
	}
}

func init() {
	LogServer2Client = nanolog.AddLogger("ReqId is %s, ChunkId is %i, Server2Client total time is %s, AppendBulk time is %s, Flush time is %s, TimeStamp is %i64")
	//LogServer = nanolog.AddLogger("KEY is %s, IN %s, ReqID is %s, ConnID is %i, ChunkID is %i64, LambdaStoreID is %i64")
	//LogServerBufio = nanolog.AddLogger("ReadBulk ReadLen time is %s, ReadBulk Require time is %s, ReadBulk Append time is %s")
	LogProxy = nanolog.AddLogger("ReqId is %s" +
		"ChunkId is %i " +
		"lambda2Server total time is %s" +
		"First byte time %s, " +
	//"Sever read field0 clientId time is %s, " +
	//"Sever PeekType chunkId time is %s, " +
	//"Sever read field1 chunkId time is %s, " +
	//	"Sever PeekType objBody time is %s, " +
		"Sever read field2 chunkBody time is %s")
	LogData = nanolog.AddLogger("%i, %i, %s, %s, %s, %s, %s, %s")

}
