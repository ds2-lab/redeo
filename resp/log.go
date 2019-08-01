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
	LogProxy  nanolog.Handle = 10001
	LogData   nanolog.Handle = 10002
	LogStart  nanolog.Handle = 10003
	LogLambda nanolog.Handle = 10004
)

func MyPrint(a ...interface{}) {
	if isPrint {
		fmt.Println(a)
	}
}

func init() {
	//LogServer2Client = nanolog.AddLogger("Cmd is %s, ReqId is %s, ChunkId is %i64, Server2Client total time is %i64, AppendBulk time is %i64, Flush time is %i64, TimeStamp is %i64")
	//LogServer = nanolog.AddLogger("KEY is %s, IN %s, ReqID is %s, ConnID is %i, ChunkID is %i64, LambdaStoreID is %i64")
	//LogServerBufio = nanolog.AddLogger("ReadBulk ReadLen time is %s, ReadBulk Require time is %s, ReadBulk Append time is %s")
	//LogProxy = nanolog.AddLogger("Cmd is %s, ReqId is %s, ChunkId is %i64, First byte is %i64, lambda2Server total time is %i64, Server read chunkBody is %i64")
	//"Sever read field0 clientId time is %s, " +
	//"Sever PeekType chunkId time is %s, " +
	//"Sever read field1 chunkId time is %s, " +
	//	"Sever PeekType objBody time is %s, " +
	LogData = nanolog.AddLogger("%s,%s,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64,%i64")
	//LogLambda = nanolog.AddLogger("%s,%s,%s,%s,%s,%s,%s,%s")
	LogLambda = nanolog.AddLogger("%s,%s")
}
