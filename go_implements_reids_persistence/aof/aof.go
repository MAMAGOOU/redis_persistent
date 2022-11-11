package aof

import (
	"go_implements_reids_persistence/config"
	"go_implements_reids_persistence/interface/database"
	"go_implements_reids_persistence/lib/logger"
	"go_implements_reids_persistence/lib/utils"
	"go_implements_reids_persistence/resp/connection"
	"go_implements_reids_persistence/resp/parser"
	"go_implements_reids_persistence/resp/reply"
	"io"
	"os"
	"strconv"
)

type CmdLine = [][]byte

const aofQueueSize = 1 << 16

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// AofHandler receive messages from channel and write to AOF file
type AofHandler struct {
	database    database.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

// NewAofHandler create a new aof.AofHandler
func NewAofHandler(database database.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.database = database
	// Load Aof
	handler.LoadAof()
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	// channel
	handler.aofChan = make(chan *payload, aofQueueSize)
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

// AddAof send command to aof goroutine through channel
func (handler *AofHandler) AddAof(dbIndex int, cmd CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmd,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into files
func (handler *AofHandler) handleAof() {
	//initialization is already 0, this is for safety
	handler.currentDB = 0
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
			continue
		}
	}
}

// LoadAof read aof files
func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	// when opening the stream, remember to close
	defer file.Close()
	// parsed user instructions
	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need multi bulk")
			continue
		}
		rep := handler.database.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(rep) {
			logger.Error("exec err", rep.ToBytes())
		}
	}
}
