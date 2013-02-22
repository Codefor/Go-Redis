package redis

import(
    "log"
    "strconv"
)

/**
MasterSpec connect to the sentinel server 

    last-ok-ping-reply
    pending-commands
    quorum
    num-slaves
    info-refresh
    num-other-sentinels
    port
    flags
    name
    ip
    runid
    last-ping-reply

Fetch the master spec
And return
*/
func MasterSpec(sentinelSpec *ConnectionSpec)(spec *ConnectionSpec, err Error) {
    sentinel,err := NewSyncConnection(sentinelSpec)
    response, e := sentinel.ServiceRequest(&SENTINEL, [][]byte{[]byte("MASTERS")})
    if e != nil {
        panic(e)
    }
    status,_    := parseSentinelMasters(response)
    host        := string(status["ip"].([]byte))
    port,_      := strconv.Atoi(string(status["port"].([]byte)))
    log.Println("master confirm:",host,port)
    spec = DefaultSpec().Host(host).Port(port)
    return
}

/**
use the sentinel's host and port
*/
func InitRedisMasterAsynConnection(host string,port int,db int)(client AsyncClient, err Error){
    sentinel_spec := DefaultSpec().Host(host).Port(port)
    master_spec,_ := MasterSpec(sentinel_spec)
    client, err = NewAsynchClientWithSpec(master_spec.Db(db))
    if err != nil {
        panic(err)
    }
    return
}

func parseSentinelMasters(res Response)(m map[string]interface{},err error){
    if data,ok := res.GetMultiBulkData().([]interface{});ok{
        m = make(map[string]interface{})
        length := len(data)
        for i:=0;i<length;i+= 2{
            m[string(data[i].([]byte))] = data[i+1]
        }
    }
    return
}
