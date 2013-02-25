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
    if err != nil{
        log.Fatal(err)
    }
    response, e := sentinel.ServiceRequest(&SENTINEL, [][]byte{[]byte("MASTERS")})
    if e != nil {
        panic(e)
    }
    status,_    := parseSentinelMasters(response)
    host        := string(status["ip"].([]byte))
    port,_      := strconv.Atoi(string(status["port"].([]byte)))
    log.Println("master confirmed:",host,port)
    spec = DefaultSpec().Host(host).Port(port)
    return
}

/**
use the sentinel's host and port
*/
func InitRedisMasterAsynConnection(host string,port int,db int)(conn *AsyncClient, err Error){
    sentinel_spec := DefaultSpec().Host(host).Port(port)
    master_spec,_ := MasterSpec(sentinel_spec)
    master_spec.Db(db)
    client, err := NewAsynchClientWithSpec(master_spec)
    conn = &client
    if err != nil {
        panic(err)
    }
    return
}

func parseSentinelMasters(res Response)(m map[string]interface{},err error){
    hit := res.GetMultiBulkData().([]interface{})[0]
    if data,ok := hit.([]interface{});ok{
        m = make(map[string]interface{})
        length := len(data)
        for i:=0;i<length;i+= 2{
            m[string(data[i].([]byte))] = data[i+1]
        }
    }
    return
}
