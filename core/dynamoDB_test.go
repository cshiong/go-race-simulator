package core

import (
	"io/ioutil"
	"code.comcast.com/VariousArtists/csv-context-go"
	"os"
	"testing"
	"strconv"
)

var db *DynamoDB


func init() {
	ctx := context.NewContext(ioutil.Discard)
	d, err := NewDynamoDB(ctx, "local", 10)
	if err != nil {
		ctx.Log.Error(err.Error())
		os.Exit(2)
	}
	db =d


}

func TestDynamoFunctions(t *testing.T) {

	db.Insert("12345567890")

       for i:=0; i<db.numShard; i++ {
		jobs, err :=db.Get(strconv.Itoa(i))
	       if err != nil {
		       t.Logf("failed to get shard: %d",i)
	       }
	       if len(jobs)>0 {
		       t.Logf("job: %v",jobs[0])
	       }
	}



}

