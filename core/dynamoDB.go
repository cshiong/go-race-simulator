package core

import (
	"code.comcast.com/VariousArtists/csv-context-go"
	"fmt"
	"github.com/AdRoll/goamz/dynamodb"
	"time"

	"crypto/sha1"
	"errors"
	"github.com/AdRoll/goamz/aws"
	"sync"
	"strconv"
)

//simplfied job table column name
const (
	table_name    = "myJob"
	key_name      = "Shard"
	sort_key_name = "Id"
	value_name    = "timeStamp"
	read_cap      = 10
	write_cap     = 10
	poll_size     = 10
)

type Job struct {
	Shard string
	Id    string
	Time  string
}

type DynamoDB struct {
	ctx       *context.Context
	server    *dynamodb.Server
	jobsTable *dynamodb.Table
	tableName string
	region    string
	numShard  int
	mu        *sync.Mutex
}

func NewDynamoDB(ctx *context.Context, region string, numShard int) (*DynamoDB, error) {

	d := &DynamoDB{}
	d.ctx = ctx.SubContext()
	d.ctx.AddLogValue("system", "DynamoDB")

	d.region = region
	d.numShard = numShard
	d.tableName = table_name
	d.mu = &sync.Mutex{}

	d.ctx.Log.Info("op", "NewDynamoDB()", "region", d.region, "tableName", d.tableName)

	// validate regions we can handle
	switch d.region {
	case "local":
		r := aws.Region{DynamoDBEndpoint: "http://127.0.0.1:8000"}
		auth := aws.Auth{AccessKey: "DUMMY_KEY", SecretKey: "DUMMY_SECRET"}
		d.server = dynamodb.New(auth, r)
		d.ctx.Log.Info("dynamoRegion", "local", "table", d.tableName)
	case "":
		return nil, errors.New("region must be specified")
	default:
		r, found := aws.Regions[d.region]
		if !found {
			d.ctx.Log.Error("Bad region name", d.region)
			err := fmt.Errorf("Bad region name '%s'", d.region)
			return nil, err
		}
		auth, err := aws.EnvAuth()
		if err != nil {
			auth, err = aws.GetAuth("", "", "", time.Now().Add(100000*time.Hour))
			if err != nil {
				d.ctx.Log.Error("event", "getAuth", "err", err)
				return nil, err
			}
		}
		d.ctx.Log.Info("dynamoRegion", d.region, "table", d.tableName)
		d.server = dynamodb.New(auth, r)
	}
	// at this point we have a server
	table, err := d.ensureTable()
	if err != nil {
		d.ctx.Log.Error("Error", err.Error())
		return nil, err
	}
	d.jobsTable = table
	d.ctx.Log.Info("table", d.tableName, "actived", true)
	return d, nil
}

//health_table description
func (d *DynamoDB) tableDescription() *dynamodb.TableDescriptionT {
	return &dynamodb.TableDescriptionT{
		TableName: d.tableName,
		AttributeDefinitions: []dynamodb.AttributeDefinitionT{
			dynamodb.AttributeDefinitionT{key_name, "S"},
			dynamodb.AttributeDefinitionT{sort_key_name, "S"},
		},
		KeySchema: []dynamodb.KeySchemaT{
			dynamodb.KeySchemaT{key_name, "HASH"},
			dynamodb.KeySchemaT{sort_key_name, "RANGE"},
		},
		ProvisionedThroughput: dynamodb.ProvisionedThroughputT{
			ReadCapacityUnits:  int64(read_cap),
			WriteCapacityUnits: int64(write_cap),
		},
	}
}

func (d *DynamoDB) DeleteTable(){
	d.server.DeleteTable(*d.tableDescription())
}

// ensureTable verifies the table already exists, otherwise creates it
func (d *DynamoDB) ensureTable() (*dynamodb.Table, error) {
	cx := d.ctx.SubContext()
	cx.AddLogValue("op", "ensureTable()")
	cx.Log.Info("jobsTableName", d.tableName)
	td, err := d.server.DescribeTable(d.tableName)
	if err != nil {
		td = d.tableDescription()
		_, err = d.server.CreateTable(*td)
		if err != nil {
			cx.Log.Error("event", "CreateTable", "message", err)
			return nil, err
		}
	}
	pk, err := td.BuildPrimaryKey()
	if err != nil {
		cx.Log.Error("event", "BuildPrimaryKey", "message", err)
		return nil, err
	}

	// TODO: better wait algo
	for i := 0; i < 60; i++ {
		td, err = d.server.DescribeTable(d.tableName)
		if err != nil {
			time.Sleep(time.Second)
			cx.Log.Info("event", "waitForActive", "RetryableError", err.Error())
			continue
		}
		if td.TableStatus == "ACTIVE" {
			break
		} else {
			cx.Log.Info("event", "waitForActive", "tableStatus", td.TableStatus, "count", i)
			time.Sleep(time.Second)
		}
	}
	if td == nil || td.TableStatus != "ACTIVE" {
		return nil, errors.New("cannot create table %s" + d.tableName)
	}
	return d.server.NewTable(d.tableName, pk), nil
}

func (d *DynamoDB) Insert(Id string) {
	subCtx := d.ctx.SubContext()
	subCtx.AddLogValue("op","Insert()")
	shard := idToShard(Id, d.numShard)
	var attrs []dynamodb.Attribute
	attrs = append(attrs, *dynamodb.NewStringAttribute("Time", Timestamp(time.Now())))
	//not sure if need to lock or not
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.jobsTable.PutItem(shard, Id, attrs)
	if err != nil {
		subCtx.Log.Error("insert_error ", err)
		return

	}
	//subCtx.Log.Info("insert", "success","shard",shard, "jobId",Id, "jobTime",attrs[0].Value)
}

func (d *DynamoDB) Get(shard string) ([]Job, error) {
	cx := d.ctx.SubContext()

	cx.AddLogValue("op", "Get()")
	cx.AddLogValue("shard",shard)
	var jobs []Job
	acs := make([]dynamodb.AttributeComparison, 0, 0)
	acs = append(acs, *dynamodb.NewEqualStringAttributeComparison("Shard", shard))

	q := dynamodb.NewQuery(d.jobsTable)

	attrs := []string{"Shard", "Id", "Time"}
	q.AddAttributesToGet(attrs)
	q.AddKeyConditions(acs)
	q.AddLimit(int64(poll_size))
	q.AddScanIndexForward(false)

	results, _, err := d.jobsTable.QueryTable(q)
	if err != nil {
		return nil, err
	}

	for _, m := range results {
		var job Job
		idAttr, found := m["Id"]
		if !found {
			cx.Log.Error("message", "noIdFound", "result", m)
			continue
		}
		timeAttr, found := m["Time"]
		if !found {
			cx.Log.Error("message", "noDataForAttributeTime", "id", idAttr.Value)
			continue
		}
		job.Shard = shard
		job.Id = idAttr.Value
		job.Time = timeAttr.Value
		//cx.Logger().Info("jobID",job.Id,"jobShard",job.Shard,"jobTime",job.Time)
		jobs = append(jobs, job)

	}
        cx.Logger().Info("jobNumber", strconv.Itoa(len(jobs)))
	return jobs, nil

}
func (d *DynamoDB) Delete(job Job) error {
	ctx := d.ctx.SubContext()
	ctx.AddLogValue("op","Delete()")

	_, err := d.jobsTable.DeleteItem(&dynamodb.Key{job.Shard, job.Id})
	//ctx.Log.Info("job", job.Id, "found", found)
	return err

}

// Timestamp formats into the one true time format
func Timestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func ParseTime(timestamp string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, timestamp)
}

// idToKey is given an ID and returns a Key based on the shard count
func idToShard(id string, numShard int) string {

	hash := sha1.Sum([]byte(id))
	b := byte(0)
	for _, x := range hash {
		b = b ^ x
	}
	shard := fmt.Sprintf("%d", int(b)%numShard)
	return shard
}
