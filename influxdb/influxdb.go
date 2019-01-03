package influxdb

import (
	"fmt"
	"time"

	"github.com/brunopita/go-common/commonsys"
	"github.com/influxdata/influxdb/client/v2"
)

var host string
var port string

func init() {
	var env = commonsys.GetEnvironment()
	host = env.GetOrDefault("TELEGRAF_HOST", "localhost")
	port = env.GetOrDefault("TELEGRAF_PORT", "8086")
}

func WriteMetrics(measurement string, fields map[string]interface{}, tag map[string]string) {
	conn := getInfluxConn()
	defer conn.Close()

	bp := getDatabase()
	pt := createPoint(measurement, fields, tag)

	bp.AddPoint(pt)
	saveBatchPoint(bp, conn)
}

//measurement = name of table in influxdb
//fields = name of column and result
//tags = filters
func WriteAllMetrics(measurement string, tags []map[string]string, fields []map[string]interface{}) {
	conn := getInfluxConn()
	defer conn.Close()
	bp := getDatabase()
	for i, tag := range tags {
		pt := createPoint(measurement, fields[i], tag)
		bp.AddPoint(pt)
	}
	saveBatchPoint(bp, conn)

}

func getInfluxConn() client.Client {
	serverAddr := getInfluxServerAddr()
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     serverAddr,
		Username: "",
		Password: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	return c
}

func getInfluxServerAddr() string {
	return fmt.Sprintf("http://%s:%s", host, port)
}

func getDatabase() client.BatchPoints {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "telegraf",
		Precision: "",
	})
	if err != nil {
		fmt.Println(err)
	}
	return bp
}

func createPoint(measurement string, fields map[string]interface{}, tags map[string]string) *client.Point {
	pt, err := client.NewPoint(measurement, tags, fields)
	if err != nil {
		fmt.Println(err)
	}
	return pt
}

// Call influxdb api for save data
func saveBatchPoint(bp client.BatchPoints, conn client.Client) {
	for {
		if err := conn.Write(bp); err != nil {
			fmt.Println(err)
			time.Sleep(5000)
		}
		break
	}
}
