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

func WriteMetrics(measurement string, fields map[string]interface{}, metrics map[string]string) {
	conn := getInfluxConn()
	defer conn.Close()

	bp := getDatabase()
	pt := createPoint(measurement, fields, metrics)

	bp.AddPoint(pt)
	saveBatchPoint(bp, conn)
}

//measurement = name of table in influxdb
//metrics
func WriteAllMetrics(measurement string, metrics []map[string]string, fields []map[string]interface{}) {
	conn := getInfluxConn()
	defer conn.Close()
	bp := getDatabase()
	for i, m := range metrics {
		pt := createPoint(measurement, fields[i], m)
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

func createPoint(measurement string, fields map[string]interface{}, metrics map[string]string) *client.Point {
	pt, err := client.NewPoint(measurement, metrics, fields)
	if err != nil {
		fmt.Println(err)
	}
	return pt
}

func saveBatchPoint(bp client.BatchPoints, conn client.Client) {
	for {
		if err := conn.Write(bp); err != nil {
			fmt.Println(err)
			time.Sleep(5000)
		}
		break
	}
}
