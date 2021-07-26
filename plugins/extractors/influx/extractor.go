package influx

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/influxdata/influxdb1-client/v2"
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
)

var defaultDBList = []string{
	"information_schema",
	"postgres",
	"root",
}

type Config struct {
	UserID          string `mapstructure:"user_id" validate:"required"`
	Password        string `mapstructure:"password" validate:"required"`
	Host            string `mapstructure:"host" validate:"required"`
	RetentionPolicy string `mapstructure:"retention_policy" default:"autogen"`
	DatabaseName    string `mapstructure:"database_name" default:"test"`
}

type Extractor struct{}

func New() extractor.TableExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(c map[string]interface{}) (result []meta.Table, err error) {
	var config Config
	err = utils.BuildConfig(c, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}

	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("http://%s", config.Host),
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer cli.Close()

	e.getMeasurements(cli, config.DatabaseName, config.RetentionPolicy, result)

	return
}

func (e *Extractor) getMeasurements(influxClient client.Client, dbName, retentionPolicy string, r []meta.Table) (result []meta.Table, err error) {

	var tags []string
	q := client.NewQuery("show measurements", dbName, "")
	response, err := influxClient.Query(q)
	if err != nil || response.Error() != nil {
		return nil, err
	}
	for _, value := range response.Results {
		jsonString, _ := json.MarshalIndent(value, "", "\t")
		fmt.Println(string(jsonString))

		for _, series := range value.Series {
			for _, measurements := range series.Values {
				measurement := (measurements[0]).(string)
				tags, err = e.getTagkeysForMeasurement(influxClient, dbName, measurement)
				if err != nil {
					return
				}
				jsonString, _ := json.MarshalIndent(tags, "", "\t")
				fmt.Println(string(jsonString))
				result = append(result, meta.Table{
					Urn:  fmt.Sprintf("%s.%s.%s", dbName, retentionPolicy, measurements[0]),
					Name: (measurements[0]).(string),
					Custom: &facets.Custom{
						CustomProperties: map[string]string{
							// tags: tags,
						},
					},
				})
			}
		}
	}
	return
}

func (e *Extractor) getDatabases(db *sql.DB) (result []meta.Table, err error) {
	return
}

func (e *Extractor) getTagkeysForMeasurement(influxClient client.Client, dbName, measurement string) (tags []string, err error) {
	q := client.NewQuery(fmt.Sprintf("show tag keys from \"%s\"", measurement), dbName, "")
	response, err := influxClient.Query(q)
	if err != nil || response.Error() != nil {
		return nil, err
	}
	for _, value := range response.Results[0].Series[0].Values {
		tags = append(tags, (value[0]).(string))
	}
	return tags, nil
}

func (e *Extractor) isNullable(value string) bool {
	if value == "YES" {
		return true
	}

	return false
}

func checkNotDefaultDatabase(database string) bool {
	for i := 0; i < len(defaultDBList); i++ {
		if database == defaultDBList[i] {
			return false
		}
	}
	return true
}
