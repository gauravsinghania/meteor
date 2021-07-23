package influx

import (
	"database/sql"
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
		Addr: "http://localhost:8086",
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer cli.Close()

	q := client.NewQuery("show measurements", config.DatabaseName, "")
	response, err := cli.Query(q)
	if err != nil || response.Error() != nil {
		return nil, err
	}
	for _, value := range response.Results {
		// fmt.Println(index)
		// fmt.Println(value)
		for _, v := range value.Series {
			for _, names := range v.Values {
				result = append(result, meta.Table{
					Urn:  fmt.Sprintf("%s.%s.%s", config.DatabaseName, config.RetentionPolicy, names[0]),
					Name: (names[0]).(string),
				})
				// for _, name := range names {
				// 	fmt.Println(name)
				// }
			}
		}
		// jsonString, _ := json.Marshal(value)
		// fmt.Println("Marshal Datasets Result : ", string(jsonString))

		//{0 [{measurements map[] [name] [[measurment1]] false}] [] }
	}

	return
	// db, err := sql.Open("postgres", fmt.Sprintf(
	// 	"postgres://%s:%s@%s/%s?sslmode=disable",
	// 	config.UserID, config.Password, config.Host, config.DatabaseName))
	// if err != nil {
	// 	return
	// }
	// defer db.Close()

	// result, err = e.getDatabases(db)
	// if err != nil {
	// 	return
	// }

	// return
}

func (e *Extractor) getDatabases(db *sql.DB) (result []meta.Table, err error) {
	res, err := db.Query("SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return
	}
	for res.Next() {
		var database string
		res.Scan(&database)
		if checkNotDefaultDatabase(database) {
			result, err = e.getTablesInfo(db, database, result)
			if err != nil {
				return
			}
		}
	}
	return
}

func (e *Extractor) getTablesInfo(db *sql.DB, dbName string, result []meta.Table) (_ []meta.Table, err error) {
	sqlStr := `SELECT table_name
	FROM information_schema.tables
	WHERE table_schema = 'public'
	ORDER BY table_name;`
	_, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", dbName))
	if err != nil {
		return
	}
	rows, err := db.Query(sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return
		}
		var columns []*facets.Column
		columns, err = e.getColumns(db, dbName, tableName)
		if err != nil {
			return
		}

		result = append(result, meta.Table{
			Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
			Name: tableName,
			Schema: &facets.Columns{
				Columns: columns,
			},
		})
	}
	return result, err
}

func (e *Extractor) getColumns(db *sql.DB, dbName string, tableName string) (result []*facets.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE TABLE_NAME = '%s' ORDER BY COLUMN_NAME ASC;`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		return
	}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &dataType, &isNullableString, &length)
		if err != nil {
			return
		}
		result = append(result, &facets.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: e.isNullable(isNullableString),
			Length:     int64(length),
		})
	}
	return result, nil
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
