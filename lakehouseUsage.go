package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/contentsquare/lakehouse-cli/helpers"
	"github.com/contentsquare/lakehouse-cli/providers"
	"github.com/contentsquare/lakehouse-cli/slack"
	"github.com/spf13/cobra"
)

// The Response struct defines the structure to store the result of the ClickHouse query.
type Response struct {
	Table       string `json:"use_table_name"`
	Count       int    `json:"cnt"`
	UnusedTable string `json:"table_name"`
}

// Variables to store flag values.
var (
	lhURL           string
	lhUser          string
	lhPassword      string
	lhRegion        string
	lhEnv           string
	lhCloudProvider string
	lhSlackToken    string
)

// Define the lakehouseUsageCmd command with its usage, short description, and execution function.
var lakehouseUsageCmd = &cobra.Command{
	Use:   "lakehouse-usage",
	Short: "Checks two ClickHouse clusters and finds discrepancies between them",
	RunE:  runLakeHouseUsageCmd,
}

// init function to initialize the command and its flags.
func init() {
	rootCmd.AddCommand(lakehouseUsageCmd)

	// Define persistent flags for the command
	lakehouseUsageCmd.PersistentFlags().StringVar(&lhURL,
		"url",
		"",
		"URL of the ClickHouse cluster")

	lakehouseUsageCmd.PersistentFlags().StringVar(&lhUser,
		"user",
		"default",
		"User to connect to the ClickHouse cluster")

	lakehouseUsageCmd.PersistentFlags().StringVar(&lhPassword,
		"password",
		"",
		"Password to connect to the ClickHouse cluster")

	lakehouseUsageCmd.PersistentFlags().StringVar(&lhRegion,
		"region",
		"",
		"Region of the ClickHouse cluster")

	lakehouseUsageCmd.PersistentFlags().StringVar(&lhEnv,
		"env",
		"",
		"Environment of the ClickHouse cluster")
	lakehouseUsageCmd.PersistentFlags().StringVar(&lhCloudProvider,
		"cloud",
		"",
		"cloud provider of the Clickhouse cluster")

	lakehouseUsageCmd.PersistentFlags().StringVar(&lhSlackToken,
		"slack-token",
		os.Getenv("SLACK_TOKEN"),
		"token to publish stats on Slack")
}

// The runLakeHouseUsageCmd function is executed when the lakehouse-usage command is run.
func runLakeHouseUsageCmd(_ *cobra.Command, _ []string) error {
	// Get the ClickHouse Analytics URL based on the provided region
	clickhouseAnalyticsURL, ok := providers.ClickHouseClusters[providers.LakeHouse][lhRegion]
	if !ok {
		return fmt.Errorf("%s is an invalid ClickHouse analytics region", lhRegion)
	}
	clickhouseAnalyticsURL = fmt.Sprintf(clickhouseAnalyticsURL, lhEnv)

	// Define credentials for connection to Clickhouse Cluster with  User and Password.
	sourceCreds := providers.ClickHouseCredentials{User: lhUser, Password: lhPassword}

	// The number of queries successful per table for yesterday.
	Query := "SELECT _table AS use_table_name, " +
		"cnt, " +
		"st.name AS table_name " +
		"FROM " +
		"( " +
		"SELECT splitByChar('.', `table`)[2] AS _table, " +
		"count(1) AS cnt " +
		"FROM system.query_log " +
		"ARRAY JOIN tables AS `table` " +
		"WHERE event_date = yesterday() " +
		"AND NOT hasAny(databases, ['system']) " +
		"AND query_kind = 'Select' " +
		"AND NOT startsWith(_table, '_tmp') " +
		"AND type IN (2, 3, 4) " +
		"GROUP BY _table " +
		") AS used_tables " +
		"FULL OUTER JOIN system.tables AS st ON st.name = _table " +
		"WHERE st.database = 'default' " +
		"ORDER BY used_tables.cnt DESC" +
		BuildSettings(&Settings{true, true})

	// Execute the query with the ExecuteClickHouseQuery function.
	sourceResponse, err := helpers.ExecuteClickHouseQuery[Response](
		Query,
		clickhouseAnalyticsURL,
		sourceCreds)
	if err != nil {
		return err
	}
	// Process the response data to separate used and unused tables.
	usedTables, unusedTables := processResponseData(sourceResponse.Data)

	// Create a summary text for Slack.
	lhSummaryText := fmt.Sprintf("[%s][%s][%s] Yesterday %d tables have been used on the Lakehouse and %d  not used.",
		lhCloudProvider, lhRegion, lhEnv, len(usedTables), len(unusedTables))
	lhSlackThreadMessage := detailedMessage(usedTables, unusedTables)
	fmt.Println(lhSlackThreadMessage)

	// Publish message in Slack.
	err = slack.PublishMsgInThread(lhSlackToken, lhSummaryText, lhSlackThreadMessage)
	if err != nil {
		return err
	}
	return nil
}

// Convert used tables map to string.
func usedTablesToString(usedTables map[string]int) string {
	var usedTablesStr string
	for key, count := range usedTables {
		usedTablesStr += fmt.Sprintf("%s\t%d\n", key, count)
	}
	return usedTablesStr
}

// Function to process response data and separate used and unused tables.
func processResponseData(resp []*Response) (map[string]int, []string) {
	usedTables := make(map[string]int)
	unusedTables := []string{}
	// Iterate over the response data and print the number of queries.
	for _, row := range resp {
		if row.Count > 0 {
			usedTables[row.Table] = row.Count
		} else {
			unusedTables = append(unusedTables, row.UnusedTable)
		}
	}
	return usedTables, unusedTables
}

// detailedMessage generates a formatted message for Slack with details on used and unused tables.
func detailedMessage(usedTables map[string]int, unusedTables []string) string {
	detailedSlackThreadMessage := fmt.Sprintf("Detailed data consistency:```\nUsed Tables:\n%s\nUnused Tables:\n%s```",
		usedTablesToString(usedTables), strings.Join(unusedTables, ","))
	return detailedSlackThreadMessage
}
