package db

import (
	"database/sql"
	"time"
)

// https://www.postgresql.org/docs/10/view-pg-settings.html
type Setting struct {
	ServerID       *ServerID
	Name           string
	Value          string
	Unit           sql.NullString
	Category       string
	Description    string
	Context        string
	VarType        string
	Source         string
	MinVal         sql.NullString
	MaxVal         sql.NullString
	EnumVals       sql.NullString
	BootVal        sql.NullString
	ResetVal       sql.NullString
	PendingRestart bool
	MeasuredAt     int64
}

type SettingsMonitor struct {
	settingsChannel chan *Setting
}

func (o *Observer) MonitorSettings() {
	for _, postgresClient := range o.postgresClients {
		go NewMonitorWorker(
			o.config,
			postgresClient,
			&SettingsMonitor{
				settingsChannel: o.settingsChannel,
			},
		).Start()
	}
}

func (m *SettingsMonitor) Run(postgresClient *PostgresClient) {
	settings := m.FindSettings(postgresClient)

	for _, setting := range settings {
		m.settingsChannel <- setting
	}
}

func (m *SettingsMonitor) FindSettings(postgresClient *PostgresClient) []*Setting {
	query := `select name, setting, unit, category, short_desc || ' ' || coalesce(extra_desc, '') as desc,
						context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, pending_restart
						from pg_settings` + postgresMonitorQueryComment()

	rows, err := postgresClient.client.Query(query)

	if err != nil {
		return []*Setting{}
	}
	defer rows.Close()

	var settings []*Setting
	measuredAt := time.Now().UTC().Unix()

	for rows.Next() {
		var setting Setting

		err := rows.Scan(
			&setting.Name,
			&setting.Value,
			&setting.Unit,
			&setting.Category,
			&setting.Description,
			&setting.Context,
			&setting.VarType,
			&setting.Source,
			&setting.MinVal,
			&setting.MaxVal,
			&setting.EnumVals,
			&setting.BootVal,
			&setting.ResetVal,
			&setting.PendingRestart,
		)
		if err != nil {
			continue
		}

		setting.MeasuredAt = measuredAt
		setting.ServerID = postgresClient.serverID

		settings = append(settings, &setting)
	}

	return settings
}
