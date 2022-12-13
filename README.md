# Postgres Monitor Agent

The [Postgres Monitor](https://postgresmonitor.com) agent collects useful metrics and telemetry from PostgreSQL databases to help diagnose and improve performance issues.

The agent collects slow query stats, table & index stats and schema data, metrics for replication and other Postgres features, as well as server health metrics (CPU, memory and disk).


## Supported Platforms

The Postgres Monitor agent currently only supports monitoring Heroku PostgreSQL databases but additional platforms will be added in the future.

Please let us know at support@postgresmonitor.com if there is a Postgres platform that you are interested in.


## Getting Started with Heroku

**Note: the Postgres Monitor Heroku Add-on is currently in alpha. If you would like to try out the agent, please reach out to us at support@postgresmonitor.com and we can give you access.**

To monitor a Heroku PostgreSQL database, start by provisioning the Heroku Postgres Monitor add-on at: https://elements.heroku.com/addons/postgres-monitor

After provisioning the Heroku add-on, follow the returned setup instructions or open the Postgres Monitor dashboard to finish setup by running:

```
heroku addons:open postgres-monitor
```


## Configuration

See the Postgres Monitor [configuration docs](https://postgresmonitor.com/docs/configuring-agent/).


## Privacy

The agent obfuscates all query and log data before sending it to the Postgres Monitor API. If you have any questions or concerns about how the agent handles sensitive data, please reach out to [support@postgresmonitor.com](mailto:support@postgresmonitor.com).

See also Postgres Monitor's [privacy policy](https://postgresmonitor.com/privacy).


## Feedback / Support

For any issues or questions about the Postgres Monitor agent, please email us at [support@postgresmonitor.com](mailto:support@postgresmonitor.com).


## Contributing

Please see our [contribution guidelines](docs/contributing.md).


## Testing

Run tests with:

```
go test ./...
```


## License

The agent is licensed under the Apache 2.0 license which can be viewed at [LICENSE](LICENSE).
