# omnata-plugin-examples
Public examples of plugins for Omnata Sync

## Prerequisites

1. [Omnata Sync Engine](https://app.snowflake.com/marketplace/listing/GZSUZ59IJT/omnata-omnata-sync-engine) installed in your Snowflake development environment

2. `omnata-plugin-devkit` pip package installed:

```
pip install --upgrade omnata-plugin-devkit
```

3. [snowcli](https://github.com/Snowflake-Labs/snowcli) dev connection configured to your Snowflake development environment. You must be using a role which is:
 - Allowed to create application packages and applications
 - Is granted the OMNATA_SYNC_ENGINE.OMNATA_MANAGEMENT application role



## To install a plugin

```
cd slack/src
omnata plugin_dev upload --package_name SLACK_PLUGIN_PACKAGE
omnata plugin_dev deploy SLACK_PLUGIN_PACKAGE SLACK_PLUGIN_APP
omnata plugin_dev register SLACK_PLUGIN_APP
```

Command notes:
- `upload` downloads pip requirements, zips up the codebase, uploads to a stage in your account and creates an application package
- `deploy` installs the application locally from the application package
- `register` pairs the plugin app with the Omnata Sync Engine, so that the plugin appears in the list when creating connections.
