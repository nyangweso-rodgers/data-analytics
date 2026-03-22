# ClickHouse MPC Server

# Prerequisites

1. Claude Desktop app installed
2. Node.js (v18 or later) - required for running MCP servers
3. ClickHouse Cloud account with database credentials

# Setup

## 1. Install the ClickHouse MCP Server

- Open your terminal and run:
  ```sh
    npm install -g @modelcontextprotocol/server-clickhouse
  ```

## 2. Configure Claude Desktop

- You need to edit Claude Desktop's configuration file. The location depends on your OS:
  - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
  - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

## 3. Add ClickHouse Configuration

- Edit the config file to add your ClickHouse connection details:
  ```json
  {
    "mcpServers": {
      "clickhouse": {
        "command": "node",
        "args": [
          "/path/to/global/node_modules/@modelcontextprotocol/server-clickhouse/dist/index.js"
        ],
        "env": {
          "CLICKHOUSE_HOST": "your-instance.clickhouse.cloud",
          "CLICKHOUSE_PORT": "8443",
          "CLICKHOUSE_USER": "default",
          "CLICKHOUSE_PASSWORD": "your-password",
          "CLICKHOUSE_DATABASE": "your-database"
        }
      }
    }
  }
  ```
- **Finding the global node_modules path**: Run this command to find where npm installs global packages:
  ```sh
    npm root -g
  ```

## 4. Get Your ClickHouse Cloud Credentials

- From ClickHouse Cloud console:
  - Select your service
  - Click "Connect"
  - Note the hostname, port (usually 8443 for HTTPS), username, and password
  - Copy these into the config above

## 5. Restart Claude Desktop

- Completely quit and restart Claude Desktop for the changes to take effect.

## 6. Verify the Connection

- In a new Claude conversation, you should see a small icon indicating MCP tools are available. You can ask Claude to:
  - "List the tables in my ClickHouse database"
  - "Show me the schema for table X"
  - "Query the last 100 rows from table Y"
  - "Analyze the distribution of column Z"

# Resources and Further Reading
