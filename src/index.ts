import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import mysql from "mysql2/promise";

// Configuration interface
interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  connectionLimit?: number;
  acquireTimeout?: number;
  timeout?: number;
}

class MySQLReadOnlyServer {
  private server: Server;
  private pool: mysql.Pool | null = null;
  private config: DatabaseConfig;

  constructor() {
    this.server = new Server(
      {
        name: "mysql-readonly-server",
        version: "1.0.0",
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    // Load configuration from environment variables
    this.config = {
      host: process.env.MYSQL_HOST || "localhost",
      port: parseInt(process.env.MYSQL_PORT || "3306"),
      user: process.env.MYSQL_USER || "root",
      password: process.env.MYSQL_PASSWORD || "",
      database: process.env.MYSQL_DATABASE || "",
      connectionLimit: parseInt(process.env.MYSQL_CONNECTION_LIMIT || "10"),
      acquireTimeout: parseInt(process.env.MYSQL_ACQUIRE_TIMEOUT || "60000"),
      timeout: parseInt(process.env.MYSQL_TIMEOUT || "60000"),
    };

    this.setupToolHandlers();
  }

  private async createPool(): Promise<mysql.Pool> {
    if (!this.pool) {
      this.pool = mysql.createPool({
        host: this.config.host,
        port: this.config.port,
        user: this.config.user,
        password: this.config.password,
        database: this.config.database,
        waitForConnections: true,
        connectionLimit: this.config.connectionLimit,
        queueLimit: 0,
      });

      // Test the connection
      try {
        const connection = await this.pool.getConnection();
        await connection.ping();
        connection.release();
        console.error("✅ Database connection established successfully");
      } catch (error) {
        console.error("❌ Failed to connect to database:", error);
        throw error;
      }
    }
    return this.pool;
  }

  private isReadOnlyQuery(query: string): boolean {
    const normalizedQuery = query.trim().toLowerCase();

    // List of allowed read-only operations
    const allowedOperations = [
      "select",
      "show",
      "describe",
      "desc",
      "explain",
      "with", // For CTEs that start with WITH
    ];

    // Check if query starts with allowed operations
    const startsWithAllowed = allowedOperations.some(
      (op) => normalizedQuery.startsWith(op + " ") || normalizedQuery === op
    );

    // Additional checks for potentially dangerous operations
    const dangerousKeywords = [
      "insert",
      "update",
      "delete",
      "drop",
      "create",
      "alter",
      "truncate",
      "replace",
      "merge",
      "call", // stored procedures
      "do", // MySQL DO statement
      "load", // LOAD DATA
      "import",
      "export",
      "backup",
      "restore",
      "grant",
      "revoke",
      "flush",
      "reset",
      "shutdown",
      "kill",
      "set", // SET statements can be dangerous
      "lock",
      "unlock",
    ];

    const containsDangerousKeywords = dangerousKeywords.some(
      (keyword) =>
        normalizedQuery.includes(" " + keyword + " ") ||
        normalizedQuery.includes(" " + keyword + "(") ||
        normalizedQuery.startsWith(keyword + " ") ||
        normalizedQuery.startsWith(keyword + "(") ||
        normalizedQuery.endsWith(" " + keyword) ||
        normalizedQuery === keyword
    );

    return startsWithAllowed && !containsDangerousKeywords;
  }

  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: "mysql_query",
            description:
              "Execute a read-only SQL query against the MySQL database. Only SELECT, SHOW, DESCRIBE, EXPLAIN statements are allowed.",
            inputSchema: {
              type: "object",
              properties: {
                query: {
                  type: "string",
                  description:
                    "The SQL query to execute (read-only operations only)",
                },
                limit: {
                  type: "number",
                  description:
                    "Optional limit for the number of rows to return (default: 100, max: 1000)",
                  minimum: 1,
                  maximum: 1000,
                  default: 100,
                },
              },
              required: ["query"],
            },
          },
          {
            name: "mysql_schema",
            description:
              "Get database schema information including tables, columns, and indexes",
            inputSchema: {
              type: "object",
              properties: {
                table_name: {
                  type: "string",
                  description:
                    "Optional: Get schema for a specific table. If not provided, returns all tables",
                },
                include_indexes: {
                  type: "boolean",
                  description:
                    "Whether to include index information (default: false)",
                  default: false,
                },
              },
              required: [],
            },
          },
          {
            name: "mysql_tables",
            description:
              "List all tables in the database with their basic information",
            inputSchema: {
              type: "object",
              properties: {},
              required: [],
            },
          },
        ],
      };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      try {
        const { name, arguments: args } = request.params;

        switch (name) {
          case "mysql_query":
            return await this.handleQuery(
              args as { query: string; limit?: number }
            );

          case "mysql_schema":
            return await this.handleSchema(
              args as { table_name?: string; include_indexes?: boolean }
            );

          case "mysql_tables":
            return await this.handleTables();

          default:
            throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : "Unknown error occurred";
        return {
          content: [
            {
              type: "text",
              text: `Error: ${errorMessage}`,
            },
          ],
        };
      }
    });
  }

  private async handleQuery(args: { query: string; limit?: number }) {
    const { query, limit = 100 } = args;

    if (!this.isReadOnlyQuery(query)) {
      throw new Error(
        "Only read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN) are allowed"
      );
    }

    const pool = await this.createPool();

    try {
      // Apply limit if it's a SELECT query and doesn't already have LIMIT
      let finalQuery = query.trim();
      if (
        finalQuery.toLowerCase().startsWith("select") &&
        !finalQuery.toLowerCase().includes("limit") &&
        limit > 0
      ) {
        finalQuery += ` LIMIT ${Math.min(limit, 1000)}`;
      }

      const [rows, fields] = await pool.execute(finalQuery);

      // Format the results
      let resultText = "";

      if (Array.isArray(rows) && rows.length > 0) {
        // Get column names
        const columns = fields?.map((field) => field.name) || [];

        // Create table header
        if (columns.length > 0) {
          resultText += columns.join(" | ") + "\n";
          resultText += columns.map(() => "---").join(" | ") + "\n";
        }

        // Add rows
        rows.forEach((row: any) => {
          const values = columns.map((col) => {
            const value = row[col];
            if (value === null) return "NULL";
            if (value === undefined) return "undefined";
            return String(value);
          });
          resultText += values.join(" | ") + "\n";
        });

        resultText += `\n(${rows.length} row${
          rows.length !== 1 ? "s" : ""
        } returned)`;
      } else {
        resultText = "Query executed successfully. No rows returned.";
      }

      return {
        content: [
          {
            type: "text",
            text: resultText,
          },
        ],
      };
    } catch (error) {
      throw new Error(
        `Query execution failed: ${
          error instanceof Error ? error.message : "Unknown error"
        }`
      );
    }
  }

  private async handleSchema(args: {
    table_name?: string;
    include_indexes?: boolean;
  }) {
    const { table_name, include_indexes = false } = args;
    const pool = await this.createPool();

    try {
      let resultText = "";

      if (table_name) {
        // Get specific table schema
        const [columns] = await pool.execute(
          "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
          [table_name]
        );

        if (Array.isArray(columns) && columns.length > 0) {
          resultText += `Schema for table: ${table_name}\n\n`;
          resultText += "Column | Type | Nullable | Default | Key | Extra\n";
          resultText += "--- | --- | --- | --- | --- | ---\n";

          columns.forEach((col: any) => {
            resultText += `${col.COLUMN_NAME} | ${col.DATA_TYPE} | ${
              col.IS_NULLABLE
            } | ${col.COLUMN_DEFAULT || "NULL"} | ${col.COLUMN_KEY || ""} | ${
              col.EXTRA || ""
            }\n`;
          });

          if (include_indexes) {
            const [indexes] = await pool.execute(
              "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX",
              [table_name]
            );

            if (Array.isArray(indexes) && indexes.length > 0) {
              resultText += "\n\nIndexes:\n";
              resultText += "Index | Column | Unique\n";
              resultText += "--- | --- | ---\n";

              indexes.forEach((idx: any) => {
                resultText += `${idx.INDEX_NAME} | ${idx.COLUMN_NAME} | ${
                  idx.NON_UNIQUE === 0 ? "Yes" : "No"
                }\n`;
              });
            }
          }
        } else {
          resultText = `Table '${table_name}' not found.`;
        }
      } else {
        // Get all tables
        const [tables] = await pool.execute(
          "SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_ROWS, DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME"
        );

        if (Array.isArray(tables) && tables.length > 0) {
          resultText += "All tables in database:\n\n";
          resultText += "Table | Type | Engine | Rows | Size\n";
          resultText += "--- | --- | --- | --- | ---\n";

          tables.forEach((table: any) => {
            const size = table.DATA_LENGTH
              ? `${Math.round(table.DATA_LENGTH / 1024)} KB`
              : "N/A";
            resultText += `${table.TABLE_NAME} | ${table.TABLE_TYPE} | ${
              table.ENGINE || "N/A"
            } | ${table.TABLE_ROWS || "N/A"} | ${size}\n`;
          });
        } else {
          resultText = "No tables found in database.";
        }
      }

      return {
        content: [
          {
            type: "text",
            text: resultText,
          },
        ],
      };
    } catch (error) {
      throw new Error(
        `Schema query failed: ${
          error instanceof Error ? error.message : "Unknown error"
        }`
      );
    }
  }

  private async handleTables() {
    const pool = await this.createPool();

    try {
      const [tables] = await pool.execute("SHOW TABLES");

      if (Array.isArray(tables) && tables.length > 0) {
        const tableNames = tables.map((table: any) => Object.values(table)[0]);
        const resultText = `Tables in database:\n\n${tableNames
          .map((name) => `• ${name}`)
          .join("\n")}`;

        return {
          content: [
            {
              type: "text",
              text: resultText,
            },
          ],
        };
      } else {
        return {
          content: [
            {
              type: "text",
              text: "No tables found in database.",
            },
          ],
        };
      }
    } catch (error) {
      throw new Error(
        `Failed to list tables: ${
          error instanceof Error ? error.message : "Unknown error"
        }`
      );
    }
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error("MySQL Read-only MCP Server running on stdio");
  }

  async cleanup() {
    if (this.pool) {
      await this.pool.end();
    }
  }
}

// Handle graceful shutdown
const server = new MySQLReadOnlyServer();

process.on("SIGINT", async () => {
  console.error("Received SIGINT, shutting down gracefully...");
  await server.cleanup();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.error("Received SIGTERM, shutting down gracefully...");
  await server.cleanup();
  process.exit(0);
});

// Start the server
server.run().catch((error) => {
  console.error("Failed to run server:", error);
  process.exit(1);
});
