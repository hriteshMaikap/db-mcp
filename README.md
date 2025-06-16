# MongoDB MCP Server

This is a Model Context Protocol (MCP) server for MongoDB. It is inspired by the official MongoDB MCP server and currently supports read-only tasks.

## Features

* List databases
* List collections
* Find documents
* Count documents
* Aggregate documents
* List collection indexes

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/db-mcp.git
   ```

2. Create a virtual environment and install dependencies:

   ```bash
   uv init
   source .venv/Scripts/activate
   uv add -r requirements.txt
   ```

## Usage

1. Start the server:

   ```bash
   python mongodb/src/server.py
   ```

2. Use the client to connect to the server:

   ```bash
   python client.py
   ```

## Configuration

The server can be configured by editing the `mongodb/config.py` file. The following options are available:

* `MONGODB_URI`: The MongoDB connection URI.
* `HOST`: The host to bind the server to.
* `PORT`: The port to bind the server to.

## Future Scope

* Add context of the database schema for efficient parsing.
