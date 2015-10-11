Rethink.swift
-------------

A Swift client driver for RethinkDB.

### Usage

```swift
let connection = try ReConnection(url: NSURL(string: "rethinkdb://localhost:28016")!) {
	// Connected!
	R.dbCreate(databaseName).run(connection) { (response) in
		assert(!response.isError, "Failed to create database: \(response)")

		R.db(databaseName).tableCreate(tableName).run(self.connection) { (response) in
			assert(!response.isError, "Failed to create table: \(response)")

			R.db(databaseName).table(tableName).indexWait().run(self.connection) { (response) in
				assert(!response.isError, "Failed to wait for index: \(response)")

				// Insert 1000 documents
				var docs: [ReDocument] = []
				for i in 0..<1000 {
					docs.append(["foo": "bar", "id": i])
				}

				R.db(databaseName).table(tableName).insert(docs).run(self.connection) { (response) in
					XCTAssert(!response.isError, "Failed to insert data: \(response)")

					R.db(databaseName).table(tableName).count().run(self.connection) { (response) in
						...
					}
				}
			}
		}
	}
}
```

### Status

This is a very early version of the driver. It implements the V0_4 basic structure (based on JSON). It currently supports
the most basic commands (e.g. creation of tables, databases, ..). No extra option arguments are currently supported, and
neither are expressions. Error handling needs some more love. Do not use in production code!

### Installation

Currently, the code is all in a single file, so adding Rethink.swift to your project should be sufficient. There are no
external dependencies apart from Foundation.

### FAQ

- __Can I contribute?__

Feel free to send a pull request. If you want to implement a new feature, please open
an issue first, especially if it's a non backward compatible one.