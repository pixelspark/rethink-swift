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

### License

```
Rethink.swift. Copyright (c) 2015 Pixelspark

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:
The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
```

### FAQ

- __Can I contribute?__

Feel free to send a pull request. If you want to implement a new feature, please open
an issue first, especially if it's a non backward compatible one.