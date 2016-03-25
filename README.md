Rethink.swift
-------------

A client driver for RethinkDB in Swift.

Looking for a Mac app to easily query RethinkDB? Convert and analyze large data sets at the speed of light with [Warp](http://warp.one). 

### Usage

```swift
self.connection = R.connect(NSURL(string: "rethinkdb://localhost:28016")!) { err in
	assert(err == nil, "Connection error: \(err)")

	// Connected!
	R.dbCreate(databaseName).run(connection) { response in
		assert(!response.isError, "Failed to create database: \(response)")

		R.db(databaseName).tableCreate(tableName).run(self.connection) { response in
			assert(!response.isError, "Failed to create table: \(response)")

			R.db(databaseName).table(tableName).indexWait().run(self.connection) { response in
				assert(!response.isError, "Failed to wait for index: \(response)")

				// Insert 1000 documents
				var docs: [ReDocument] = []
				for i in 0..<1000 {
					docs.append(["foo": "bar", "id": i])
				}

				R.db(databaseName).table(tableName).insert(docs).run(self.connection) { response in
					assert(!response.isError, "Failed to insert data: \(response)")

					R.db(databaseName).table(tableName).filter({ r in return r["foo"].eq(R.expr("bar")) }).run(self.connection) { response in 
						...
					}

					R.db(databaseName).table(tableName).count().run(self.connection) { response in
						...
					}
				}
			}
		}
	}
}
```

### Status

This is an early but functional and quite reliable version of the driver. It implements the V0_4 protocol (based on JSON). It currently supports
the most basic commands (e.g. creation of tables, databases, ..). Not all of the extra option arguments are currently supported (but these
can easily be added to the driver code).

### Installation

Drag Rethink.xcodeproj into your own project, then add Rethink.framework as dependency (build targets) and link to it.
You should then be able to simply 'import Rethink' from Swift code.

### License

```
Rethink.swift. Copyright (c) 2015-2016 Pixelspark

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