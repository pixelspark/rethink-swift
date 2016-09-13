Rethink.swift
-------------

A client driver for RethinkDB in Swift.

Looking for a Mac app to easily query RethinkDB? Convert and analyze large data sets at the speed of light with [Warp](http://warp.one). 

### Usage

```swift
R.connect(URL(string: "rethinkdb://localhost:28016")!, user: "admin", password: "") { err, connection in
	assert(err == nil, "Connection error: \(err)")

	// Connected!
	R.dbCreate(databaseName).run(connection) { response in
		assert(!response.isError, "Failed to create database: \(response)")

		R.db(databaseName).tableCreate(tableName).run(connection) { response in
			assert(!response.isError, "Failed to create table: \(response)")

			R.db(databaseName).table(tableName).indexWait().run(connection) { response in
				assert(!response.isError, "Failed to wait for index: \(response)")

				// Insert 1000 documents
				var docs: [ReDocument] = []
				for i in 0..<1000 {
					docs.append(["foo": "bar", "id": i])
				}

				R.db(databaseName).table(tableName).insert(docs).run(connection) { response in
					assert(!response.isError, "Failed to insert data: \(response)")

					R.db(databaseName).table(tableName).filter({ r in return r["foo"].eq(R.expr("bar")) }).run(connection) { response in 
						...
					}

					R.db(databaseName).table(tableName).count().run(connection) { response in
						...
					}
				}
			}
		}
	}
}
```

### Status

The driver implements the V1_0 protocol (which supports username/password authentication using SCRAM, and is available 
from RethinkDB 2.3.0). Alternatively, you can also use V0_4. Some commands and optional arguments may still be missing,
but are usually easy to add to the code.

The driver is written for Swift 3. The last version working in Swift 2.2 can be found at the tag 'last-swift2'.

### Installation

#### Swift Package Manager (SPM)

You can install the driver using Swift Package Manager by adding the following line to your ```Package.swift``` as a dependency:

```
.Package(url: "https://github.com/pixelspark/rethink-swift.git", majorVersion: 0)
```

#### Manual

Drag Rethink.xcodeproj into your own project, then add Rethink or Rethink iOS as dependency (build targets) and link to it.
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
