import XCTest
import Rethink

class RethinkTests: XCTestCase {
	var connection: ReConnection!

    func testBasicCommands() {
		var finished = false

		self.connection = R.connect(NSURL(string: "rethinkdb://localhost:28016")!) { (err) in
			XCTAssert(err == nil, "Connection error: \(err)")
			print("Connected!")
			let databaseName = "swift_test"
			let tableName = "swift_test"

			R.uuid().run(self.connection) { (response) in
				XCTAssert(!response.isError, "Failed to UUID: \(response)")
			}

			let date = NSDate()
			R.expr(date).run(self.connection) { (response) in
				XCTAssert(!response.isError && response.value is NSDate, "Failed to date: \(response)")
				print(response)
			}

			R.now().run(self.connection) { (response) in
				XCTAssert(!response.isError && response.value is NSDate, "Failed to date: \(response)")
				print(response)
			}

			var outstanding = 100
			var reader : ReResponse.Callback? = nil
			reader = { (response) -> () in
				XCTAssert(!response.isError, "Failed to fetch documents: \(response)")

				switch response {
					case .Rows(_, let cont):
						if cont == nil {
							outstanding--
							if outstanding == 0 {
								R.dbDrop(databaseName).run(self.connection) { (response) in
									XCTAssert(!response.isError, "Failed to drop database: \(response)")
									finished = true
								}
							}
						}
						cont?(reader!)

					default:
						print("Unknown response")
				}
			}

			R.dbCreate(databaseName).run(self.connection) { (response) in
				XCTAssert(!response.isError, "Failed to create database: \(response)")

				R.dbList().run(self.connection) { (response) in
					XCTAssert(!response.isError, "Failed to dbList: \(response)")
					XCTAssert(response.value is NSArray && (response.value as! NSArray).containsObject(databaseName), "Created database not listed in response")
				}

				R.db(databaseName).tableCreate(tableName).run(self.connection) { (response) in
					XCTAssert(!response.isError, "Failed to create table: \(response)")

					R.db(databaseName).table(tableName).indexWait().run(self.connection) { (response) in
						XCTAssert(!response.isError, "Failed to wait for index: \(response)")

						// Insert 1000 documents
						var docs: [ReDocument] = []
						for i in 0..<1000 {
							docs.append(["foo": "bar", "id": i])
						}

						R.db(databaseName).table(tableName).insert(docs).run(self.connection) { (response) in
							XCTAssert(!response.isError, "Failed to insert data: \(response)")

							R.db(databaseName).table(tableName).filter({ r in return r["foo"].eq(R.expr("bar")) }).count().run(self.connection) { (response) in
								XCTAssert(!response.isError, "Failed to count: \(response)")
								XCTAssert(response.value is NSNumber && (response.value as! NSNumber).integerValue == 1000, "Not all documents were inserted, or count is failing: \(response)")

								for _ in 0..<outstanding {
									R.db(databaseName).table(tableName).run(self.connection, callback: reader!)
								}
							}
						}
					}
				}
			}
		}

		while !finished {
			NSRunLoop.mainRunLoop().runMode(NSDefaultRunLoopMode, beforeDate: NSDate.distantFuture())
		}
		print("Error= \(self.connection.error)")
    }
}
