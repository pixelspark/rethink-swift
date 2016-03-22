import XCTest
import Rethink

class RethinkTests: XCTestCase {
	@objc func done() {
	}

    func testBasicCommands() {
		var finished = false

		R.connect(NSURL(string: "rethinkdb://localhost:28015")!) { (err, connection) in
			XCTAssert(err == nil, "Connection error: \(err)")

			print("Connected!")
			let databaseName = "swift_test"
			let tableName = "swift_test"

			R.uuid().run(connection) { (response) in
				XCTAssert(!response.isError, "Failed to UUID: \(response)")
			}

			let date = NSDate()
			R.expr(date).run(connection) { (response) in
				XCTAssert(!response.isError && response.value is NSDate, "Failed to date: \(response)")
				print(response)
			}

			R.now().run(connection) { (response) in
				XCTAssert(!response.isError && response.value is NSDate, "Failed to date: \(response)")
				print(response)
			}

			R.range(1, 10).map { e in return e.mul(10) }.run(connection) { response in
				if let r = response.value as? [Int] {
					XCTAssert(r == Array(1..<10).map { return $0 * 10 })
				}
				else {
					XCTAssert(false, "invalid response")
				}
			}

			var outstanding = 100
			var reader : ReResponse.Callback? = nil
			reader = { (response) -> () in
				XCTAssert(!response.isError, "Failed to fetch documents: \(response)")

				switch response {
					case .Rows(_, let cont):
						if cont == nil {
							outstanding -= 1
							print("Outstanding=\(outstanding)")
							if outstanding == 0 {
								R.dbDrop(databaseName).run(connection) { (response) in
									XCTAssert(!response.isError, "Failed to drop database: \(response)")

									// End the run loop
									finished = true
									self.performSelector(#selector(RethinkTests.done), onThread: NSThread.mainThread(), withObject: nil, waitUntilDone: false)
								}
							}
						}
						cont?(reader!)

					default:
						print("Unknown response")
				}
			}

			R.dbCreate(databaseName).run(connection) { (response) in
				XCTAssert(!response.isError, "Failed to create database: \(response)")

				R.dbList().run(connection) { (response) in
					XCTAssert(!response.isError, "Failed to dbList: \(response)")
					XCTAssert(response.value is NSArray && (response.value as! NSArray).containsObject(databaseName), "Created database not listed in response")
				}

				R.db(databaseName).tableCreate(tableName).run(connection) { (response) in
					XCTAssert(!response.isError, "Failed to create table: \(response)")

					R.db(databaseName).table(tableName).indexWait().run(connection) { (response) in
						XCTAssert(!response.isError, "Failed to wait for index: \(response)")

						// Insert 1000 documents
						var docs: [ReDocument] = []
						for i in 0..<1000 {
							docs.append(["foo": "bar", "id": i])
						}

						R.db(databaseName).table(tableName).insert(docs).run(connection) { (response) in
							XCTAssert(!response.isError, "Failed to insert data: \(response)")

							R.db(databaseName).table(tableName).filter({ r in return r["foo"].eq(R.expr("bar")) }).count().run(connection) { (response) in
								XCTAssert(!response.isError, "Failed to count: \(response)")
								XCTAssert(response.value is NSNumber && (response.value as! NSNumber).integerValue == 1000, "Not all documents were inserted, or count is failing: \(response)")

								for _ in 0..<outstanding {
									R.db(databaseName).table(tableName).run(connection, callback: reader!)
								}
							}
						}
					}
				}
			}
		}

		// Keep the test alive while we are not finished (stuff is being done asynchronously)
		while !finished {
			NSRunLoop.mainRunLoop().runMode(NSDefaultRunLoopMode, beforeDate: NSDate.distantFuture())
		}
    }
}
