import XCTest
import Rethink

class RethinkTests: XCTestCase {
	var connection: ReConnection!

	override func setUp() {
		do {
			self.connection = try ReConnection(url: NSURL(string: "rethinkdb://localhost:28016")!) {
				print("Connected!")
			}

			// Pump the run loop until we're connected (normally you don't need to do this when in an application)
			while !self.connection.connected && self.connection.error == nil {
				NSRunLoop.mainRunLoop().runMode(NSDefaultRunLoopMode, beforeDate: NSDate.distantFuture())
			}
			XCTAssert(self.connection.error == nil, "Error occurred while connecting")
		}
		catch {
			XCTFail("Error connecting")
		}
		super.setUp()
	}

    func testBasicCommands() {
		let databaseName = "swift_test"
		let tableName = "swift_test"
		var finished = false

		R.uuid().run(connection) { (response) in
			XCTAssert(!response.isError, "Failed to UUID: \(response)")
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

		R.dbCreate(databaseName).run(connection) { (response) in
			XCTAssert(!response.isError, "Failed to create database: \(response)")

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

						R.db(databaseName).table(tableName).count().run(self.connection) { (response) in
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

		while self.connection.error == nil && !finished {
			NSRunLoop.mainRunLoop().runMode(NSDefaultRunLoopMode, beforeDate: NSDate.distantFuture())
		}
		print("Error= \(self.connection.error)")
    }
}
