import XCTest
import Rethink

class RethinkTests: XCTestCase {
	private func asyncTest(_ block: (_ callback: @escaping () -> ()) -> ()) {
		let expectFinish = self.expectation(description: "CSV tests")

		block {
			expectFinish.fulfill()
		}

		self.waitForExpectations(timeout: 5.0) { (err) -> Void in
			if let e = err {
				// Note: referencing self here deliberately to prevent test from being destroyed prematurely
				print("Error=\(e) \(self)")
			}
		}
	}

	func testSCRAM() {
		let s = SCRAM(username: "user", password: "pencil", nonce: "rOprNGfwEbeRWgbNEkqO")
		XCTAssert(!s.authenticated)
		XCTAssert(s.clientFirstMessage == "n,,n=user,r=rOprNGfwEbeRWgbNEkqO")

		let c2 = s.receive("r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
		XCTAssert(!s.authenticated)
		XCTAssert(c2! == "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
		XCTAssert(!s.authenticated)
		XCTAssert(s.receive("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=") == nil)
		XCTAssert(s.authenticated)
	}

    func testBasicCommands() {
		asyncTest { testDoneCallback in
			R.connect(URL(string: "rethinkdb://localhost:28015")!) { (err, connection) in
				XCTAssert(err == nil, "Connection error: \(err)")

				print("Connected!")
				let databaseName = "swift_test"
				let tableName = "swift_test"

				R.uuid().run(connection) { (response) in
					XCTAssert(!response.isError, "Failed to UUID: \(response)")
				}

				let date = Date()
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
				var outstandingChanges = 1000
				var reader : ReResponse.Callback? = nil
				reader = { (response) -> () in
					XCTAssert(!response.isError, "Failed to fetch documents: \(response)")

					switch response {
						case .rows(_, let cont):
							if cont == nil {
								outstanding -= 1
								print("Outstanding=\(outstanding) outstanding changes=\(outstandingChanges)")
								if outstanding == 0 && outstandingChanges == 0 {
									R.dbDrop(databaseName).run(connection) { (response) in
										XCTAssert(!response.isError, "Failed to drop database: \(response)")
										testDoneCallback()
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
						XCTAssert(response.value is NSArray && (response.value as! NSArray).contains(databaseName), "Created database not listed in response")
					}

					R.db(databaseName).tableCreate(tableName).run(connection) { (response) in
						XCTAssert(!response.isError, "Failed to create table: \(response)")

						R.db(databaseName).table(tableName).indexWait().run(connection) { (response) in
							XCTAssert(!response.isError, "Failed to wait for index: \(response)")

							R.db(databaseName).table(tableName).changes().run(connection) { response in
								XCTAssert(!response.isError, "Failed to obtain changes: \(response)")

								var consumeChanges: ((_ response: ReResponse) -> ())? = nil

								consumeChanges = { (response: ReResponse) -> () in
									if case ReResponse.rows(let docs, let cb) = response {
										outstandingChanges -= docs.count
										print("Received \(docs.count) changes, need \(outstandingChanges) more")
										cb!(consumeChanges!)
									}
									else {
										print("Received unexpected response for .changes request: \(response)")
									}
								}

								consumeChanges!(response)

							}

							// Insert 1000 documents
							var docs: [ReDocument] = []
							for i in 0..<1000 {
								docs.append(["foo": "bar", "id": i])
							}

							R.db(databaseName).table(tableName).insert(docs).run(connection) { (response) in
								XCTAssert(!response.isError, "Failed to insert data: \(response)")

								R.db(databaseName).table(tableName).filter({ r in return r["foo"].eq(R.expr("bar")) }).count().run(connection) { (response) in
									XCTAssert(!response.isError, "Failed to count: \(response)")
									XCTAssert(response.value is NSNumber && (response.value as! NSNumber).intValue == 1000, "Not all documents were inserted, or count is failing: \(response)")

									for _ in 0..<outstanding {
										R.db(databaseName).table(tableName).run(connection, callback: reader!)
									}
								}
							}
						}
					}
				}
			}
		}
	}
}
