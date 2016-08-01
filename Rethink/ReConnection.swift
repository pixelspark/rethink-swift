/**  Rethink.swift
Copyright (c) 2016 Pixelspark
Author: Tommy van der Vorst (tommy@pixelspark.nl)

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
OTHER DEALINGS IN THE SOFTWARE. **/
import Foundation

public class ReConnection: NSObject, GCDAsyncSocketDelegate {
	public let url: URL
	public let protocolVersion: ReProtocolVersion
	public var authenticationKey: String? { get { return self.url.user } }

	private var state = ReConnectionState.unconnected
	private let socket: ReSocket
	private var outstandingQueries: [ReQueryToken: ReResponse.Callback] = [:]
	private var onConnectCallback: ((String?) -> ())? = nil

	private static var tokenCounter = ReTokenCounter()
	private let queue = DispatchQueue(label: "nl.pixelspark.Rethink.ReConnectionQueue")

	/** Create a connection to a RethinkDB instance. The URL should be of the form 'rethinkdb://host:port'. If
	no port is given, the default port is used. If the server requires the use of an authentication key, put it
	in the 'user' part of the URL, e.g. "rethinkdb://key@server:port". */
	internal init(url: URL, protocolVersion: ReProtocolVersion) {
		self.url = url
		self.protocolVersion = protocolVersion
		self.socket = ReSocket(queue: self.queue)
	}

	internal func connect(_ username: String = ReProtocol.defaultUser, password: String = ReProtocol.defaultPassword, callback: (ReError?) -> ()) {
		self.socket.connect(self.url) { err in
			if let e = err {
				return callback(ReError.fatal(e))
			}

			// Start authentication
			guard let data = NSMutableData(capacity: 128) else {
				let e = ReError.fatal("Could not create data object")
				self.state = .error(e)
				return callback(e)
			}

			switch self.protocolVersion {
			case .v0_4:
				// Append protocol version
				data.append(Data.dataWithLittleEndianOf(UInt32(self.protocolVersion.protocolVersionCode)))

				// Append authentication key length and the key itself (as ASCII)
				if let authKey = self.authenticationKey?.data(using: String.Encoding.ascii) {
					data.append(Data.dataWithLittleEndianOf(UInt32(authKey.count)))
					data.append(authKey)
				}
				else {
					data.append(Data.dataWithLittleEndianOf(UInt32(0)))
				}

				// Append protocol type (JSON)
				data.append(Data.dataWithLittleEndianOf(UInt32(ReProtocol.protocolType)))

			case .v1_0:
				data.append(Data.dataWithLittleEndianOf(UInt32(self.protocolVersion.protocolVersionCode)))
			}

			self.socket.write(data as Data) { err in
				if let e = err {
					self.state = .error(ReError.fatal(e))
					return callback(ReError.fatal(e))
				}

				self.state = .handshakeSent

				// Let's see if we get a reply
				if self.socket.state == .connected {
					self.socket.readZeroTerminatedASCII() { s in
						switch self.protocolVersion {
						case .v0_4:
							if s == ReProtocol.handshakeSuccessResponse {
								// Start read loop
								self.state = .connected
								self.startReading()
								return callback(nil)
							}
							else {
								let e = ReError.fatal("Handshake failed, server returned: \(s)")
								self.state = .error(e)
								return callback(e)
							}

						case .v1_0:
							do {
								if let replyString = s {
									/* The reply is a JSON object containing the keys 'success' (should be true), 'min_protocol_version',
									'max_protocol_version' and 'server_version'. */
									let reply = try JSONSerialization.jsonObject(with: replyString.data(using: String.Encoding.ascii)!, options: [])

									if let replyDictionary = reply as? [String: AnyObject], let success = replyDictionary["success"] as? NSNumber, success.boolValue {
										self.performSCRAMAuthentication(username, password: password) { err in
											if err == nil {
												// Start read loop
												self.state = .connected
												self.startReading()
												return callback(nil)
											}
											else {
												self.state = .error(err!)
												return callback(err)
											}
										}
									}
									else {
										/* On error, the server will return a null-terminated error string (non JSON), 
										or a JSON object with 'success' set to false. */
										let e = ReError.fatal("Server returned \(replyString)")
										self.state = .error(e)
										return callback(e)
									}
								}
								else {
									let e = ReError.fatal("Handshake failed, server returned: \(s)")
									self.state = .error(e)
									return callback(e)
								}
							}
							catch let error {
								return callback(ReError.fatal(error.localizedDescription))
							}
						}
					}
				}
			}
		}
	}

	private func performSCRAMAuthentication(_ username: String, password: String, callback: (ReError?) -> ()) {
		assert(self.protocolVersion == .v1_0, "SCRAM authentication not supported with protocol version \(self.protocolVersion)")
		let scram = SCRAM(username: username, password: password)
		var zeroByte: UInt8 = 0x00

		// Send authentication first message
		do {
			let firstMessage = ["protocol_version": 0, "authentication_method": "SCRAM-SHA-256", "authentication": scram.clientFirstMessage]
			let data = try JSONSerialization.data(withJSONObject: firstMessage, options: [])
			var zeroTerminatedData = NSData(data: data) as Data
			zeroTerminatedData.append(&zeroByte, count: 1)

			self.socket.write(zeroTerminatedData) { err in
				if let e = err {
					return callback(ReError.fatal(e))
				}

				// Server should send us some JSON back
				self.socket.readZeroTerminatedASCII() { replyString in
					do {
						if let s = replyString {
							if let reply = try JSONSerialization.jsonObject(with: s.data(using: String.Encoding.ascii)!, options: []) as? [String: AnyObject] {
								if let success = reply["success"] as? NSNumber, success.boolValue {
									let authData = reply["authentication"] as! String
									if let shouldSend = scram.receive(authData) {
										// Send the generated reply back
										let secondMessage = [
											"authentication": shouldSend
										]
										let secondReply = try JSONSerialization.data(withJSONObject: secondMessage, options: [])
										var zeroSecondReply = NSData(data: secondReply) as Data
										zeroSecondReply.append(&zeroByte, count: 1)

										self.socket.write(zeroSecondReply) { err in
											if let e = err {
												return callback(ReError.fatal(e))
											}

											// Verify server signature
											self.socket.readZeroTerminatedASCII() { replyString in
												do {
													if let s = replyString {
														if let reply = try JSONSerialization.jsonObject(with: s.data(using: String.Encoding.ascii)!, options: []) as? [String: AnyObject] {
															if let success = reply["success"] as? NSNumber, success.boolValue {
																let authData = reply["authentication"] as! String
																scram.receive(authData)
																if scram.authenticated {
																	return callback(nil)
																}
																else {
																	return callback(ReError.fatal("SCRAM authentication invalid!"))
																}
															}
															else {
																if let errorString = reply["error"] as? String {
																	return callback(ReError.fatal(errorString))
																}
																else {
																	return callback(ReError.fatal("Server returned \(s)"))
																}
															}
														}
														else {
															return callback(ReError.fatal("Server returned \(s)"))
														}
													}
													else {
														return callback(ReError.fatal("Server did not return a server final message in SCRAM exchange"))
													}
												}
												catch let error {
													return callback(ReError.fatal(error.localizedDescription))
												}
											}
										}
									}
									else {
										return callback(ReError.fatal("SCRAM authentication failed"))
									}
								}
								else {
									if let errorString = reply["error"] as? String {
										return callback(ReError.fatal(errorString))
									}
									else {
										return callback(ReError.fatal("Server returned \(s)"))
									}
								}
							}
							else {
								return callback(ReError.fatal("Server returned \(s)"))
							}
						}
						else {
							return callback(ReError.fatal("Server did not return a reply to our first authentication message"))
						}
					}
					catch let error {
						return callback(ReError.fatal(error.localizedDescription))
					}
				}
			}
		}
		catch let error {
			return callback(ReError.fatal(error.localizedDescription))
		}
	}

	public func close() {
		self.queue.async {
			self.socket.disconnect()
			self.state = .unconnected
			self.outstandingQueries.removeAll()
		}
	}

	public var connected: Bool {
		if case ReConnectionState.connected = state {
			return true
		}
		return false
	}

	public var error: ReError? {
		switch self.state {
		case .error(let e):
			return e
		default: return nil
		}
	}

	private func startReading() {
		self.socket.read(8 + 4) { data in
			if let d = data {
				let queryToken = d.readLittleEndianUInt64(0)
				let responseSize = d.readLittleEndianUInt32(8)

				self.socket.read(Int(responseSize), callback: { data in
					if let d = data {
						assert(d.count == Int(responseSize))

						var called = false
						let continuation: ReResponse.ContinuationCallback = { [weak self] (cb: ReResponse.Callback) -> () in
							assert(!called, "continuation callback for query token \(queryToken) must never be called more than once")
							called = true
							self?.sendContinuation(queryToken, callback: cb)
						}

						self.queue.async {
							if let handler = self.outstandingQueries[queryToken] {
								if let response = ReResponse(json: d, continuation: continuation) {
									self.outstandingQueries.removeValue(forKey: queryToken)
									handler(response)
									self.startReading()
								}
								else {
									self.state = .error(ReError.fatal("Invalid response object from server"))
								}
							}
						}
					}
					else {
						self.state = .error(ReError.fatal("Disconnected"))
					}
				})
			}
			else {
				self.state = .error(ReError.fatal("Disconnected"))
			}
		}
	}

	private func sendContinuation(_ token: ReQueryToken, callback: ReResponse.Callback) {
		let json = [ReProtocol.ReQueryType.continue.rawValue];
		let query = try! JSONSerialization.data(withJSONObject: json, options: [])
		self.sendQuery(query, token: token, callback: callback)
	}

	private func dummy() {
	}

	private func sendQuery(_ query: Data, token: ReQueryToken, callback: ReResponse.Callback) {
		queue.async {
			assert(self.outstandingQueries[token] == nil, "A query with token \(token) is already outstanding")
			assert(self.connected, "Cannot send a query when the connection is not open")
			let data = NSMutableData(capacity: query.count + 8 + 4)!

			let reffingCallback: ReResponse.Callback = { (res) -> () in
				// This is used to create a reference to ReConnection, and keeps it alive at least until the query has finished.
				self.dummy()
				callback(res)
			}


			data.append(Data.dataWithLittleEndianOf(token))
			data.append(Data.dataWithLittleEndianOf(UInt32(query.count)))
			data.append(query)
			self.socket.write(data as Data) { err in
				if let e = err {
					self.state = .error(ReError.fatal(e))
					callback(ReResponse.error(e))
				}
				else {
					self.outstandingQueries[token] = reffingCallback
				}
			}
		}
	}

	internal func startQuery(_ query: Data, callback: ReResponse.Callback) throws {
		queue.async {
			let token = ReConnection.tokenCounter.next()
			self.sendQuery(query, token: token, callback: callback)
		}
	}
}

private enum ReConnectionState {
	case unconnected // Nothing has been done yet
	case handshakeSent // Our handshake has been sent, we are waiting for confirmation of success
	case connected // Handshake has been completed, and we are awaiting respones from the server
	case error(ReError) // A protocol error has occurred
	case terminated // The connection has been terminated
}

private extension Data {
	static func dataWithLittleEndianOf(_ nr: UInt64) -> Data {
		var swapped = CFSwapInt64HostToLittle(nr)

		var bytes: [UInt8] = [0,0,0,0,0,0,0,0]
		for i in 0...7 {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return Data(bytes: UnsafePointer<UInt8>(bytes), count: 8)
	}

	static func dataWithLittleEndianOf(_ nr: UInt32) -> Data {
		var swapped = CFSwapInt32HostToLittle(nr) // No-op on little endian archs

		var bytes: [UInt8] = [0,0,0,0]
		for i in 0...3 {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return Data(bytes: UnsafePointer<UInt8>(bytes), count: 4)
	}

	func readLittleEndianUInt64(_ atIndex: Int = 0) -> UInt64 {
		assert(self.count >= atIndex + 8)
		let buffer = UnsafeMutablePointer<UInt8>((self as NSData).bytes)
		var read: UInt64 = 0
		for i in (0...7).reversed() {
			read = (read << 8) + UInt64(buffer[atIndex + i])
		}
		return CFSwapInt64LittleToHost(read)
	}

	func readLittleEndianUInt32(_ atIndex: Int = 0) -> UInt32 {
		assert(self.count >= (atIndex + 4))
		let buffer = UnsafeMutablePointer<UInt8>((self as NSData).bytes)
		var read: UInt32 = 0
		for i in (0...3).reversed() {
			read = (read << 8) + UInt32(buffer[atIndex + i])
		}
		return CFSwapInt32LittleToHost(read)
	}
}
