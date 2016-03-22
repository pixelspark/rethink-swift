/**  Rethink.swift
Copyright (c) 2015 Pixelspark
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

private typealias ReQueryToken = UInt64

private struct ReTokenCounter {
	private var nextQueryToken: UInt64 = 0x5ADFACE
	private var queue = dispatch_queue_create("nl.pixelspark.Rethink.ReTokenCounter", DISPATCH_QUEUE_SERIAL)

	mutating func next() -> ReQueryToken {
		var nt: UInt64 = 0
		dispatch_sync(self.queue) {
			nt = self.nextQueryToken
			self.nextQueryToken += 1
		}
		return nt
	}
}

private enum ReSocketState {
	case Unconnected
	case Connecting
	case Connected
}

private class ReSocket: GCDAsyncSocketDelegate {
	typealias WriteCallback = (String?) -> ()
	typealias ReadCallback = (NSData?) -> ()
	private static let timeOut = 5.0

	let socket: GCDAsyncSocket
	private var state: ReSocketState = .Unconnected

	private var onConnect: ((String?) -> ())?
	private var writeCallbacks: [Int: WriteCallback] = [:]
	private var readCallbacks: [Int: ReadCallback] = [:]

	init(queue: dispatch_queue_t) {
		self.socket = GCDAsyncSocket(delegate: nil, delegateQueue: queue)
		self.socket.delegate = self
	}

	func connect(url: NSURL, callback: (String?) -> ()) {
		assert(self.state == .Unconnected, "Already connected or connecting")
		self.onConnect = callback
		self.state = .Connecting

		guard let host = url.host else { return callback("Invalid URL") }
		let port = url.port ?? 28015

		do {
			try socket.connectToHost(host, onPort: port.unsignedShortValue)
		}
		catch let e as NSError {
			return callback(e.localizedDescription)
		}
	}

	@objc private func socket(sock: GCDAsyncSocket!, didConnectToHost host: String!, port: UInt16) {
		self.state = .Connected
		self.onConnect?(nil)
	}

	@objc private func socketDidDisconnect(sock: GCDAsyncSocket!, withError err: NSError!) {
		self.state = .Unconnected
	}

	func read(length: Int, callback: ReadCallback)  {
		assert(length > 0, "Length cannot be zero or less")

		if self.state != .Connected {
			return callback(nil)
		}

		dispatch_async(socket.delegateQueue) {
			let tag = (self.readCallbacks.count + 1)
			self.readCallbacks[tag] = callback
			self.socket.readDataToLength(UInt(length), withTimeout: ReSocket.timeOut, tag: tag)
		}
	}

	func readZeroTerminatedASCII(callback: (String?) -> ()) {
		if self.state != .Connected {
			return callback(nil)
		}

		let zero = NSData(bytes: [UInt8(0)], length: 1)
		dispatch_async(socket.delegateQueue) {
			let tag = (self.readCallbacks.count + 1)
			self.readCallbacks[tag] = { data in
				if let d = data {
					let s = NSString(data: d.subdataWithRange(NSRange(location: 0, length: d.length - 1)), encoding: NSASCIIStringEncoding)!
					callback(String(s))
				}
				else {
					callback(nil)
				}
			}
			self.socket.readDataToData(zero, withTimeout: ReSocket.timeOut, tag: tag)
		}
	}

	func write(data: NSData, callback: WriteCallback) {
		if self.state != .Connected {
			return callback("socket is not connected!")
		}

		dispatch_async(socket.delegateQueue) {
			let tag = (self.writeCallbacks.count + 1)
			self.writeCallbacks[tag] = callback
			self.socket.writeData(data, withTimeout: ReSocket.timeOut, tag: tag)
		}
	}

	@objc private func socket(sock: GCDAsyncSocket!, didWriteDataWithTag tag: Int) {
		dispatch_async(socket.delegateQueue) {
			if let cb = self.writeCallbacks[tag] {
				cb(nil)
				self.writeCallbacks.removeValueForKey(tag)
			}
		}
	}

	@objc private func socket(sock: GCDAsyncSocket!, didReadData data: NSData!, withTag tag: Int) {
		dispatch_async(socket.delegateQueue) {
			if let cb = self.readCallbacks[tag] {
				cb(data)
				self.readCallbacks.removeValueForKey(tag)
			}
		}
	}

	deinit {
		self.socket.disconnect()
	}
}

public class ReConnection: NSObject, GCDAsyncSocketDelegate {
	public let url: NSURL
	public var authenticationKey: String? { get { return self.url.user } }

	private var state = ReConnectionState.Unconnected
	private let socket: ReSocket
	private var outstandingQueries: [ReQueryToken: ReResponse.Callback] = [:]
	private var onConnectCallback: ((String?) -> ())? = nil

	private static var tokenCounter = ReTokenCounter()
	private let queue = dispatch_queue_create("nl.pixelspark.Rethink.ReConnectionQueue", DISPATCH_QUEUE_SERIAL)

	/** Create a connection to a RethinkDB instance. The URL should be of the form 'rethinkdb://host:port'. If
	no port is given, the default port is used. If the server requires the use of an authentication key, put it
	in the 'user' part of the URL, e.g. "rethinkdb://key@server:port". */
	internal init(url: NSURL) {
		self.url = url
		self.socket = ReSocket(queue: self.queue)
	}

	internal func connect(callback: (ReError?) -> ()) {
		self.socket.connect(self.url) { err in
			if let e = err {
				return callback(ReError.Fatal(e))
			}

			// Start authentication
			guard let data = NSMutableData(capacity: 128) else {
				let e = ReError.Fatal("Could not create data object")
				self.state = .Error(e)
				return callback(e)
			}

			// Append protocol version
			data.appendData(NSData.dataWithLittleEndianOf(UInt32(ReProtocol.protocolVersion)))

			// Append authentication key length and the key itself (as ASCII)
			if let authKey = self.authenticationKey?.dataUsingEncoding(NSASCIIStringEncoding) {
				data.appendData(NSData.dataWithLittleEndianOf(UInt32(authKey.length)))
				data.appendData(authKey)
			}
			else {
				data.appendData(NSData.dataWithLittleEndianOf(UInt32(0)))
			}

			// Append protocol type (JSON)
			data.appendData(NSData.dataWithLittleEndianOf(UInt32(ReProtocol.protocolType)))
			self.socket.write(data) { err in
				if let e = err {
					self.state = .Error(ReError.Fatal(e))
					return callback(ReError.Fatal(e))
				}

				self.state = .HandshakeSent

				// Let's see if we get a reply
				if self.socket.state == .Connected {
					self.socket.readZeroTerminatedASCII() { s in
						if s == ReProtocol.handshakeSuccessResponse {
							// Start read loop
							self.state = .Connected
							self.startReading()
							return callback(nil)
						}
						else {
							let e = ReError.Fatal("Handshake failed, server returned: \(s)")
							self.state = .Error(e)
							return callback(e)
						}
					}
				}
			}
		}
	}

	public var connected: Bool {
		if case ReConnectionState.Connected = state {
			return true
		}
		return false
	}

	public var error: ReError? {
		return self.state.error
	}

	private func startReading() {
		self.socket.read(8 + 4) { data in
			if let d = data {
				let queryToken = d.readLittleEndianUInt64(0)
				let responseSize = d.readLittleEndianUInt32(8)

				self.socket.read(Int(responseSize), callback: { data in
					if let d = data {
						assert(d.length == Int(responseSize))

						var called = false
						let continuation: ReResponse.ContinuationCallback = { [weak self] (cb: ReResponse.Callback) -> () in
							assert(!called, "continuation callback for query token \(queryToken) must never be called more than once")
							called = true
							self?.sendContinuation(queryToken, callback: cb)
						}

						dispatch_async(self.queue) {
							if let handler = self.outstandingQueries[queryToken] {
								if let response = ReResponse(json: d, continuation: continuation) {
									self.outstandingQueries.removeValueForKey(queryToken)
									handler(response)
									self.startReading()
								}
								else {
									self.state = .Error(ReError.Fatal("Invalid response object from server"))
								}
							}
							else {
								fatalError("No handler found for server response. This should never happen unless server is behaving badly.")
							}
						}
					}
					else {
						self.state = .Error(ReError.Fatal("Disconnected"))
					}
				})
			}
			else {
				self.state = .Error(ReError.Fatal("Disconnected"))
			}
		}
	}

	private func sendContinuation(token: ReQueryToken, callback: ReResponse.Callback) {
		let json = [ReProtocol.ReQueryType.CONTINUE.rawValue];
		let query = try! NSJSONSerialization.dataWithJSONObject(json, options: [])
		self.sendQuery(query, token: token, callback: callback)
	}

	private func dummy() {
	}

	private func sendQuery(query: NSData, token: ReQueryToken, callback: ReResponse.Callback) {
		dispatch_async(queue) {
			assert(self.outstandingQueries[token] == nil, "A query with token \(token) is already outstanding")
			assert(self.connected, "Cannot send a query when the connection is not open")
			let data = NSMutableData(capacity: query.length + 8 + 4)!

			let reffingCallback: ReResponse.Callback = { (res) -> () in
				// This is used to create a reference to ReConnection, and keeps it alive at least until the query has finished.
				self.dummy()
				callback(res)
			}


			data.appendData(NSData.dataWithLittleEndianOf(token))
			data.appendData(NSData.dataWithLittleEndianOf(UInt32(query.length)))
			data.appendData(query)
			self.socket.write(data) { err in
				if let e = err {
					self.state = .Error(ReError.Fatal(e))
					callback(ReResponse.Error(e))
				}
				else {
					self.outstandingQueries[token] = reffingCallback
				}
			}
		}
	}

	internal func startQuery(query: NSData, callback: ReResponse.Callback) throws {
		dispatch_async(queue) {
			let token = ReConnection.tokenCounter.next()
			self.sendQuery(query, token: token, callback: callback)
		}
	}
}

private enum ReConnectionState {
	case Unconnected // Nothing has been done yet
	case HandshakeSent // Our handshake has been sent, we are waiting for confirmation of success
	case Connected // Handshake has been completed, and we are awaiting respones from the server
	case Error(ReError) // A protocol error has occurred
	case Terminated // The connection has been terminated

	var error: ReError? {
		switch self {
		case .Error(let e):
			return e

		default:
			return nil
		}
	}
}

public enum ReError: ErrorType {
	case Fatal(String)
	case Other(ErrorType)
}

public enum ReResponse {
	public typealias Callback = (ReResponse) -> ()
	public typealias ContinuationCallback = (Callback) -> ()

	case Error(String)
	case Value(AnyObject)
	case Rows([ReDocument], ContinuationCallback?)
	case Unknown

	init?(json: NSData, continuation: ContinuationCallback) {
		do {
			if let d = try NSJSONSerialization.JSONObjectWithData(json, options: []) as? NSDictionary {
				if let type = d.valueForKey("t") as? NSNumber {
					switch type.integerValue {
					case ReProtocol.responseTypeSuccessAtom:
						guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
						if r.count != 1 { return nil }
						self = .Value(ReDatum(jsonSerialization: r.first!).value)

					case ReProtocol.responseTypeSuccessPartial, ReProtocol.responseTypeSuccessSequence:
						if let r = d.valueForKey("r") as? [[String: AnyObject]] {
							let deserialized = r.map { (document) -> ReDocument in
								var dedoc: ReDocument = [:]
								for (k, v) in document {
									dedoc[k] = ReDatum(jsonSerialization: v).value
								}
								return dedoc
							}

							self = .Rows(deserialized, type.integerValue == ReProtocol.responseTypeSuccessPartial ? continuation : nil)
						}
						else if let r = d.valueForKey("r") as? [AnyObject] {
							let deserialized = r.map { (value) -> AnyObject in
								return ReDatum(jsonSerialization: value).value
							}

							self = .Value(deserialized)
						}
						else {
							return nil
						}

					case ReProtocol.responseTypeClientError:
						guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
						if r.count != 1 { return nil }
						self = .Error("Client error: \(r.first!)")

					case ReProtocol.responseTypeCompileError:
						guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
						if r.count != 1 { return nil }
						self = .Error("Compile error: \(r.first!)")

					case ReProtocol.responseTypeRuntimeError:
						guard let r = d.valueForKey("r") as? [AnyObject] else { return nil }
						if r.count != 1 { return nil }
						self = .Error("Run-time error: \(r.first!)")

					default:
						self = .Unknown
					}
				}
				else {
					return nil
				}
			}
			else {
				return nil
			}
		}
		catch {
			return nil
		}
	}

	public var isError: Bool {
		switch self {
		case .Error(_): return true
		default: return false
		}
	}

	public var value: AnyObject? {
		switch self {
		case .Value(let v):
			return v

		default:
			return nil
		}
	}
}

private extension NSData {
	static func dataWithLittleEndianOf(nr: UInt64) -> NSData {
		var swapped = CFSwapInt64HostToLittle(nr)

		var bytes: [UInt8] = [0,0,0,0,0,0,0,0]
		for i in 0...7 {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return NSData(bytes: bytes, length: 8)
	}

	static func dataWithLittleEndianOf(nr: UInt32) -> NSData {
		var swapped = CFSwapInt32HostToLittle(nr) // No-op on little endian archs

		var bytes: [UInt8] = [0,0,0,0]
		for i in 0...3 {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return NSData(bytes: bytes, length: 4)
	}

	func readLittleEndianUInt64(atIndex: Int = 0) -> UInt64 {
		assert(self.length >= atIndex + 8)
		let buffer = UnsafeMutablePointer<UInt8>(self.bytes)
		var read: UInt64 = 0
		for i in (0...7).reverse() {
			read = (read << 8) + UInt64(buffer[atIndex + i])
		}
		return CFSwapInt64LittleToHost(read)
	}

	func readLittleEndianUInt32(atIndex: Int = 0) -> UInt32 {
		assert(self.length >= (atIndex + 4))
		let buffer = UnsafeMutablePointer<UInt8>(self.bytes)
		var read: UInt32 = 0
		for i in (0...3).reverse() {
			read = (read << 8) + UInt32(buffer[atIndex + i])
		}
		return CFSwapInt32LittleToHost(read)
	}
}
