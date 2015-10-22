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
			self.nextQueryToken++
		}
		return nt
	}
}

public class ReConnection: NSObject, NSStreamDelegate {
	public let url: NSURL
	public var authenticationKey: String? { get { return self.url.user } }

	private var inStream: ReInputStream! = nil
	private var outStream: ReOutputStream! = nil
	private var state: ReConnectionState = .Unconnected
	private var outstandingQueries: [ReQueryToken: ReResponse.Callback] = [:]
	private var onConnectCallback: ((String?) -> ())? = nil

	private static var tokenCounter = ReTokenCounter()
	private let queue = dispatch_queue_create("nl.pixelspark.Rethink.ReConnectionQueue", DISPATCH_QUEUE_SERIAL)

	/** Create a connection to a RethinkDB instance. The URL should be of the form 'rethinkdb://host:port'. If
	no port is given, the default port is used. If the server requires the use of an authentication key, put it
	in the 'user' part of the URL, e.g. "rethinkdb://key@server:port". */
	internal init(url: NSURL) {
		self.url = url
	}

	internal func connect(callback: (String?) -> ()) {
		assert(!self.state.connected)
		onConnectCallback = callback

		let port = (url.port ?? ReProtocol.defaultPort).integerValue
		assert(port < 65536)

		guard let hostname = url.host else {
			self.inStream = nil
			self.outStream = nil
			self.onConnectCallback = nil
			callback("Invalid URL")
			return
		}

		// Create a pair of streams to read/write to the database
		var readStream: Unmanaged<CFReadStreamRef>?
		var writeStream: Unmanaged<CFWriteStreamRef>?
		CFStreamCreatePairWithSocketToHost(nil, hostname, UInt32(port), &readStream, &writeStream)

		if let r = readStream, w = writeStream {
			let inStream: NSInputStream = r.takeRetainedValue()
			let outStream: NSOutputStream = w.takeRetainedValue()
			inStream.delegate = self
			outStream.delegate = self
			self.outStream = ReOutputStream(stream: outStream)
			self.inStream = ReInputStream(stream: inStream)


			dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0)) { [weak self] in
				inStream.scheduleInRunLoop(NSRunLoop.currentRunLoop(), forMode: NSDefaultRunLoopMode)
				outStream.scheduleInRunLoop(NSRunLoop.currentRunLoop(), forMode: NSDefaultRunLoopMode)
				inStream.open()
				outStream.open()
				if let s = self {
					s.state.onReceive(s)
				}

				while self != nil {
					if !NSRunLoop.currentRunLoop().runMode(NSDefaultRunLoopMode, beforeDate: NSDate(timeInterval: 5.0, sinceDate: NSDate())) {
						break
					}
				}

				#if DEBUG
				print("Runloop end")
				#endif
			}
		}
		else {
			inStream = nil
			outStream = nil
			callback("connection failed")
			self.onConnectCallback = nil
		}
	}

	public func close() {
		dispatch_async(queue) {
			self.state = .Terminated
			self.inStream = nil
			self.outStream = nil
			self.onConnectCallback = nil
			self.outstandingQueries.removeAll()
		}
	}

	public var connected: Bool { get {
		return self.state.connected
		} }

	public var error: ReError? { get {
		return self.state.error
		} }

	public func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
		dispatch_sync(self.queue) {
			do {
				switch eventCode {
				case NSStreamEvent.ErrorOccurred:
					throw aStream.streamError!

				case NSStreamEvent.OpenCompleted:
					break;

				case NSStreamEvent.HasBytesAvailable:
					if aStream == self.inStream?.stream {
						self.inStream.read()
						self.state.onReceive(self)
						if self.connected {
							if let cc = self.onConnectCallback {
								cc(nil)
								self.onConnectCallback = nil
							}
						}
					}

				case NSStreamEvent.HasSpaceAvailable:
					if aStream == self.outStream?.stream {
						self.outStream.write()
					}

				case NSStreamEvent.EndEncountered:
					throw ReError.Fatal("The server unexpectedly closed the connection.")

				default:
					throw ReError.Fatal("unhandled stream event: \(eventCode)")
				}
			}
			catch ReError.Fatal(let m) {
				if let cc = self.onConnectCallback {
					cc(m)
					self.onConnectCallback = nil
				}
				else {
					fatalError("An error occurred: \(m)")
				}
			}
			catch let e as NSError {
				if let cc = self.onConnectCallback {
					cc(e.localizedDescription)
					self.onConnectCallback = nil
				}
				else {
					fatalError("An error occurred: \(self.error)")
				}
			}
		}
	}

	private func sendContinuation(token: ReQueryToken, callback: ReResponse.Callback) throws {
		let json = [ReProtocol.ReQueryType.CONTINUE.rawValue];
		let query = try NSJSONSerialization.dataWithJSONObject(json, options: [])
		self.sendQuery(query, token: token, callback: callback)
	}

	private func sendQuery(query: NSData, token: ReQueryToken, callback: ReResponse.Callback) {
		dispatch_async(queue) {
			assert(self.outstandingQueries[token] == nil, "A query with token \(token) is already outstanding")
			assert(self.connected, "Cannot send a query when the connection is not open")
			let data = NSMutableData(capacity: query.length + 8 + 4)!
			self.outstandingQueries[token] = callback
			data.appendData(NSData.dataWithLittleEndianOf(token))
			data.appendData(NSData.dataWithLittleEndianOf(UInt32(query.length)))
			data.appendData(query)
			self.outStream.send(data)
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
	case Receiving(UInt64, UInt32) // Server is sending us data for the query (Query token, response length)
	case Error(ReError) // A protocol error has occurred
	case Terminated // The connection has been terminated

	var connected: Bool { get {
		switch self {
		case .Connected, .Receiving(_, _):
			return true

		default:
			return false
		}
		} }

	var error: ReError? { get {
		switch self {
		case .Error(let e):
			return e

		default:
			return nil
		}
		} }

	mutating func onReceive(database: ReConnection) {
		switch self {
		case .Unconnected:
			// Start authentication
			guard let data = NSMutableData(capacity: 128) else {
				self = .Error(ReError.Fatal("Could not create data object"));
				return
			}

			// Append protocol version
			data.appendData(NSData.dataWithLittleEndianOf(UInt32(ReProtocol.protocolVersion)))

			// Append authentication key length and the key itself (as ASCII)
			if let authKey = database.authenticationKey?.dataUsingEncoding(NSASCIIStringEncoding) {
				data.appendData(NSData.dataWithLittleEndianOf(UInt32(authKey.length)))
				data.appendData(authKey)
			}
			else {
				data.appendData(NSData.dataWithLittleEndianOf(UInt32(0)))
			}

			// Append protocol type (JSON)
			data.appendData(NSData.dataWithLittleEndianOf(UInt32(ReProtocol.protocolType)))
			database.outStream.send(data)
			self = .HandshakeSent

		case .HandshakeSent:
			if let s = database.inStream.consumeZeroTerminatedASCII() {
				if s == ReProtocol.handshakeSuccessResponse {
					self = .Connected
				}
				else {
					self = .Error(ReError.Fatal("Handshake failed, server returned: \(s)"))
				}
			}
			break

		case .Connected:
			if let d = database.inStream.consumeData(8 + 4) {
				let queryToken = d.readLittleEndianUInt64(0)
				let responseSize = d.readLittleEndianUInt32(8)
				self = .Receiving(queryToken, responseSize)
				self.onReceive(database)
			}
			break

		case .Receiving(let queryToken, let size):
			if database.inStream.buffer.availableBytes() >= Int(size) {
				let d = database.inStream.consumeData(Int(size))!
				assert(d.length == Int(size))
				self = .Connected
				var called = false
				let continuation: ReResponse.ContinuationCallback = { [weak database] (callback: ReResponse.Callback) -> () in
					assert(!called, "continuation callback for query token \(queryToken) must never be called more than once")
					called = true
					do {
						try database?.sendContinuation(queryToken, callback: callback)
					}
					catch {
						callback(ReResponse.Error("Could not send continuation"))
					}
				}
				
				dispatch_async(database.queue) {
					if let handler = database.outstandingQueries[queryToken] {
						if let response = ReResponse(json: d, continuation: continuation) {
							database.outstandingQueries.removeValueForKey(queryToken)
							handler(response)
						}
						else {
							self = .Error(ReError.Fatal("Invalid response object from server"))
						}
					}
					else {
						fatalError("No handler found for server response. This should never happen unless server is behaving badly.")
					}
				}

				self.onReceive(database)
			}
			else {
				// Need more data for this query token before we can continue
			}
			break

		case .Terminated:
			break

		case .Error:
			break
		}
	}
}

public enum ReError: ErrorType {
	case Fatal(String)
	case Other(ErrorType)
}

private class ReInputStream: NSObject {
	typealias Callback = (ReInputStream) -> ()
	let stream: NSInputStream
	private var error: ReError? = nil
	private let queue = dispatch_queue_create("nl.pixelspark.Rethink.ReInputStream", DISPATCH_QUEUE_SERIAL)
	private var buffer: MAMirroredQueue

	init(stream: NSInputStream) {
		self.stream = stream
		self.buffer = MAMirroredQueue()
		super.init()
	}

	private func read() {
		dispatch_sync(self.queue) {
			while self.stream.hasBytesAvailable {
				let buffer = NSMutableData(length: 512)!
				let read = self.stream.read(UnsafeMutablePointer<UInt8>(buffer.mutableBytes), maxLength: buffer.length)
				if read > 0 {
					self.buffer.write(buffer.bytes, count: read)
				}
			}
		}
	}

	private func consumeData(length: Int) -> NSData? {
		var ret: NSData? = nil
		dispatch_sync(self.queue) {
			assert(length>0)
			if self.buffer.availableBytes() >= length {
				let sliced = NSData(bytes: self.buffer.readPointer(), length: length)
				self.buffer.advanceReadPointer(length)
				ret = sliced
				return
			}
		}
		return ret
	}

	private func consumeZeroTerminatedASCII() -> String? {
		var ret: String? = nil
		dispatch_sync(self.queue) {
			let bytes = UnsafePointer<UInt8>(self.buffer.readPointer())
			for(var i=0; i<self.buffer.availableBytes(); i++) {
				if bytes[i] == 0 {
					if let x = NSString(bytes: bytes, length: i, encoding: NSASCIIStringEncoding) {
						self.buffer.advanceReadPointer(i+1)
						ret = x as String
						return
					}
					return
				}
			}
			return
		}
		return ret
	}

	private func checkError() throws {
		if let e = self.error {
			throw e
		}
		if let e = self.stream.streamError {
			throw ReError.Other(e)
		}
	}

	deinit {
		self.stream.close()
	}
}

private class ReOutputStream: NSObject {
	let stream: NSOutputStream
	private var error: ReError? = nil
	private var outQueue: MAMirroredQueue
	private let queue = dispatch_queue_create("nl.pixelspark.Rethink.ReOutputStream", DISPATCH_QUEUE_SERIAL)

	init(stream: NSOutputStream) {
		self.stream = stream
		self.outQueue = MAMirroredQueue()
		super.init()
	}

	deinit {
		self.stream.close()
	}

	private func send(data: NSData) {
		dispatch_async(self.queue) {
			self.outQueue.write(data.bytes, count: data.length)
			self.write()
		}
	}

	private func write() {
		dispatch_async(self.queue) {
			while self.stream.hasSpaceAvailable && self.outQueue.availableBytes()>0 {
				let bytesWritten = self.stream.write(UnsafePointer<UInt8>(self.outQueue.readPointer()), maxLength: self.outQueue.availableBytes())

				if bytesWritten > 0 {
					self.outQueue.advanceReadPointer(bytesWritten)
				}
			}
		}
	}
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

	public var isError: Bool { get {
		switch self {
		case .Error(_): return true
		default: return false
		}
		} }

	public var value: AnyObject? { get {
		switch self {
		case .Value(let v):
			return v

		default:
			return nil
		}
		} }
}

private extension NSData {
	static func dataWithLittleEndianOf(nr: UInt64) -> NSData {
		var swapped = CFSwapInt64HostToLittle(nr)

		var bytes: [UInt8] = [0,0,0,0,0,0,0,0]
		for(var i = 0; i <= 7; i++) {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return NSData(bytes: bytes, length: 8)
	}

	static func dataWithLittleEndianOf(nr: UInt32) -> NSData {
		var swapped = CFSwapInt32HostToLittle(nr) // No-op on little endian archs

		var bytes: [UInt8] = [0,0,0,0]
		for(var i = 0; i <= 3; i++) {
			bytes[i] = UInt8(swapped & 0xFF)
			swapped = swapped >> 8
		}

		return NSData(bytes: bytes, length: 4)
	}

	func readLittleEndianUInt64(atIndex: Int = 0) -> UInt64 {
		assert(self.length >= atIndex + 8)
		let buffer = UnsafeMutablePointer<UInt8>(self.bytes)
		var read: UInt64 = 0
		for(var i = 7; i >= 0; i--) {
			read = (read << 8) + UInt64(buffer[atIndex + i])
		}
		return CFSwapInt64LittleToHost(read)
	}

	func readLittleEndianUInt32(atIndex: Int = 0) -> UInt32 {
		assert(self.length >= (atIndex + 4))
		let buffer = UnsafeMutablePointer<UInt8>(self.bytes)
		var read: UInt32 = 0
		for(var i = 3; i >= 0; i--) {
			read = (read << 8) + UInt32(buffer[atIndex + i])
		}
		return CFSwapInt32LittleToHost(read)
	}
}
