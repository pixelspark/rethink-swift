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

internal enum ReSocketState {
	case unconnected
	case connecting
	case connected
}

internal class ReSocket: NSObject, GCDAsyncSocketDelegate {
	typealias WriteCallback = (String?) -> ()
	typealias ReadCallback = (Data?) -> ()

	let socket: GCDAsyncSocket
	internal var state: ReSocketState = .unconnected

	private var onConnect: ((String?) -> ())?
	private var writeCallbacks: [Int: WriteCallback] = [:]
	private var readCallbacks: [Int: ReadCallback] = [:]

	init(queue: DispatchQueue) {
		self.socket = GCDAsyncSocket(delegate: nil, delegateQueue: queue)
		super.init()
		self.socket.delegate = self
	}

	func connect(_ url: URL, withTimeout timeout: TimeInterval = 5.0, callback: (String?) -> ()) {
		assert(self.state == .unconnected, "Already connected or connecting")
		self.onConnect = callback
		self.state = .connecting

		guard let host = url.host else { return callback("Invalid URL") }
		let port = (url as NSURL).port ?? 28015

		do {
			try socket.connect(toHost: host, onPort: port.uint16Value, withTimeout: timeout)
		}
		catch let e {
			return callback(e.localizedDescription)
		}
	}

	@objc internal func socket(_ sock: GCDAsyncSocket, didConnectToHost host: String, port: UInt16) {
		self.state = .connected
		self.onConnect?(nil)
	}

	@objc internal func socketDidDisconnect(_ sock: GCDAsyncSocket, withError err: Error?) {
		self.state = .unconnected
	}

	func read(_ length: Int, callback: ReadCallback)  {
		assert(length > 0, "Length cannot be zero or less")

		if self.state != .connected {
			return callback(nil)
		}

		socket.delegateQueue!.async {
			let tag = (self.readCallbacks.count + 1)
			self.readCallbacks[tag] = callback
			self.socket.readData(toLength: UInt(length), withTimeout: -1.0, tag: tag)
		}
	}

	func readZeroTerminatedASCII(_ callback: (String?) -> ()) {
		if self.state != .connected {
			return callback(nil)
		}

		let zero = Data(bytes: UnsafePointer<UInt8>([UInt8(0)]), count: 1)
		socket.delegateQueue!.async {
			let tag = (self.readCallbacks.count + 1)
			self.readCallbacks[tag] = { data in
				if let d = data {
					if let s = NSString(data: d.subdata(in: 0..<(d.count-1)), encoding: String.Encoding.ascii.rawValue) {
						callback(String(s))
					}
					else {
						callback(nil)
					}
				}
				else {
					callback(nil)
				}
			}
			self.socket.readData(to: zero, withTimeout: -1.0, tag: tag)
		}
	}

	func write(_ data: Data, callback: WriteCallback) {
		if self.state != .connected {
			return callback("socket is not connected!")
		}

		socket.delegateQueue!.async {
			let tag = (self.writeCallbacks.count + 1)
			self.writeCallbacks[tag] = callback
			self.socket.write(data, withTimeout: -1.0, tag: tag)
		}
	}

	@objc internal func socket(_ sock: GCDAsyncSocket, didWriteDataWithTag tag: Int) {
		socket.delegateQueue!.async {
			if let cb = self.writeCallbacks[tag] {
				cb(nil)
				self.writeCallbacks.removeValue(forKey: tag)
			}
		}
	}

	@objc internal func socket(_ sock: GCDAsyncSocket, didRead data: Data, withTag tag: Int) {
		socket.delegateQueue!.async {
			if let cb = self.readCallbacks[tag] {
				cb(data)
				self.readCallbacks.removeValue(forKey: tag)
			}
		}
	}

	func disconnect() {
		self.socket.disconnect()
		self.state = .unconnected
	}

	deinit {
		self.socket.disconnect()
	}
}

/** A pthread-based recursive mutex lock. */
internal class Mutex {
	private var mutex: pthread_mutex_t = pthread_mutex_t()

	public init() {
		var attr: pthread_mutexattr_t = pthread_mutexattr_t()
		pthread_mutexattr_init(&attr)
		pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE)

		let err = pthread_mutex_init(&self.mutex, &attr)
		pthread_mutexattr_destroy(&attr)

		switch err {
		case 0:
			// Success
			break

		default:
			fatalError("Could not create mutex, error \(err)")
		}
	}

	private final func lock() {
		let ret = pthread_mutex_lock(&self.mutex)
		switch ret {
		case 0:
			// Success
			break

		default:
			fatalError("Could not lock mutex: error \(ret)")
		}
	}

	private final func unlock() {
		let ret = pthread_mutex_unlock(&self.mutex)
		switch ret {
		case 0:
			// Success
			break
		default:
			fatalError("Could not unlock mutex: error \(ret)")
		}
	}

	deinit {
		assert(pthread_mutex_trylock(&self.mutex) == 0 && pthread_mutex_unlock(&self.mutex) == 0, "deinitialization of a locked mutex results in undefined behavior!")
		pthread_mutex_destroy(&self.mutex)
	}

	@discardableResult public final func locked<T>(_ file: StaticString = #file, line: UInt = #line, block: @noescape () -> (T)) -> T {
		self.lock()
		defer {
			self.unlock()
		}
		let ret: T = block()
		return ret
	}
}
